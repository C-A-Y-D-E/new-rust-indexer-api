use clickhouse::Compression;
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

use std::any::Any;
use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use fixnum::ops::{CheckedAdd, CheckedMul, RoundMode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::{Client, Row, error::Result};
use tracing::{error, warn};

use crate::defaults::QuoteTokenData;
use crate::defaults::{SOL_TOKEN, USDC_TOKEN};
use crate::models::account::{Account, DBTokenAccount};
use crate::models::extra::{HolderResponse, PairInfo, TopTrader};
use crate::models::ohlcv::OHLCV;
use crate::models::pool::{DBPool, Pool};
use crate::models::pool_report::PoolReport;
use crate::models::swap::{DBSwap, Swap};
use crate::models::token::{DBToken, Token};
use crate::models::transfer::{DbTransferSol, TransferSol};
use crate::routes::pool_report::ReportType;
use crate::types::token_info::{TokenInfo, TokenInfoRow};
use crate::utils::{Decimal18, calculate_percentage};

#[derive(Clone)]
pub struct ClickhouseService {
    pub client: Client,
}

#[derive(Debug, Serialize, Deserialize)]

pub struct PoolAndTokenData {
    pub pool_address: String,
    pub base_liquidity: f64,
    pub quote_liquidity: f64,
    pub marketcap_sol: f64,
    pub factory: String,
    pub pre_factory: Option<String>,

    pub volume_quote: f64,
    pub base_token: DBToken,
    pub quote_token: QuoteTokenData,
}

impl ClickhouseService {
    pub async fn init() -> Self {
        let url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
        let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
        let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "".to_string());
        let database =
            std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());

        let client = Client::default()
            .with_url(&url)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0")
            .with_option("async_insert_max_data_size", "104857600") // 100MB (keep this)
            .with_option("async_insert_busy_timeout_ms", "400") // Increase to 2 seconds
            .with_option("async_insert_stale_timeout_ms", "0") // Set to 1 second
            // Memory optimization
            .with_option("max_memory_usage", "0")
            .with_option("max_memory_usage_for_user", "0")
            // CPU optimization
            .with_option("max_threads", "0")
            .with_option("max_execution_time", "0")
            // Write performance
            .with_option("insert_quorum", "1")
            .with_option("insert_quorum_timeout", "60000")
            .with_option("insert_quorum_parallel", "1")
            // JSON support
            .with_option("allow_experimental_json_type", "1")
            .with_option("input_format_binary_read_json_as_string", "1")
            .with_option("output_format_binary_write_json_as_string", "1")
            // Performance tuning
            .with_option("max_insert_block_size", "1048576")
            .with_option("min_insert_block_size_rows", "1048576")
            .with_option("min_insert_block_size_bytes", "268435456")
            .with_compression(Compression::Lz4)
            .with_user(&user)
            .with_password(&password)
            .with_database(&database);

        let service = Self { client };

        service
    }
    pub async fn search_pools(&self, pool_address: String) -> Result<Vec<DBPool>> {
        let query = r#"
        SELECT 
            pools.pool_address,
            pools.factory,
            pools.pre_factory,
            pools.reversed,
            pools.token_base_address,
            pools.token_quote_address,
            pools.pool_base_address,
            pools.pool_quote_address,
            pools.initial_token_base_reserve,
            pools.initial_token_quote_reserve,
            pools.slot,
            pools.creator,
            pools.hash,
            pools.metadata,
            pools.created_at,
            COALESCE(pcu.curve_percentage, 0) as curve_percentage,
            COALESCE(pcu.updated_at, pools.created_at) as updated_at
        FROM pools
        LEFT JOIN (SELECT * FROM pool_curve_updates FINAL) pcu ON pools.pool_address = pcu.pool_address
        WHERE pools.pool_address = ?
        ORDER BY pools.pool_address DESC
        LIMIT 10
    "#;

        let pools: Vec<DBPool> = self
            .client
            .query(query)
            .bind(pool_address)
            .fetch_all()
            .await?;

        Ok(pools)
    }
    pub async fn search_tokens(
        &self,
        search: String,
    ) -> Result<Vec<Token>, clickhouse::error::Error> {
        // ClickHouse does not support ILIKE, so we use LIKE with lower() for case-insensitive search
        let query = r#"
            SELECT 
                i.hash as hash,
                i.mint_address as mint_address,
                COALESCE(m.name, '') as name,
                COALESCE(m.symbol, '') as symbol,
                i.decimals as decimals,
                COALESCE(m.uri, '') as uri,
                COALESCE(a.mint_authority, i.mint_authority) as mint_authority,
                COALESCE(s.total_supply, 0) as supply,
                COALESCE(a.freeze_authority, i.freeze_authority) as freeze_authority,
                i.slot as slot,
                m.image as image,
                m.twitter as twitter,
                m.telegram as telegram,
                m.website as website,
                i.program_id as program_id
            FROM token_initialize_events i
            LEFT JOIN (
                SELECT mint_address, sum(total_raw_supply) as total_supply 
                FROM token_supply_mv FINAL 
                GROUP BY mint_address
            ) s ON i.mint_address = s.mint_address
            LEFT JOIN (SELECT * FROM token_latest_metadata_mv FINAL) m ON i.mint_address = m.mint_address
            LEFT JOIN (SELECT * FROM token_latest_authority_mv FINAL) a ON i.mint_address = a.mint_address
            WHERE lower(COALESCE(m.name, '')) LIKE lower(?) OR lower(COALESCE(m.symbol, '')) LIKE lower(?)
            ORDER BY COALESCE(m.name, '') DESC
            LIMIT 10
        "#;

        // Add wildcards for LIKE search
        let search_pattern = format!("%{}%", search);

        let tokens: Vec<DBToken> = self
            .client
            .query(query)
            .bind(&search_pattern)
            .bind(&search_pattern)
            .fetch_all()
            .await?;

        let response_tokens = tokens
            .into_iter()
            .filter_map(|token| Token::try_from(token).ok())
            .collect();

        Ok(response_tokens)
    }
    pub async fn get_pool_and_token_data(
        &self,
        address: String,
    ) -> Result<Option<PoolAndTokenData>, clickhouse::error::Error> {
        // Alternative strategy: perform multiple simpler queries and combine results in Rust

        // 1. Try to find the pool by pool_address or token_base_address
        let pool_query = r#"
            SELECT
                *
            FROM pools
            WHERE pool_address = ? OR token_base_address = ?
            LIMIT 1
        "#;

        let mut pools: Vec<DBPool> = self
            .client
            .query(pool_query)
            .bind(&address)
            .bind(&address)
            .fetch_all()
            .await?;

        let pool = match pools.pop() {
            Some(pool) => pool,
            None => return Ok(None),
        };

        // 2. Get the base token info
        let token_query = r#"
        SELECT
            hash,
            mint_address,
            COALESCE(name, '') as name,
            COALESCE(symbol, '') as symbol,
            decimals,
            COALESCE(uri, '') as uri,
            mint_authority,
            supply,
            freeze_authority,
            slot,
            image,
            twitter,
            telegram,
            website,
            program_id
        FROM tokens
        WHERE mint_address = ?
        LIMIT 1
    "#;

        let token: DBToken = self
            .client
            .query(token_query)
            .bind(&pool.token_base_address)
            .fetch_one()
            .await?;

        // 3. Get the latest swap info for this pool
        let swap_query = r#"
            SELECT *
            FROM swaps
            WHERE pool_address = ?
            ORDER BY created_at DESC
            LIMIT 1
        "#;

        let swap_row = self
            .client
            .query(swap_query)
            .bind(&pool.pool_address)
            .fetch_one::<DBSwap>()
            .await?;

        // 4. Get the latest 24h volume for this pool
        // Instead of selecting directly from pool_report_24h (which contains SimpleAggregateFunction columns that Clickhouse Rust client may not parse directly),
        // select from an aggregation query using FINAL and materializedAggregate functions to cast to native decimals.
        let volume_query = r#"
  SELECT
      bucket_start,
      pool_address,
      sum(buy_volume) AS buy_volume,
      sum(buy_count) AS buy_count,
      sum(sell_volume) AS sell_volume, 
      sum(sell_count) AS sell_count,
      sum(unique_traders) AS unique_traders,
      sum(unique_buyers) AS unique_buyers,
      sum(unique_sellers) AS unique_sellers 
    FROM pool_report_24h FINAL
    WHERE pool_address = ?
    GROUP BY pool_address, bucket_start
    ORDER BY bucket_start DESC
    LIMIT 1
        "#;

        let volume_row: PoolReport = self
            .client
            .query(volume_query)
            .bind(&pool.pool_address)
            .fetch_one()
            .await?;

        // 5. Build quote_token using the pool's token_quote_address
        let quote_token = if pool.token_quote_address == SOL_TOKEN.address {
            QuoteTokenData {
                address: SOL_TOKEN.address.to_string(),
                name: SOL_TOKEN.name.to_string(),
                symbol: SOL_TOKEN.symbol.to_string(),
                decimals: SOL_TOKEN.decimals,
                logo: SOL_TOKEN.logo.to_string(),
            }
        } else if pool.token_quote_address == USDC_TOKEN.address {
            QuoteTokenData {
                address: USDC_TOKEN.address.to_string(),
                name: USDC_TOKEN.name.to_string(),
                symbol: USDC_TOKEN.symbol.to_string(),
                decimals: USDC_TOKEN.decimals,
                logo: USDC_TOKEN.logo.to_string(),
            }
        } else {
            // You may want to handle this more gracefully in production
            panic!("Quote token not found");
        };

        Ok(Some(PoolAndTokenData {
            pool_address: pool.pool_address.clone(),
            base_liquidity: swap_row.base_reserve,
            quote_liquidity: swap_row.quote_reserve,
            marketcap_sol: swap_row.price_sol * token.supply,
            factory: pool.factory.clone(),
            pre_factory: pool.pre_factory.clone(),

            volume_quote: volume_row.buy_volume + volume_row.sell_volume,
            base_token: token,
            quote_token,
        }))
    }

    pub async fn get_last_transaction(&self, pool_address: String) -> Result<Option<DBSwap>> {
        // Try to be explicit with columns and types, as SELECT * can cause issues if struct and table don't match

        let query = r#"
            SELECT 
  creator,
  pool_address,
  hash,
  base_amount,
  quote_amount,
  base_reserve,
  quote_reserve,
  price_sol,
  swap_type,
  slot,
  created_at
    FROM swaps
    PREWHERE pool_address = ?
    ORDER BY slot DESC
    LIMIT 1
    SETTINGS optimize_read_in_order = 1
        "#;

        match self
            .client
            .query(query)
            .bind(&pool_address)
            .fetch_optional::<DBSwap>() // Use DBSwap to deserialize the row into your struct
            .await
        {
            Ok(result) => Ok(result),
            Err(e) => {
                println!("Error getting last transaction: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_candlestick(
        &self,
        pool_address: String,
        interval: String,
        start_time: i64,
        end_time: i64,
        limit: i32,
    ) -> Result<Vec<serde_json::Value>, clickhouse::error::Error> {
        let table_name = format!("candles_{}", interval);
        let query = format!(
            "SELECT timestamp, open, high, low, close, volume_base, volume_quote, trades \
             FROM {} WHERE pool_address = ? AND timestamp >= ? AND timestamp <= ? \
             ORDER BY timestamp ASC LIMIT ?",
            table_name
        );

        let mut lines = self
            .client
            .query(&query)
            .bind(&pool_address)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_bytes("JSONEachRow")
            .unwrap()
            .lines();

        let mut values = vec![];
        while let Some(line) = lines.next_line().await.unwrap() {
            let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            // println!("JSONEachRow value: {value}");
            values.push(value);
        }

        Ok(values)
    }

    pub async fn get_top_traders(
        &self,
        pool_address: String,
    ) -> Result<Vec<TopTrader>, clickhouse::error::Error> {
        let query = r#"
        WITH first_swap AS (
            SELECT slot
            FROM swaps
            WHERE pool_address = ?
              AND (swap_type = 'BUY' OR swap_type = 'SELL')
            ORDER BY slot ASC
            LIMIT 1
        ),
        pool_info AS (
            SELECT token_base_address 
            FROM pools 
            WHERE pool_address = ?
        )
        SELECT
            s.creator,
            max(s.slot = f.slot) as is_sniper,
            coalesce(sumIf(s.base_amount, s.swap_type = 'BUY'), 0) as base_bought,
            coalesce(sumIf(s.base_amount, s.swap_type = 'SELL'), 0) as base_sold,
            coalesce(sumIf(s.quote_amount, s.swap_type = 'BUY'), 0) as quote_bought,
            coalesce(sumIf(s.quote_amount, s.swap_type = 'SELL'), 0) as quote_sold,
            coalesce(max(a.amount) / pow(10, COALESCE(max(t.decimals), 0)), 0) as holding_base_token
        FROM swaps s
        CROSS JOIN first_swap f
        CROSS JOIN pool_info pi
        LEFT JOIN (
            SELECT owner, mint, amount 
            FROM accounts FINAL 
            WHERE amount > 0
        ) a ON a.owner = s.creator AND a.mint = pi.token_base_address
        LEFT JOIN token_initialize_events t ON t.mint_address = pi.token_base_address
        WHERE s.pool_address = ?
          AND (s.swap_type = 'BUY' OR s.swap_type = 'SELL')
        GROUP BY s.creator
        ORDER BY base_bought DESC
        LIMIT 20
        "#;

        let traders: Vec<TopTrader> = self
            .client
            .query(query)
            .bind(&pool_address) // For first_swap CTE
            .bind(&pool_address) // For pool_info CTE
            .bind(&pool_address) // For main WHERE clause
            .fetch_all()
            .await?;

        Ok(traders)
    }

    pub async fn get_pair_info(&self, pool_address: String) -> Result<PairInfo> {
        warn!("Getting pair info for pool address: {}", pool_address);
        let query = r#"
        SELECT
            pools.pool_address as pool_address,
            pools.pool_base_address as pool_base_address,
            pools.pool_quote_address as pool_quote_address,
            pools.token_base_address as token_base_address,
            pools.token_quote_address as token_quote_address,
            pools.creator as creator,
            pools.hash as hash,
            pools.factory as factory,
            pools.pre_factory as pre_factory,
            pools.reversed as reversed,
            COALESCE(pcu.curve_percentage, 0) as curve_percentage,
            pools.initial_token_base_reserve as initial_token_base_reserve,
            pools.initial_token_quote_reserve as initial_token_quote_reserve,
            pools.slot as slot,
            pools.metadata as metadata,
            pools.created_at as created_at,
            t.hash AS token_hash,
            t.mint_address as mint_address,
            COALESCE(t.name, '') as name,
            COALESCE(t.symbol, '') as symbol,
            t.decimals as decimals,
            COALESCE(t.uri, '') as uri,
            t.mint_authority as mint_authority,
            t.supply as supply,
            t.freeze_authority as freeze_authority,
            t.slot AS token_slot,
            t.image as image,
            t.twitter as twitter,
            t.telegram as telegram,
            t.website as website,
            t.program_id as program_id
        FROM pools
        LEFT JOIN (SELECT * FROM pool_curve_updates FINAL) pcu ON pools.pool_address = pcu.pool_address
        INNER JOIN tokens t ON pools.token_base_address = t.mint_address
        WHERE pools.pool_address = ? OR pools.token_base_address = ?
        LIMIT 1
    "#;

        // Use a struct for strict schema alignment (schema: 31 columns, struct: 31 fields)
        #[derive(Debug, serde::Deserialize, Row, Serialize)]
        struct PairInfoRow {
            pool_address: String,
            pool_base_address: String,
            pool_quote_address: String,
            token_base_address: String,
            token_quote_address: String,
            creator: String,
            hash: String,
            factory: String,
            pre_factory: Option<String>,
            reversed: bool,
            curve_percentage: f32,
            initial_token_base_reserve: f64,
            initial_token_quote_reserve: f64,
            slot: i64,
            metadata: String,
            #[serde(with = "clickhouse::serde::chrono::datetime")]
            created_at: DateTime<Utc>,

            // tokens.* fields
            token_hash: String,
            mint_address: String,
            name: String,
            symbol: String,
            decimals: i8,
            uri: String,
            supply: f64,
            token_slot: i64,
            mint_authority: Option<String>,
            freeze_authority: Option<String>,
            image: Option<String>,
            twitter: Option<String>,
            telegram: Option<String>,
            website: Option<String>,
            program_id: String,
        }

        let rows: Vec<PairInfoRow> = self
            .client
            .query(query)
            .bind(&pool_address)
            .bind(&pool_address)
            .fetch_all()
            .await?;

        println!("rows: {:?}", rows);

        if let Some(row) = rows.into_iter().next() {
            let pair_info = PairInfo {
                pool: DBPool {
                    pool_address: row.pool_address,
                    pool_base_address: row.pool_base_address,
                    pool_quote_address: row.pool_quote_address,
                    token_base_address: row.token_base_address,
                    token_quote_address: row.token_quote_address,
                    creator: row.creator,
                    hash: row.hash,
                    factory: row.factory,
                    pre_factory: row.pre_factory,
                    reversed: row.reversed,

                    initial_token_base_reserve: row.initial_token_base_reserve,
                    initial_token_quote_reserve: row.initial_token_quote_reserve,
                    slot: row.slot,
                    metadata: row.metadata.to_string(),
                    created_at: row.created_at,
                },
                base_token: DBToken {
                    mint_address: row.mint_address,
                    name: row.name,
                    symbol: row.symbol,
                    decimals: row.decimals,
                    uri: row.uri,
                    supply: row.supply,
                    slot: row.token_slot,
                    mint_authority: row.mint_authority,
                    freeze_authority: row.freeze_authority,
                    hash: row.token_hash,
                    image: row.image,
                    twitter: row.twitter,
                    telegram: row.telegram,
                    website: row.website,
                    program_id: row.program_id,
                },
            };
            Ok(pair_info)
        } else {
            Err(clickhouse::error::Error::Custom("No pair found".into()))
        }
    }
    pub async fn get_holders(
        &self,
        mint: String,
    ) -> Result<Vec<HolderResponse>, clickhouse::error::Error> {
        let query = r#"
            SELECT 
                assumeNotNull(accounts.owner) as address,
                accounts.account as account,
                assumeNotNull(accounts.mint) as mint,
                CAST(t.decimals AS UInt8) as decimals,
                CAST(accounts.amount / POW(10, t.decimals) AS Float64) as amount,
                COALESCE(accounts.delegated_amount, 0) as delegated_amount
            FROM accounts FINAL
            INNER JOIN token_initialize_events t ON accounts.mint = t.mint_address
            WHERE accounts.mint = ? 
              AND accounts.amount > 0
              AND accounts.owner IS NOT NULL
              AND accounts.mint IS NOT NULL
              AND accounts.account IS NOT NULL
            ORDER BY amount DESC
            LIMIT 50
        "#;

        let holders = self
            .client
            .query(query)
            .bind(&mint)
            .fetch_all::<HolderResponse>()
            .await?;

        Ok(holders)
    }

    pub async fn get_pool_swaps(
        &self,
        pool_address: String,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
    ) -> Result<Vec<DBSwap>, clickhouse::error::Error> {
        // ClickHouse expects Unix timestamp (i64) for date comparisons in many data models.
        // Convert Option<DateTime<Utc>> to i64 (unix timestamp), or use a default min/max if None.
        use chrono::{TimeZone, Utc};

        let start_ts = start_date.map(|dt| dt.timestamp()).unwrap_or(0); // Default: epoch 0
        let end_ts = end_date.map(|dt| dt.timestamp()).unwrap_or(253402300799); // Default: year 9999

        let query = r#"
            SELECT *
            FROM swaps
            WHERE pool_address = ?
              AND created_at >= ?
              AND created_at <= ?
            ORDER BY created_at DESC
            LIMIT 20
        "#;

        let swaps: Vec<DBSwap> = self
            .client
            .query(query)
            .bind(&pool_address)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_all()
            .await?;

        Ok(swaps)
    }

    pub async fn get_pool_report(
        &self,
        pool_address: String,
        report_type: ReportType,
    ) -> Result<Option<Vec<PoolReport>>> {
        let table_suffix = match report_type {
            ReportType::OneMinute => "1m",
            ReportType::FiveMinutes => "5m",
            ReportType::OneHour => "1h",
            ReportType::SixHours => "6h",
            ReportType::OneDay => "24h",
        };

        let query = format!(
            r#"
            SELECT
                bucket_start,
                pool_address,
                buy_volume,
                buy_count,
                sell_volume, 
                sell_count,
                unique_traders,
                unique_buyers,
                unique_sellers 
            FROM pool_report_{}
            WHERE pool_address = ?
            ORDER BY bucket_start DESC
            LIMIT 1000
            "#,
            table_suffix
        );
        // let data = self
        //     .client
        //     .query(&query)
        //     .bind(&pool_address)
        //     .fetch_bytes("JSONEachRow")
        //     .unwrap()
        //     .lines();
        // let data: Vec<PoolReport> = data
        //     .map(|line| serde_json::from_str(&line).unwrap())
        //     .collect();
        // Ok(Some(data))
        // // while let Some(line) = lines.next_line().await.unwrap() {
        // //     let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
        // //     println!("JSONEachRow value: {value}");
        // // }
        // // Ok(Some(data))

        let data: Vec<PoolReport> = self
            .client
            .query(&query)
            .bind(&pool_address)
            .fetch_all()
            .await?;
        Ok(Some(data))
    }

    pub async fn get_token_info(
        &self,
        pool_address: String,
    ) -> Result<TokenInfo, clickhouse::error::Error> {
        let query = r#"
WITH pool_info AS (
    SELECT 
        p.pool_address, 
        p.token_base_address, 
        p.creator,
        p.pool_base_address, 
        p.pool_quote_address,
        p.slot
    FROM pools p
    WHERE p.pool_address = ?
),
top10_holders AS (
    SELECT CAST(sum(x.amount) AS Float64) AS top10_amount_raw
    FROM (
        SELECT
            a.amount,
            row_number() OVER (ORDER BY a.amount DESC) AS rn
        FROM pool_info pi
        JOIN (SELECT * FROM accounts FINAL) a ON a.mint = pi.token_base_address
        WHERE a.amount > 0
          AND NOT EXISTS (
            SELECT 1 FROM pool_info pi2 
            WHERE pi2.pool_address = a.owner 
               OR pi2.pool_base_address = a.owner 
               OR pi2.pool_quote_address = a.owner
        )
    ) x
    WHERE rn <= 10
),
dev_hold AS (
    SELECT CAST(coalesce(a.amount, 0) AS Float64) AS dev_amount_raw
    FROM pool_info pi
    LEFT JOIN (SELECT * FROM accounts FINAL) a
        ON a.mint = pi.token_base_address
        AND a.owner = pi.creator
),
bundlers_holds AS (
    SELECT CAST(coalesce(sum(s.base_amount), 0) AS Float64) AS bundlers_amount_raw
    FROM pool_info pi
    LEFT JOIN swaps s ON s.pool_address = pi.pool_address
    WHERE (s.swap_type = 'BUY' AND s.slot = pi.slot) OR s.swap_type IS NULL
      AND (NOT EXISTS (
          SELECT 1 FROM pool_info pi2 
          WHERE pi2.pool_address = s.creator 
             OR pi2.pool_base_address = s.creator 
             OR pi2.pool_quote_address = s.creator
      ) OR s.creator IS NULL)
),
snipers_holds AS (
    SELECT CAST(coalesce(sum(s.base_amount), 0) AS Float64) AS snipers_amount_raw
    FROM pool_info pi
    LEFT JOIN swaps s ON s.pool_address = pi.pool_address
    WHERE (s.swap_type = 'BUY' AND s.slot = pi.slot + 1) OR s.swap_type IS NULL
      AND (NOT EXISTS (
          SELECT 1 FROM pool_info pi2 
          WHERE pi2.pool_address = s.creator 
             OR pi2.pool_base_address = s.creator 
             OR pi2.pool_quote_address = s.creator
      ) OR s.creator IS NULL)
),
total_holders AS (
    SELECT count(DISTINCT a.owner) AS num_holders
    FROM pool_info pi
    JOIN (SELECT * FROM accounts FINAL) a ON a.mint = pi.token_base_address
    WHERE a.amount > 0
      AND NOT EXISTS (
        SELECT 1 FROM pool_info pi2 
        WHERE pi2.pool_address = a.owner 
           OR pi2.pool_base_address = a.owner 
           OR pi2.pool_quote_address = a.owner
    )
),
tok AS (
    SELECT
        i.mint_address,
        i.decimals,
        COALESCE(s.total_supply, 0) AS token_supply
    FROM token_initialize_events i
    LEFT JOIN (
        SELECT mint_address, sum(total_raw_supply) as total_supply 
        FROM token_supply_mv FINAL 
        GROUP BY mint_address
    ) s ON i.mint_address = s.mint_address
    JOIN pool_info pi ON i.mint_address = pi.token_base_address
)
SELECT
    coalesce(th.top10_amount_raw, 0) AS top10_amount_raw,
    coalesce(d.dev_amount_raw, 0) AS dev_amount_raw,
    coalesce(sh.snipers_amount_raw, 0) AS snipers_amount_raw,
    coalesce(t.num_holders, 0) AS num_holders,
    coalesce(tk.token_supply, 0) AS token_supply,
    coalesce(tk.decimals, 0) AS decimals,
    coalesce(bh.bundlers_amount_raw, 0) AS bundlers_amount_raw
FROM pool_info pi
LEFT JOIN top10_holders th ON 1=1
LEFT JOIN dev_hold d ON 1=1
LEFT JOIN snipers_holds sh ON 1=1
LEFT JOIN bundlers_holds bh ON 1=1
LEFT JOIN total_holders t ON 1=1
LEFT JOIN tok tk ON 1=1
        "#;

        let rows: Vec<TokenInfoRow> = match self
            .client
            .query(query)
            .bind(&pool_address)
            .fetch_all()
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                error!("Error fetching token info: {:?}", e);
                return Err(e);
            }
        };

        println!("rows: {:?}", rows);

        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| clickhouse::error::Error::Custom("No token info found".into()))?;

        // Calculate scale factor in Rust: 10^decimals
        let scale_factor = 10.0_f64.powi(row.decimals as i32);

        let bundlers_decimal_adjusted = row.bundlers_amount_raw / scale_factor;
        let dev_decimal_adjusted = row.dev_amount_raw / scale_factor;
        let snipers_decimal_adjusted = row.snipers_amount_raw / scale_factor;
        let top10_decimal_adjusted = row.top10_amount_raw / scale_factor;

        let token_info = TokenInfo {
            bundlers_hold_percent: calculate_percentage(
                bundlers_decimal_adjusted,
                row.token_supply,
            ),
            dev_holds_percent: calculate_percentage(dev_decimal_adjusted, row.token_supply),
            num_holders: row.num_holders as i64,
            snipers_hold_percent: calculate_percentage(snipers_decimal_adjusted, row.token_supply),
            top10_holders_percent: calculate_percentage(top10_decimal_adjusted, row.token_supply),
        };

        Ok(token_info)
    }

    pub async fn get_trader_details(
        &self,
        creator: String,
        pool_address: String,
    ) -> Result<Option<TopTrader>> {
        let query = r#"
        WITH pool_info AS (
            SELECT token_base_address
            FROM pools
            WHERE pool_address = ?
        ),
        first_swap AS (
            SELECT min(slot) AS first_slot
            FROM swaps
            WHERE pool_address = ?
              AND (swap_type = 'BUY' OR swap_type = 'SELL')
        )
        SELECT
            s.creator,
            max(s.slot = fs.first_slot) AS is_sniper,
            coalesce(sumIf(s.base_amount, s.swap_type = 'BUY'), 0) AS base_bought,
            coalesce(sumIf(s.base_amount, s.swap_type = 'SELL'), 0) AS base_sold,
            coalesce(sumIf(s.quote_amount, s.swap_type = 'BUY'), 0) AS quote_bought,
            coalesce(sumIf(s.quote_amount, s.swap_type = 'SELL'), 0) AS quote_sold,
            coalesce(anyOrNull(a.amount) / pow(10, COALESCE(anyOrNull(t.decimals), 0)), 0) AS holding_base_token
        FROM swaps s
        CROSS JOIN pool_info pi
        CROSS JOIN first_swap fs
        LEFT JOIN accounts a
            ON a.owner = ?
           AND a.mint = pi.token_base_address
           AND a.amount > 0
        LEFT JOIN token_initialize_events t ON t.mint_address = pi.token_base_address
        PREWHERE s.pool_address = ? AND s.creator = ?
        WHERE s.swap_type IN ('BUY', 'SELL')
        GROUP BY s.creator
        "#;

        let rows: Option<TopTrader> = self
            .client
            .query(query)
            .bind(&pool_address) // pool_info
            .bind(&pool_address) // first_swap
            .bind(&creator) // accounts join filter
            .bind(&pool_address) // PREWHERE s.pool_address
            .bind(&creator) // PREWHERE s.creator
            .fetch_optional()
            .await?;

        Ok(rows)
    }
}
