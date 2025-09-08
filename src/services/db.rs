use std::env;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::postgres::PgListener;
use sqlx::prelude::FromRow;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::pin::pin;
use std::time::Duration;
use tracing::error;

use crate::defaults::QuoteToken;
use crate::models::account::{DBAccountState, DBTokenAccount, HolderResponse};

use crate::models::swap::{DBSwap, Swap};

use crate::routes::pool_report::ReportType;
use crate::types::candlestick::Interval;
use crate::types::pulse::{DevWalletFunding, PulseDataResponse};
use crate::types::token_info::TokenInfo;
use crate::utils::{calculate_market_cap, calculate_percentage};
use crate::{
    defaults::{SOL_TOKEN, USDC_TOKEN},
    models::{
        pool::{DBPool, Pool},
        sniper::{DevHolding, SniperSummary},
        swap::SwapType,
        token::{DBToken, Token},
    },
};

#[derive(Clone)]
pub struct DbService {
    pub pool: PgPool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OHLCV {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume_base: Decimal,
    pub volume_quote: Decimal,
    pub trades: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteTokenData {
    pub address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub logo: String,
}

/// Structure to represent the swap event data from PostgreSQL notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub creator: String,
    pub pool_address: String,
    pub hash: String,
    pub base_amount: Decimal,
    pub quote_amount: Decimal,
    pub base_reserve: Decimal,
    pub quote_reserve: Decimal,
    pub price_sol: Decimal,
    pub swap_type: SwapType,
    pub slot: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolEvent {
    pub creator: String,
    pub pool_address: String,
    pub pool_base_address: String,
    pub pool_quote_address: String,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub token_base_address: String,
    pub token_quote_address: String,
    pub initial_token_base_reserve: Decimal,
    pub initial_token_quote_reserve: Decimal,
    pub slot: i64,
    pub curve_percentage: Option<Decimal>,
    pub reversed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub hash: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolReport {
    pub bucket_start: DateTime<Utc>,
    pub buy_volume: Decimal,
    pub buy_count: i64,
    pub buyer_count: i64,
    pub sell_volume: Decimal,
    pub sell_count: i64,
    pub seller_count: i64,
    pub trader_count: i64,
    pub open_price: Option<Decimal>,
    pub close_price: Option<Decimal>,
    pub price_change_percent: Option<Decimal>,
}

#[derive(Debug, Serialize, Deserialize)]

pub struct PoolAndTokenData {
    pool_address: String,
    base_liquidity: Decimal,
    quote_liquidity: Decimal,
    marketcap_sol: Decimal,
    factory: String,
    pre_factory: Option<String>,
    curve_percentage: Option<Decimal>,
    volume_quote: Decimal,
    base_token: DBToken,
    quote_token: QuoteTokenData,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct PairInfo {
    pub pool: DBPool,
    pub base_token: DBToken,
}
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct TopTrader {
    creator: String,
    is_sniper: bool,
    base_bought: Decimal,
    base_sold: Decimal,
    quote_bought: Decimal,
    quote_sold: Decimal,
}

impl DbService {
    pub async fn init() -> Self {
        let url = env::var("DATABASE_URL").unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("failed to connect to postgres");

        Self { pool }
    }

    /// Listen to swap events from the database using the modern PgListener approach
    pub async fn listen_to_swap_events<F>(&self, mut callback: F) -> Result<(), sqlx::Error>
    where
        F: FnMut(SwapEvent) + Send + 'static,
    {
        let pool = self.pool.clone();

        tokio::spawn(async move {
            // Create a PgListener connected to the pool
            let mut listener = match PgListener::connect_with(&pool).await {
                Ok(listener) => listener,
                Err(e) => {
                    eprintln!("Failed to create PgListener: {}", e);
                    return;
                }
            };

            // Listen to the swap_inserted channel
            if let Err(e) = listener.listen_all(vec!["swap_inserted"]).await {
                eprintln!("Failed to listen to swap_inserted channel: {}", e);
                return;
            }

            // Convert to stream for easier handling
            let mut stream = listener.into_stream();

            // Process notifications
            while let Ok(Some(notification)) = stream.try_next().await {
                if notification.channel() == "swap_inserted" {
                    // Parse the JSON payload
                    if let Ok(swap_event) =
                        serde_json::from_str::<SwapEvent>(notification.payload())
                    {
                        callback(swap_event);
                    } else {
                        eprintln!(
                            "Failed to parse swap event JSON: {}",
                            notification.payload()
                        );
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn listen_to_pool_events<F>(&self, mut callback: F) -> Result<(), sqlx::Error>
    where
        F: FnMut(DBPool) + Send + 'static,
    {
        let pool = self.pool.clone();

        tokio::spawn(async move {
            let mut listener = PgListener::connect_with(&pool).await.unwrap();
            listener.listen_all(vec!["pool_inserted"]).await.unwrap();
            let mut stream = listener.into_stream();

            while let Ok(Some(notification)) = stream.try_next().await {
                if notification.channel() == "pool_inserted" {
                    if let Ok(raw_notification) =
                        serde_json::from_str::<PoolEvent>(notification.payload())
                    {
                        let db_pool = DBPool {
                            pool_address: raw_notification.pool_address,
                            creator: raw_notification.creator,
                            token_base_address: raw_notification.token_base_address,
                            token_quote_address: raw_notification.token_quote_address,
                            pool_base_address: raw_notification.pool_base_address,
                            pool_quote_address: raw_notification.pool_quote_address,
                            slot: raw_notification.slot,

                            factory: raw_notification.factory,
                            pre_factory: raw_notification.pre_factory.unwrap_or_default(),
                            initial_token_base_reserve: Decimal::from(
                                raw_notification.initial_token_base_reserve,
                            ),
                            initial_token_quote_reserve: Decimal::from(
                                raw_notification.initial_token_quote_reserve,
                            ),
                            curve_percentage: raw_notification
                                .curve_percentage
                                .map(|x| Decimal::from_f64(x.to_f64().unwrap()).unwrap()),
                            reversed: raw_notification.reversed,
                            hash: raw_notification.hash,
                            metadata: raw_notification.metadata,
                            created_at: raw_notification.created_at,
                            updated_at: raw_notification.updated_at,
                        };
                        callback(db_pool);
                    } else {
                        eprintln!(
                            "Failed to parse pool event JSON: {}",
                            notification.payload()
                        );
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn search_pools(&self, pool_address: String) -> Result<Vec<DBPool>, sqlx::Error> {
        let query = r#"
            SELECT * FROM pools
            WHERE pool_address = $1
            ORDER BY pool_address DESC
            LIMIT 10
        "#;
        let pools = sqlx::query_as::<_, DBPool>(query)
            .bind(&pool_address)
            .fetch_all(&self.pool)
            .await;
        match pools {
            Ok(pools) => {
                let mut response_pools = Vec::new();
                for pool in pools {
                    response_pools.push(DBPool::try_from(pool).unwrap());
                }
                Ok(response_pools)
            }
            Err(e) => {
                error!("Error searching pools: {}", e);
                Err(e)
            }
        }
    }

    pub async fn search_tokens(&self, search: String) -> Result<Vec<Token>, sqlx::Error> {
        let query = r#"
            SELECT * FROM tokens
            WHERE name ILIKE $1 OR symbol ILIKE $1
            ORDER BY name DESC
            LIMIT 10
        "#;
        let tokens = sqlx::query_as::<_, DBToken>(query)
            .bind(&search)
            .fetch_all(&self.pool)
            .await;
        match tokens {
            Ok(tokens) => {
                let mut response_tokens = Vec::new();
                for token in tokens {
                    response_tokens.push(Token::try_from(token).unwrap());
                }
                Ok(response_tokens)
            }
            Err(e) => {
                error!("Error searching tokens: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_pool_and_token_data(
        &self,
        address: String,
    ) -> Result<Option<PoolAndTokenData>, sqlx::Error> {
        // 1. address can be token_base_address or pool_address

        // 2. if address is pool_address, get the pool and token_base_address using JOIN

        // 3. if address is token_base_address, get the token and pool_address using JOIN

        // 4. get the quote_token_data using quote_token_address from the pool and fill it using defaults

        let query = r#"
 SELECT 
        pools.*, 
        tokens.*,
        (SELECT base_reserve FROM swaps WHERE pool_address = pools.pool_address ORDER BY created_at DESC LIMIT 1) as base_reserve,
        (SELECT quote_reserve FROM swaps WHERE pool_address = pools.pool_address ORDER BY created_at DESC LIMIT 1) as quote_reserve,
        (SELECT price_sol FROM swaps WHERE pool_address = pools.pool_address ORDER BY created_at DESC LIMIT 1) as price_sol,
        COALESCE((SELECT buy_volume  FROM swaps_24h WHERE pool_address = pools.pool_address ORDER BY bucket_start DESC LIMIT 1), 0) as buy_volume,
        COALESCE((SELECT sell_volume FROM swaps_24h WHERE pool_address = pools.pool_address ORDER BY bucket_start DESC LIMIT 1), 0) as sell_volume
    FROM pools
    JOIN tokens ON pools.token_base_address = tokens.mint_address
    WHERE pools.pool_address = $1 OR pools.token_base_address = $1
        "#;
        let pool = sqlx::query(query)
            .bind(&address)
            .fetch_optional(&self.pool)
            .await?;
        match pool {
            Some(row) => {
                println!("{:?}", row);
                let pool = DBPool {
                    pool_address: row.get::<String, _>("pool_address"),
                    factory: row.get("factory"),
                    pre_factory: row.get("pre_factory"),
                    reversed: row.get("reversed"),
                    token_base_address: row.get::<String, _>("token_base_address"),
                    token_quote_address: row.get::<String, _>("token_quote_address"),

                    pool_base_address: row.get::<String, _>("pool_base_address"),
                    pool_quote_address: row.get::<String, _>("pool_quote_address"),
                    curve_percentage: row.get("curve_percentage"),
                    initial_token_base_reserve: row.get("initial_token_base_reserve"),
                    initial_token_quote_reserve: row.get("initial_token_quote_reserve"),
                    slot: row.get("slot"),
                    creator: row.get::<String, _>("creator"),
                    hash: row.get::<String, _>("hash"),
                    metadata: row.get("metadata"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                };

                // Build ResponseToken
                let base_token = DBToken {
                    mint_address: row.get::<String, _>("mint_address"),
                    name: row.get("name"),
                    symbol: row.get("symbol"),
                    decimals: row.get::<i16, _>("decimals"),
                    uri: row.get("uri"),
                    image: row.get("image"),
                    twitter: row.get("twitter"),
                    telegram: row.get("telegram"),
                    website: row.get("website"),
                    supply: row.get::<rust_decimal::Decimal, _>("supply"),
                    slot: row.get("slot"),
                    mint_authority: row.get("mint_authority"),
                    freeze_authority: row.get("freeze_authority"),
                    hash: row.get::<String, _>("hash"),
                    program_id: row.get::<String, _>("program_id"),
                };
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
                    panic!("Quote token not found");
                };
                Ok(Some(PoolAndTokenData {
                    pool_address: pool.pool_address,
                    base_liquidity: row.get("base_reserve"),
                    quote_liquidity: row.get("quote_reserve"),
                    marketcap_sol: row.get::<Decimal, _>("price_sol") * base_token.supply,
                    factory: pool.factory,
                    pre_factory: Some(pool.pre_factory),
                    curve_percentage: pool.curve_percentage,
                    volume_quote: row.get::<Decimal, _>("buy_volume")
                        + row.get::<Decimal, _>("sell_volume"),
                    base_token,
                    quote_token,
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn get_last_transaction(
        &self,
        pool_address: String,
    ) -> Result<Option<Swap>, sqlx::Error> {
        let query = r#"
            SELECT * FROM swaps
            WHERE pool_address = $1
            ORDER BY slot DESC, created_at DESC
            LIMIT 1
        "#;

        let row: Option<DBSwap> = sqlx::query_as(query)
            .bind(&pool_address)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some(row) => Ok(Some(Swap::try_from(row).unwrap())),
            None => Ok(None),
        }
    }

    pub async fn get_pool_report(
        &self,
        pool_address: String,
        report_type: ReportType,
    ) -> Result<Option<Vec<PoolReport>>, sqlx::Error> {
        let (table_suffix, interval_literal) = match report_type {
            ReportType::OneMinute => ("1m", "1 minute"),
            ReportType::FiveMinutes => ("5m", "5 minutes"),
            ReportType::OneHour => ("1h", "1 hour"),
            ReportType::SixHours => ("6h", "6 hours"),
            ReportType::OneDay => ("24h", "24 hours"),
        };

        let query = format!(
            r#"
            SELECT 
             bucket_start,
  COALESCE(buy_volume, 0) as buy_volume,
         COALESCE(buy_count::BIGINT, 0) as buy_count,
         COALESCE(sell_volume, 0) as sell_volume,
         COALESCE(sell_count::BIGINT, 0) as sell_count,
  open_price,
  close_price,
   COALESCE(distinct_count(traders_hll), 0) AS trader_count,
         COALESCE(distinct_count(buyers_hll), 0) AS buyer_count,
         COALESCE(distinct_count(sellers_hll), 0) AS seller_count,
CASE 
             WHEN open_price IS NOT NULL AND open_price != 0 
             THEN (close_price - open_price) / open_price * 100 
             ELSE NULL 
         END as price_change_percent
            FROM swaps_{table_suffix}
            WHERE pool_address = $1
            AND bucket_start >= NOW() - INTERVAL '24 hours'
            ORDER BY bucket_start DESC
            "#
        );
        let pool_reports = sqlx::query(&query)
            .bind(&pool_address)
            .fetch_all(&self.pool)
            .await;

        match pool_reports {
            Ok(rows) => {
                let mut pool_reports = Vec::new();
                for row in rows {
                    let pool_report = PoolReport {
                        bucket_start: row.get("bucket_start"),
                        buy_volume: row.get("buy_volume"),
                        buy_count: row.get("buy_count"),
                        buyer_count: row.get("buyer_count"),
                        sell_volume: row.get("sell_volume"),
                        sell_count: row.get("sell_count"),
                        seller_count: row.get("seller_count"),
                        trader_count: row.get("trader_count"),
                        open_price: row.get("open_price"),
                        close_price: row.get("close_price"),
                        price_change_percent: row.get("price_change_percent"),
                    };
                    pool_reports.push(pool_report);
                }
                Ok(Some(pool_reports))
            }

            Err(e) => {
                error!("Error getting pool report: {}", e);
                Ok(None)
            }
        }
    }

    pub async fn get_pool_swaps(
        &self,
        pool_address: String,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
    ) -> Result<Vec<DBSwap>, sqlx::Error> {
        let query = r#"
    SELECT * FROM swaps
    WHERE pool_address = $1
    AND created_at >= $2 
    AND created_at <= $3
    ORDER BY created_at DESC
    LIMIT 20
    "#;
        let swaps = sqlx::query_as::<_, DBSwap>(query)
            .bind(&pool_address)
            .bind(start_date)
            .bind(end_date)
            .fetch_all(&self.pool)
            .await;
        match swaps {
            Ok(swaps) => {
                let mut response_swaps = Vec::new();
                for swap in swaps {
                    response_swaps.push(DBSwap::try_from(swap).unwrap());
                }
                Ok(response_swaps
                    .into_iter()
                    .map(|swap: DBSwap| DBSwap::try_from(swap).unwrap())
                    .collect())
            }
            Err(e) => {
                error!("Error getting pool swaps: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_pair_info(&self, pool_address: String) -> Result<PairInfo, sqlx::Error> {
        let query = r#"
        SELECT 
            pools.*, 
            tokens.hash AS token_hash,
            tokens.mint_address,
            tokens.name,
            tokens.symbol,
            tokens.decimals,
            tokens.uri,
            tokens.supply,
            tokens.slot,
            tokens.mint_authority,
            tokens.freeze_authority,
            tokens.image,
            tokens.twitter,
            tokens.telegram,
            tokens.website,
            tokens.program_id
        FROM pools
        JOIN tokens ON pools.token_base_address = tokens.mint_address
        WHERE pools.pool_address = $1 OR pools.token_base_address = $1
        "#;
        let row = sqlx::query(query)
            .bind(&pool_address)
            .fetch_one(&self.pool)
            .await?;

        let pair_info = PairInfo {
            pool: DBPool {
                pool_address: row.try_get::<String, _>("pool_address")?,
                pool_base_address: row.try_get::<String, _>("pool_base_address")?,
                pool_quote_address: row.try_get::<String, _>("pool_quote_address")?,
                token_base_address: row.try_get::<String, _>("pool_quote_address")?,
                token_quote_address: row.try_get::<String, _>("token_quote_address")?,
                creator: row.try_get::<String, _>("creator")?,
                hash: row.try_get::<String, _>("hash")?,
                initial_token_base_reserve: row
                    .try_get::<Decimal, _>("initial_token_base_reserve")?,
                initial_token_quote_reserve: row
                    .try_get::<Decimal, _>("initial_token_quote_reserve")?,
                factory: row.try_get::<String, _>("factory")?,
                pre_factory: row.try_get::<String, _>("pre_factory")?,
                curve_percentage: row.try_get::<Option<Decimal>, _>("curve_percentage")?,
                created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
                updated_at: row.try_get::<DateTime<Utc>, _>("updated_at")?,
                reversed: row.try_get::<bool, _>("reversed")?,
                slot: row.try_get::<i64, _>("slot")?,
                metadata: row.try_get::<Option<Value>, _>("metadata")?,
            },
            base_token: DBToken {
                mint_address: row.try_get::<String, _>("mint_address")?,
                name: row.try_get("name")?,
                symbol: row.try_get("symbol")?,
                decimals: row.try_get::<i16, _>("decimals")?,
                uri: row.try_get("uri")?,
                supply: row.try_get("supply")?,
                slot: row.try_get("slot")?,
                mint_authority: row.try_get("mint_authority")?,
                freeze_authority: row.try_get("freeze_authority")?,
                hash: row.try_get::<String, _>("token_hash")?,
                image: row.try_get("image")?,
                twitter: row.try_get("twitter")?,
                telegram: row.try_get("telegram")?,
                website: row.try_get("website")?,
                program_id: row.try_get::<String, _>("program_id")?,
            },
        };
        Ok(pair_info)
    }

    pub async fn get_top_traders(
        &self,
        pool_address: String,
    ) -> Result<Vec<TopTrader>, sqlx::Error> {
        let query = r#"
        WITH first_swap AS (
            SELECT slot
            FROM swaps
            WHERE pool_address = $1
            ORDER BY slot ASC
            LIMIT 1
        )
        SELECT 
            s.creator,
            SUM(CASE WHEN s.base_amount > 0 THEN s.base_amount ELSE 0 END) as base_bought,
            SUM(CASE WHEN s.quote_amount > 0 THEN s.quote_amount ELSE 0 END) as quote_bought,
            SUM(CASE WHEN s.base_amount < 0 THEN -s.base_amount ELSE 0 END) as base_sold,
            SUM(CASE WHEN s.quote_amount < 0 THEN -s.quote_amount ELSE 0 END) as quote_sold,
            BOOL_OR(
                s.slot = f.slot
            ) as is_sniper
        FROM swaps s
        CROSS JOIN first_swap f
        WHERE s.pool_address = $1
        GROUP BY s.creator
        ORDER BY base_bought DESC
        LIMIT 20
        "#;
        let swaps = sqlx::query(query)
            .bind(&pool_address)
            .fetch_all(&self.pool)
            .await;
        match swaps {
            Ok(swaps) => {
                let mut top_traders = Vec::new();
                for swap in swaps {
                    top_traders.push(TopTrader {
                        creator: swap.get::<String, _>("creator"),
                        base_bought: swap.get("base_bought"),
                        base_sold: swap.get("base_sold"),
                        quote_bought: swap.get("quote_bought"),
                        quote_sold: swap.get("quote_sold"),
                        is_sniper: swap.get("is_sniper"),
                    });
                }
                Ok(top_traders)
            }
            Err(e) => {
                error!("Error getting top traders: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_holders(&self, mint: String) -> Result<Vec<HolderResponse>, sqlx::Error> {
        let query = r#"
        SELECT 
            a.*, 
            t.decimals,
            (a.amount::numeric / POWER(10, t.decimals)) AS normalized_amount
        FROM accounts a
        JOIN tokens t ON a.mint = t.mint_address
        WHERE a.mint = $1 AND a.amount > 0
        ORDER BY normalized_amount DESC
        LIMIT 50
        "#;
        let holders = sqlx::query(query).bind(&mint).fetch_all(&self.pool).await;
        match holders {
            Ok(holders) => {
                let mut holder_responses = Vec::new();
                for holder in holders {
                    holder_responses.push(HolderResponse {
                        address: holder.try_get::<String, _>("owner")?,
                        account: holder.try_get::<String, _>("account")?,
                        amount: Decimal::from_f64(holder.try_get::<f64, _>("normalized_amount")?)
                            .unwrap(),
                        mint: holder.try_get::<String, _>("mint")?,
                        state: holder.try_get::<DBAccountState, _>("state")?,
                        delegated_amount: holder.try_get::<Decimal, _>("delegated_amount")?,
                    });
                }
                Ok(holder_responses)
            }
            Err(e) => {
                error!("Error getting holders: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_candlestick(
        &self,
        pool_address: String,
        interval: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: i32,
    ) -> Result<Vec<OHLCV>, sqlx::Error> {
        let query = format!(
            r#"
        SELECT * FROM candles_{interval}
        WHERE pool_address = $1
        AND bucket_start >= $2
        AND bucket_start <= $3
        ORDER BY bucket_start DESC
        LIMIT $4
        "#
        );
        let candles = sqlx::query(&query)
            .bind(&pool_address)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await;
        match candles {
            Ok(candles) => {
                let mut ohlcvs = Vec::new();
                for candle in candles {
                    println!("candle: {:?}", candle);
                    ohlcvs.push(OHLCV {
                        timestamp: candle.get("bucket_start"),
                        open: candle.get("open"),
                        high: candle.get("high"),
                        low: candle.get("low"),
                        close: candle.get("close"),
                        volume_base: candle.get("volume_base"),
                        volume_quote: candle.get("volume_quote"),
                        trades: candle.get::<i64, _>("trades"),
                    });
                }
                Ok(ohlcvs)
            }
            Err(e) => {
                error!("Error getting candlestick: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_24hr_recent_pools(&self) -> Result<Vec<DBPool>, sqlx::Error> {
        let query = r#"
        SELECT * FROM pools
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        ORDER BY created_at DESC
        "#;
        let pools = sqlx::query_as::<_, DBPool>(query)
            .fetch_all(&self.pool)
            .await;
        match pools {
            Ok(pools) => Ok(pools),
            Err(e) => {
                error!("Error getting 24hr recent pools: {}", e);
                Err(e)
            }
        }
    }
    pub async fn get_batch_pool_data(
        &self,
        pool_addresses: &[String],
    ) -> Result<Vec<PulseDataResponse>, sqlx::Error> {
        let pool_hex_strings: Vec<String> = pool_addresses
            .iter()
            .map(|addr| format!("\\x{}", hex::encode(addr)))
            .collect();

        let query = r#"
            WITH target_pools AS (
                SELECT unnest($1) AS pool_address
            ),
            all_pools AS (
                SELECT
                    p.pool_address,
                    p.creator,
                    p.token_base_address,
                    p.token_quote_address,
                    p.pool_base_address,
                    p.pool_quote_address,
                    p.factory,
                    p.created_at,
                    p.initial_token_base_reserve,
                    p.initial_token_quote_reserve,
                    p.curve_percentage
                FROM pools p
                JOIN target_pools tp ON tp.pool_address = p.pool_address
            ),
tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply::numeric AS token_supply,
    power(10::numeric, t.decimals)::numeric AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    ls.latest_base_reserve,
    ls.latest_quote_reserve,
    ls.latest_price_sol
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      s.base_reserve  AS latest_base_reserve,
      s.quote_reserve AS latest_quote_reserve,
      s.price_sol     AS latest_price_sol
    FROM swaps s
    WHERE s.pool_address = r.pool_address
    ORDER BY s.created_at DESC
    LIMIT 1
  ) ls ON TRUE
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM all_pools r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  GROUP BY r.pool_address
),
top10_holders AS (
  SELECT pool_address, SUM(amount) AS top10_amount_raw
  FROM (
    SELECT r.pool_address, a.amount,
           ROW_NUMBER() OVER (PARTITION BY r.pool_address ORDER BY a.amount DESC) AS rn
    FROM all_pools r
    JOIN accounts a
      ON a.mint = r.token_base_address
     AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  ) x
  WHERE rn <= 10
  GROUP BY pool_address
),
dev_hold AS (
  SELECT
    r.pool_address,
    coalesce(max(a.amount), 0) AS dev_amount_raw
  FROM all_pools r
  LEFT JOIN accounts a
    ON a.mint  = r.token_base_address
   AND a.owner = r.creator
   AND a.owner <> r.pool_address
  GROUP BY r.pool_address
),
snipers_holds AS (
  SELECT s.pool_address,
         COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
  FROM swaps s
  JOIN all_pools r ON r.pool_address = s.pool_address
  WHERE s.swap_type = 'BUY'
    AND s.creator <> r.pool_address
    AND s.creator <> r.pool_base_address
    AND s.creator <> r.pool_quote_address
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    df.source,
    df.destination,
    df.amount,
    df.hash,
    df.created_at
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      ts.source,
      ts.destination,
      ts.amount,
      ts.hash,
      ts.created_at
    FROM transfer_sol ts
    WHERE ts.destination = r.creator
    ORDER BY ts.created_at ASC
    LIMIT 1
  ) df ON TRUE
),
migration AS (
  SELECT r.creator,
         count(*) FILTER (WHERE p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM all_pools r
  LEFT JOIN pools p2 ON p2.creator = r.creator
  GROUP BY r.creator
),
vol_24h AS (         -- choose one source
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         SUM(s.buy_count)::int8            AS num_buys,
         SUM(s.sell_count)::int8           AS num_sells,
         SUM(s.buy_count + s.sell_count)::int8 AS num_txns
  FROM swaps_5m s
  JOIN all_pools r USING (pool_address)
  WHERE  s.bucket_start <  now() - interval '5 minutes'
  GROUP BY s.pool_address
)
SELECT
  r.pool_address,
  r.creator,
  r.token_base_address,
  r.token_quote_address,
  r.factory,
  r.created_at,
  r.initial_token_base_reserve,
  r.initial_token_quote_reserve,
  coalesce(r.curve_percentage, 0) AS bonding_curve_percent,

  -- token meta
  t.name, t.symbol, t.image, t.decimals, t.website, t.twitter, t.telegram, t.mint_address,
  t.token_supply,
  t.scale_factor,

  -- liquidity/price (fallback to initial if no swaps yet)
  coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) AS liquidity_sol,
  coalesce(ls.latest_base_reserve,  r.initial_token_base_reserve)  AS liquidity_token,
  coalesce(ls.latest_price_sol, 0)                                 AS current_price_sol,

  -- holders
  coalesce(h.num_holders, 0)                                       AS num_holders,

  -- raw amounts for calculation in Rust
  coalesce(th.top10_amount_raw, 0)                                 AS top10_amount_raw,
  coalesce(d.dev_amount_raw, 0)                                    AS dev_amount_raw,
  coalesce(sh.snipers_amount_raw, 0)                               AS snipers_amount_raw,

  -- migrations
  coalesce(m.migration_count, 0)                                   AS migration_count,

  coalesce(v.volume_sol, 0) AS volume_sol,
coalesce(v.num_txns,   0) AS num_txns,
coalesce(v.num_buys,   0) AS num_buys,
coalesce(v.num_sells,  0) AS num_sells,

  -- dev wallet funding (first transfer)
  df.source       AS funding_wallet_address,
  df.destination  AS wallet_address,
  df.amount       AS amount_sol,
  df.hash         AS transfer_hash,
  df.created_at   AS funded_at

FROM all_pools r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator;
        "#;

        let pools = sqlx::query(query)
            .bind(&pool_hex_strings)
            .fetch_all(&self.pool)
            .await?;

        // let mut pool_data = Vec::new();
        let mut data = Vec::new();
        for pool in pools.into_iter() {
            // Get raw values from database
            let top10_amount_raw: Decimal = pool.try_get::<Decimal, _>("top10_amount_raw")?;
            let dev_amount_raw: Decimal = pool.try_get::<Decimal, _>("dev_amount_raw")?;
            let snipers_amount_raw: Decimal = pool.try_get::<Decimal, _>("snipers_amount_raw")?;
            let scale_factor = match pool.try_get::<Option<Decimal>, _>("scale_factor")? {
                Some(sf) => sf,
                None => continue, // Skip this pool if scale_factor is null
            };

            let token_supply = match pool.try_get::<Option<Decimal>, _>("token_supply")? {
                Some(ts) => ts,
                None => continue, // Skip this pool if token_supply is null
            };
            let current_price: Decimal = pool.try_get::<Decimal, _>("current_price_sol")?;

            // Calculate percentages in Rust
            let top10_holders_percent =
                calculate_percentage(top10_amount_raw, scale_factor, token_supply);
            let dev_holds_percent =
                calculate_percentage(dev_amount_raw, scale_factor, token_supply);
            let snipers_holds_percent =
                calculate_percentage(snipers_amount_raw, scale_factor, token_supply);

            // Calculate market cap in Rust
            let market_cap_sol = calculate_market_cap(current_price, token_supply);

            let pulse_data: PulseDataResponse = PulseDataResponse {
                pair_address: pool.get::<String, _>("pool_address"),
                liquidity_sol: pool.try_get::<Decimal, _>("liquidity_sol")?,
                liquidity_token: pool.try_get::<Decimal, _>("liquidity_token")?,
                token_address: pool.get::<String, _>("mint_address"),
                bonding_curve_percent: pool.try_get::<Decimal, _>("bonding_curve_percent")?,
                token_name: pool.try_get("name")?,
                token_symbol: pool.try_get("symbol")?,
                token_decimals: pool.try_get::<i16, _>("decimals")? as u8, // Fix type mismatch
                creator: pool.try_get::<String, _>("creator")?,
                protocol: pool.try_get("factory")?,
                website: pool.try_get("website")?,
                twitter: pool.try_get("twitter")?,
                telegram: pool.try_get("telegram")?,
                top10_holders_percent,
                dev_holds_percent,
                snipers_holds_percent,
                volume_sol: pool.try_get::<Decimal, _>("volume_sol")?,
                market_cap_sol,
                created_at: pool.try_get::<DateTime<Utc>, _>("created_at")?,
                migration_count: pool.try_get("migration_count")?,
                num_txns: pool.try_get("num_txns")?,
                num_buys: pool.try_get("num_buys")?,
                num_sells: pool.try_get("num_sells")?,
                num_holders: pool.try_get("num_holders")?,
                supply: pool.try_get("token_supply")?,
                token_image: pool.try_get("image")?,
                dev_wallet_funding: if let Some(funding_wallet) =
                    pool.try_get::<Option<String>, _>("funding_wallet_address")?
                {
                    Some(DevWalletFunding {
                        funding_wallet_address: funding_wallet,
                        wallet_address: pool.try_get::<String, _>("wallet_address")?,
                        amount_sol: pool.try_get("amount_sol")?,
                        hash: pool.try_get::<String, _>("transfer_hash")?,
                        funded_at: pool.try_get::<DateTime<Utc>, _>("funded_at")?,
                    })
                } else {
                    None
                },
            };
            data.push(pulse_data);
        }
        Ok(data)
    }

    pub async fn get_token_info(&self, pool_address: String) -> Result<TokenInfo, sqlx::Error> {
        let query = r#"
        WITH pool_info AS (
          SELECT p.pool_address, p.token_base_address, p.creator,
       p.pool_base_address, p.pool_quote_address,p.slot
            FROM pools p 
            WHERE p.pool_address = $1
        ),
        top10_holders AS (
            SELECT sum(amount) AS top10_amount_raw
            FROM (
                SELECT
                    a.amount,
                    row_number() OVER (ORDER BY a.amount DESC) AS rn
                FROM pool_info pi
                JOIN accounts a ON a.mint = pi.token_base_address
                WHERE a.owner NOT IN (pi.pool_address, pi.pool_base_address, pi.pool_quote_address)
            ) x
            WHERE rn <= 10
        ),
        dev_hold AS (
            SELECT coalesce(a.amount, 0) AS dev_amount_raw
            FROM pool_info pi
            LEFT JOIN accounts a
                ON a.mint = pi.token_base_address
                AND a.owner = pi.creator
        ),
        bundlers_holds AS (
            SELECT coalesce(sum(s.base_amount), 0) AS bundlers_amount_raw
            FROM pool_info pi
            JOIN swaps s ON s.pool_address = pi.pool_address
            WHERE s.swap_type = 'BUY'
                AND s.slot = pi.slot  -- Only buys after pool creation
                AND s.creator NOT IN (pi.pool_address, pi.pool_base_address, pi.pool_quote_address)
        ),
      snipers_holds AS (
            SELECT coalesce(sum(s.base_amount), 0) AS snipers_amount_raw
            FROM pool_info pi
            JOIN swaps s ON s.pool_address = pi.pool_address
            WHERE s.swap_type = 'BUY'
                AND s.slot = pi.slot + 1  -- Only buys after pool creation
                AND s.creator NOT IN (pi.pool_address, pi.pool_base_address, pi.pool_quote_address)
        ),
        total_holders AS (
            SELECT count(DISTINCT a.owner) AS num_holders
            FROM pool_info pi
            JOIN accounts a ON a.mint = pi.token_base_address
            WHERE a.owner NOT IN (pi.pool_address, pi.pool_base_address, pi.pool_quote_address)
        ),
        tok AS (
            SELECT 
                t.mint_address,
                t.decimals,
                t.supply::numeric AS token_supply,
                power(10::numeric, t.decimals)::numeric AS scale_factor
            FROM tokens t
            JOIN pool_info pi ON t.mint_address = pi.token_base_address
        )
        SELECT 
            COALESCE(th.top10_amount_raw, 0) AS top10_amount_raw,
            COALESCE(d.dev_amount_raw, 0) AS dev_amount_raw,
            COALESCE(sh.snipers_amount_raw, 0) AS snipers_amount_raw,
            COALESCE(t.num_holders, 0) AS num_holders,
            COALESCE(tk.token_supply, 0) AS token_supply,
            COALESCE(tk.scale_factor, 1) AS scale_factor,
            COALESCE(bh.bundlers_amount_raw, 0) AS bundlers_amount_raw
        FROM pool_info pi
        CROSS JOIN top10_holders th
        CROSS JOIN dev_hold d
        CROSS JOIN snipers_holds sh
        CROSS JOIN bundlers_holds bh
        CROSS JOIN total_holders t
        CROSS JOIN tok tk

    "#;
        let token_info = sqlx::query(query)
            .bind(pool_address)
            .fetch_one(&self.pool)
            .await?;

        let scale_factor: Decimal = token_info.try_get::<Decimal, _>("scale_factor")?;
        let token_supply: Decimal = token_info.try_get::<Decimal, _>("token_supply")?;
        let top10_amount_raw: Decimal = token_info.try_get::<Decimal, _>("top10_amount_raw")?;
        let dev_amount_raw: Decimal = token_info.try_get::<Decimal, _>("dev_amount_raw")?;
        let snipers_amount_raw: Decimal = token_info.try_get::<Decimal, _>("snipers_amount_raw")?;
        let num_holders: i64 = token_info.try_get::<i64, _>("num_holders")?;
        let bundlers_amount_raw: Decimal =
            token_info.try_get::<Decimal, _>("bundlers_amount_raw")?;
        let token_info = TokenInfo {
            bundlers_hold_percent: calculate_percentage(
                bundlers_amount_raw,
                scale_factor,
                token_supply,
            ),
            dev_holds_percent: calculate_percentage(dev_amount_raw, scale_factor, token_supply),
            num_holders,
            snipers_hold_percent: calculate_percentage(
                snipers_amount_raw,
                scale_factor,
                token_supply,
            ),
            top10_holders_percent: calculate_percentage(
                top10_amount_raw,
                scale_factor,
                token_supply,
            ),
        };
        Ok(token_info)
    }

    pub async fn get_trader_details(
        &self,
        creator: String,
        pool_address: String,
    ) -> Result<TopTrader, sqlx::Error> {
        let query = r#"
SELECT
  s.creator,
  COALESCE(SUM(s.base_amount)  FILTER (WHERE s.base_amount  > 0), 0) AS base_bought,
  COALESCE(SUM(s.quote_amount) FILTER (WHERE s.quote_amount > 0), 0) AS quote_bought,
  COALESCE(-SUM(s.base_amount)  FILTER (WHERE s.base_amount  < 0), 0) AS base_sold,
  COALESCE(-SUM(s.quote_amount) FILTER (WHERE s.quote_amount < 0), 0) AS quote_sold,
  COALESCE(BOOL_OR(
    s.slot = (SELECT MIN(slot) FROM swaps WHERE pool_address = $1)
  ), FALSE) AS is_sniper
FROM swaps s
WHERE s.pool_address = $1
  AND s.creator      = $2
GROUP BY s.creator;

"#;
        let row = sqlx::query(query)
            .bind(pool_address)
            .bind(creator)
            .fetch_one(&self.pool)
            .await?;

        Ok(TopTrader {
            base_bought: row.try_get::<Decimal, _>("base_bought")?,
            base_sold: row.try_get::<Decimal, _>("base_sold")?,
            creator: row.try_get::<String, _>("creator")?,
            is_sniper: row.try_get::<bool, _>("is_sniper")?,
            quote_bought: row.try_get::<Decimal, _>("quote_bought")?,
            quote_sold: row.try_get::<Decimal, _>("quote_sold")?,
        })
    }
}
