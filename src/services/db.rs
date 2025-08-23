use std::env;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::postgres::PgListener;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::pin::pin;
use std::time::Duration;
use tracing::error;

use crate::defaults::QuoteToken;
use crate::models::account::{DBAccountState, DBTokenAccount, HolderResponse};
use crate::models::pool::ResponsePool;
use crate::models::swap::{DBSwap, ResponseSwap, Swap};
use crate::models::token::ResponseToken;
use crate::routes::pool_report::ReportType;
use crate::types::candlestick::Interval;
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
    pub pool_address: String,
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
    base_token: ResponseToken,
    quote_token: QuoteTokenData,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct PairInfo {
    pool: ResponsePool,
    base_token: ResponseToken,
    quote_token: QuoteTokenData,
}
#[derive(Debug, Serialize, Deserialize)]
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
                            pool_address: hex::decode(&raw_notification.pool_address)
                                .map_err(|_| "parse pool address".to_string())
                                .unwrap(),
                            creator: hex::decode(&raw_notification.creator)
                                .map_err(|_| "parse creator".to_string())
                                .unwrap(),
                            token_base_address: hex::decode(&raw_notification.token_base_address)
                                .map_err(|_| "parse token base address".to_string())
                                .unwrap(),
                            token_quote_address: hex::decode(&raw_notification.token_quote_address)
                                .map_err(|_| "parse token quote address".to_string())
                                .unwrap(),
                            pool_base_address: hex::decode(&raw_notification.pool_base_address)
                                .map_err(|_| "parse pool base address".to_string())
                                .unwrap(),
                            pool_quote_address: hex::decode(&raw_notification.pool_quote_address)
                                .map_err(|_| "parse pool quote address".to_string())
                                .unwrap(),
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
                            hash: hex::decode(&raw_notification.hash)
                                .map_err(|_| "parse hash".to_string())
                                .unwrap(),
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

    pub async fn search_pools(
        &self,
        pool_address: Vec<u8>,
    ) -> Result<Vec<ResponsePool>, sqlx::Error> {
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
                    response_pools.push(ResponsePool::try_from(pool).unwrap());
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
        address: Vec<u8>,
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
        (SELECT buy_volume FROM swaps_24h WHERE pool_address = pools.pool_address ORDER BY bucket_start DESC LIMIT 1) as buy_volume,
        (SELECT sell_volume FROM swaps_24h WHERE pool_address = pools.pool_address ORDER BY bucket_start DESC LIMIT 1) as sell_volume
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
                let pool = ResponsePool {
                    pool_address: bs58::encode(&row.get::<Vec<u8>, _>("pool_address"))
                        .into_string(),
                    factory: row.get("factory"),
                    pre_factory: row.get("pre_factory"),
                    reversed: row.get("reversed"),
                    token_base_address: bs58::encode(&row.get::<Vec<u8>, _>("token_base_address"))
                        .into_string(),
                    token_quote_address: bs58::encode(
                        &row.get::<Vec<u8>, _>("token_quote_address"),
                    )
                    .into_string(),
                    pool_base_address: bs58::encode(&row.get::<Vec<u8>, _>("pool_base_address"))
                        .into_string(),
                    pool_quote_address: bs58::encode(&row.get::<Vec<u8>, _>("pool_quote_address"))
                        .into_string(),
                    curve_percentage: row.get("curve_percentage"),
                    initial_token_base_reserve: row.get("initial_token_base_reserve"),
                    initial_token_quote_reserve: row.get("initial_token_quote_reserve"),
                    slot: row.get("slot"),
                    creator: bs58::encode(&row.get::<Vec<u8>, _>("creator")).into_string(),
                    hash: bs58::encode(&row.get::<Vec<u8>, _>("hash")).into_string(),
                    metadata: row.get("metadata"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                };

                // Build ResponseToken
                let base_token = ResponseToken {
                    mint_address: bs58::encode(&row.get::<Vec<u8>, _>("mint_address"))
                        .into_string(),
                    name: row.get("name"),
                    symbol: row.get("symbol"),
                    decimals: row.get::<i16, _>("decimals") as u8,
                    uri: row.get("uri"),
                    image: row.get("image"),
                    twitter: row.get("twitter"),
                    telegram: row.get("telegram"),
                    website: row.get("website"),
                    supply: row.get::<rust_decimal::Decimal, _>("supply"),
                    slot: row.get("slot"),
                    mint_authority: row.get("mint_authority"),
                    freeze_authority: row.get("freeze_authority"),
                    hash: bs58::encode(&row.get::<Vec<u8>, _>("hash")).into_string(),
                    program_id: bs58::encode(&row.get::<Vec<u8>, _>("program_id")).into_string(),
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
                    pre_factory: pool.pre_factory,
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
        pool_address: Vec<u8>,
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
        pool_address: Vec<u8>,
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
                        pool_address: bs58::encode(&pool_address).into_string(),
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
        pool_address: Vec<u8>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
    ) -> Result<Vec<ResponseSwap>, sqlx::Error> {
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
                    response_swaps.push(ResponseSwap::try_from(swap).unwrap());
                }
                Ok(response_swaps
                    .into_iter()
                    .map(|swap: ResponseSwap| ResponseSwap::try_from(swap).unwrap())
                    .collect())
            }
            Err(e) => {
                error!("Error getting pool swaps: {}", e);
                Err(e)
            }
        }
    }

    pub async fn get_pair_info(
        &self,
        pool_address: Vec<u8>,
    ) -> Result<Option<PairInfo>, sqlx::Error> {
        let query = r#"
        SELECT 
            pools.*, 
            tokens.*
        FROM pools
        JOIN tokens ON pools.token_base_address = tokens.mint_address
        WHERE pools.pool_address = $1 OR pools.token_base_address = $1
        "#;
        let pool = sqlx::query(query)
            .bind(&pool_address)
            .fetch_optional(&self.pool)
            .await?;

        match pool {
            Some(row) => Ok(Some(PairInfo {
                pool: ResponsePool {
                    pool_address: bs58::encode(&row.get::<Vec<u8>, _>("pool_address"))
                        .into_string(),
                    factory: row.get("factory"),
                    pre_factory: row.get("pre_factory"),
                    reversed: row.get("reversed"),
                    token_base_address: bs58::encode(&row.get::<Vec<u8>, _>("token_base_address"))
                        .into_string(),
                    pool_base_address: bs58::encode(&row.get::<Vec<u8>, _>("pool_base_address"))
                        .into_string(),
                    pool_quote_address: bs58::encode(&row.get::<Vec<u8>, _>("pool_quote_address"))
                        .into_string(),
                    token_quote_address: bs58::encode(
                        &row.get::<Vec<u8>, _>("token_quote_address"),
                    )
                    .into_string(),
                    curve_percentage: row.get("curve_percentage"),
                    initial_token_base_reserve: row.get("initial_token_base_reserve"),
                    initial_token_quote_reserve: row.get("initial_token_quote_reserve"),
                    slot: row.get("slot"),
                    creator: bs58::encode(&row.get::<Vec<u8>, _>("creator")).into_string(),
                    hash: bs58::encode(&row.get::<Vec<u8>, _>("hash")).into_string(),
                    metadata: row.get("metadata"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                },
                base_token: ResponseToken {
                    mint_address: bs58::encode(&row.get::<Vec<u8>, _>("mint_address"))
                        .into_string(),
                    name: row.get("name"),
                    symbol: row.get("symbol"),
                    decimals: row.get::<i16, _>("decimals") as u8,
                    uri: row.get("uri"),
                    supply: row.get("supply"),
                    slot: row.get("slot"),
                    mint_authority: row.get("mint_authority"),
                    freeze_authority: row.get("freeze_authority"),
                    hash: bs58::encode(&row.get::<Vec<u8>, _>("hash")).into_string(),
                    image: row.get("image"),
                    twitter: row.get("twitter"),
                    telegram: row.get("telegram"),
                    website: row.get("website"),
                    program_id: bs58::encode(&row.get::<Vec<u8>, _>("program_id")).into_string(),
                },
                quote_token: if bs58::encode(&row.get::<Vec<u8>, _>("token_quote_address"))
                    .into_string()
                    == SOL_TOKEN.address
                {
                    QuoteTokenData {
                        address: SOL_TOKEN.address.to_string(),
                        name: SOL_TOKEN.name.to_string(),
                        symbol: SOL_TOKEN.symbol.to_string(),
                        decimals: SOL_TOKEN.decimals,
                        logo: SOL_TOKEN.logo.to_string(),
                    }
                } else if bs58::encode(&row.get::<Vec<u8>, _>("token_quote_address")).into_string()
                    == USDC_TOKEN.address
                {
                    QuoteTokenData {
                        address: USDC_TOKEN.address.to_string(),
                        name: USDC_TOKEN.name.to_string(),
                        symbol: USDC_TOKEN.symbol.to_string(),
                        decimals: USDC_TOKEN.decimals,
                        logo: USDC_TOKEN.logo.to_string(),
                    }
                } else {
                    panic!("Quote token not found");
                },
            })),
            None => Ok(None),
        }
    }

    pub async fn get_top_traders(
        &self,
        pool_address: Vec<u8>,
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
                        creator: bs58::encode(&swap.get::<Vec<u8>, _>("creator")).into_string(),
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

    pub async fn get_holders(&self, mint: Vec<u8>) -> Result<Vec<HolderResponse>, sqlx::Error> {
        let query = r#"
        SELECT 
            a.*, 
            t.decimals,
            (a.amount::numeric / POWER(10, t.decimals)) AS normalized_amount
        FROM accounts a
        JOIN tokens t ON a.mint = t.mint_address
        WHERE a.mint = $1 AND a.amount > 0
        ORDER BY normalized_amount DESC
        LIMIT 500
        "#;
        let holders = sqlx::query(query).bind(&mint).fetch_all(&self.pool).await;
        match holders {
            Ok(holders) => {
                let mut holder_responses = Vec::new();
                for holder in holders {
                    holder_responses.push(HolderResponse {
                        address: bs58::encode(&holder.get::<Vec<u8>, _>("owner")).into_string(),
                        account: bs58::encode(&holder.get::<Vec<u8>, _>("account")).into_string(),
                        amount: Decimal::from_f64(holder.get::<f64, _>("normalized_amount"))
                            .unwrap(),
                        mint: bs58::encode(&mint).into_string(),
                        state: holder.get::<DBAccountState, _>("state"),
                        delegated_amount: holder.get::<Decimal, _>("delegated_amount"),
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
        pool_address: Vec<u8>,
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
                    ohlcvs.push(OHLCV {
                        timestamp: candle.get("bucket_start"),
                        open: candle.get("open"),
                        high: candle.get("high"),
                        low: candle.get("low"),
                        close: candle.get("close"),
                        volume_base: candle.get("volume_base"),
                        volume_quote: candle.get("volume_quote"),
                        trades: candle.get("trades"),
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
}
