use std::str::FromStr;

use chrono::{DateTime, Utc};
use clickhouse::Row;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use solana_signature::Signature;
// use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

use crate::utils::Decimal18;

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct NewPool {
    pub creator: String,
    pub pool_address: String, // Keep as String (primary key)
    pub pool_base_address: String,
    pub pool_quote_address: String,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub token_base_address: String,
    pub token_quote_address: String,

    pub initial_token_base_reserve: f64,

    pub initial_token_quote_reserve: f64,
    pub slot: i64,
    pub reversed: bool,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub created_at: DateTime<Utc>,
    pub version: u32,
    pub hash: String,
    pub metadata: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pool {
    pub pool_address: Pubkey,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub reversed: bool,
    pub token_base_address: Pubkey,
    pub token_quote_address: Pubkey,
    pub pool_base_address: Pubkey,
    pub pool_quote_address: Pubkey,
    pub initial_token_base_reserve: f64,
    pub initial_token_quote_reserve: f64,
    pub slot: u64,
    pub creator: Pubkey,
    pub hash: Signature,
    pub metadata: Value, // New metadata field
}
// test-indexer/src/types/pool.rs
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct DBPool {
    pub creator: String,
    pub pool_address: String, // Keep as String (primary key)
    pub pool_base_address: String,
    pub pool_quote_address: String,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub token_base_address: String,
    pub token_quote_address: String,

    pub initial_token_base_reserve: f64,

    pub initial_token_quote_reserve: f64,
    pub slot: i64,
    pub reversed: bool,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub created_at: DateTime<Utc>,

    pub hash: String,
    pub metadata: String,
}

impl From<Pool> for DBPool {
    fn from(pool: Pool) -> Self {
        let now = Utc::now();
        Self {
            creator: pool.creator.to_string(),
            pool_address: pool.pool_address.to_string(),
            pool_base_address: pool.pool_base_address.to_string(),
            pool_quote_address: pool.pool_quote_address.to_string(),
            factory: pool.factory,
            pre_factory: pool.pre_factory,
            token_base_address: pool.token_base_address.to_string(),
            token_quote_address: pool.token_quote_address.to_string(),
            initial_token_base_reserve: pool.initial_token_base_reserve,
            initial_token_quote_reserve: pool.initial_token_quote_reserve,

            slot: pool.slot as i64,

            reversed: pool.reversed,
            created_at: now,

            hash: pool.hash.to_string(),
            metadata: pool.metadata.to_string(),
        }
    }
}

impl TryFrom<DBPool> for Pool {
    type Error = String;

    fn try_from(db_pool: DBPool) -> Result<Self, Self::Error> {
        Ok(Self {
            pool_address: Pubkey::from_str(&db_pool.pool_address)
                .map_err(|_| "parse pool address".to_string())?,
            factory: db_pool.factory,
            pre_factory: db_pool.pre_factory,
            reversed: db_pool.reversed,
            token_base_address: Pubkey::from_str(&db_pool.token_base_address)
                .map_err(|_| "parse token base address".to_string())?,
            token_quote_address: Pubkey::from_str(&db_pool.token_quote_address)
                .map_err(|_| "parse token quote address".to_string())?,
            pool_base_address: Pubkey::from_str(&db_pool.pool_base_address)
                .map_err(|_| "parse pool base address".to_string())?,
            pool_quote_address: Pubkey::from_str(&db_pool.pool_quote_address)
                .map_err(|_| "parse pool quote address".to_string())?,

            initial_token_base_reserve: db_pool.initial_token_base_reserve,
            initial_token_quote_reserve: db_pool.initial_token_quote_reserve,
            slot: db_pool.slot as u64,
            creator: Pubkey::from_str(&db_pool.creator).map_err(|_| "parse creator".to_string())?,
            hash: Signature::from_str(&db_pool.hash).map_err(|_| "parse hash".to_string())?,
            metadata: Value::String(db_pool.metadata),
        })
    }
}

#[derive(Debug)]
pub struct AccountWithNewBalance {
    pub owner: Pubkey,
    pub mint: Pubkey,
    pub account: Pubkey,
    pub amount: f64,
    pub decimals: u8,
}

impl AccountWithNewBalance {
    pub fn new(owner: Pubkey, mint: Pubkey, account: Pubkey, amount: f64, decimals: u8) -> Self {
        Self {
            owner,
            mint,
            account,
            amount,
            decimals,
        }
    }
}

#[derive(Debug)]
pub struct PoolCurveUpdate {
    pub pool_address: Pubkey,
    pub curve_percentage: f32,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct DBPoolCurveUpdate {
    pub pool_address: String,
    pub curve_percentage: f32,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub updated_at: DateTime<Utc>,
}

impl From<PoolCurveUpdate> for DBPoolCurveUpdate {
    fn from(pool_curve_update: PoolCurveUpdate) -> Self {
        let now = Utc::now();
        Self {
            pool_address: pool_curve_update.pool_address.to_string(),
            curve_percentage: pool_curve_update.curve_percentage,
            updated_at: now,
        }
    }
}
