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

#[derive(Debug, FromRow, Clone, Row, Serialize, Deserialize)]
pub struct Pool {
    pub pool_address: Pubkey,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub reversed: bool,
    pub token_base_address: Pubkey,
    pub token_quote_address: Pubkey,
    pub pool_base_address: Pubkey,
    pub pool_quote_address: Pubkey,
    pub curve_percentage: Option<Decimal18>,
    pub initial_token_base_reserve: Decimal18,
    pub initial_token_quote_reserve: Decimal18,
    pub slot: u64,
    pub creator: Pubkey,
    pub hash: Signature,
    pub metadata: Value, // New metadata field
}
#[derive(Debug, FromRow, Clone, Row, Serialize, Deserialize)]
pub struct DBPool {
    pub pool_address: String,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub reversed: bool,
    pub token_base_address: String,
    pub token_quote_address: String,
    pub pool_base_address: String,
    pub pool_quote_address: String,
    pub curve_percentage: Option<Decimal18>,
    pub initial_token_base_reserve: Decimal18,
    pub initial_token_quote_reserve: Decimal18,
    pub slot: i64,
    pub creator: String,
    pub hash: String,
    // #[serde(deserialize_with = "deserialize_json_option")]
    pub metadata: String,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub updated_at: DateTime<Utc>,
}

impl From<Pool> for DBPool {
    fn from(pool: Pool) -> Self {
        let now = Utc::now();
        Self {
            pool_address: pool.pool_address.to_string(),
            factory: pool.factory,
            pre_factory: pool.pre_factory,
            reversed: pool.reversed,
            token_base_address: pool.token_base_address.to_string(),
            token_quote_address: pool.token_quote_address.to_string(),
            pool_base_address: pool.pool_base_address.to_string(),
            pool_quote_address: pool.pool_quote_address.to_string(),
            curve_percentage: pool.curve_percentage,
            initial_token_base_reserve: pool.initial_token_base_reserve,
            initial_token_quote_reserve: pool.initial_token_quote_reserve,
            slot: pool.slot as i64,
            creator: pool.creator.to_string(),
            hash: pool.hash.to_string(),
            metadata: pool.metadata.to_string(),
            created_at: now,
            updated_at: now,
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
            curve_percentage: db_pool.curve_percentage,
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
    pub amount: Decimal,
    pub decimals: u8,
}

impl AccountWithNewBalance {
    pub fn new(
        owner: Pubkey,
        mint: Pubkey,
        account: Pubkey,
        amount: Decimal,
        decimals: u8,
    ) -> Self {
        Self {
            owner,
            mint,
            account,
            amount,
            decimals,
        }
    }
}
