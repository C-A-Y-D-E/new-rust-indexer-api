use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, dec};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use solana_signature::Signature;
// use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow, Clone, Serialize, Deserialize)]
pub struct Pool {
    pub pool_address: Pubkey,
    pub factory: String,
    pub pre_factory: Option<String>,
    pub reversed: bool,
    pub token_base_address: Pubkey,
    pub token_quote_address: Pubkey,
    pub pool_base_address: Pubkey,
    pub pool_quote_address: Pubkey,
    pub curve_percentage: Option<Decimal>,
    pub initial_token_base_reserve: Decimal,
    pub initial_token_quote_reserve: Decimal,
    pub slot: u64,
    pub creator: Pubkey,
    pub hash: Signature,
    pub metadata: Option<Value>, // New metadata field
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct DBPool {
    pub pool_address: String,
    pub factory: String,
    pub pre_factory: String,
    pub reversed: bool,
    pub token_base_address: String,
    pub token_quote_address: String,
    pub pool_base_address: String,
    pub pool_quote_address: String,
    pub curve_percentage: Option<Decimal>,
    pub initial_token_base_reserve: Decimal,
    pub initial_token_quote_reserve: Decimal,
    pub slot: i64,
    pub creator: String,
    pub hash: String,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}



impl From<Pool> for DBPool {
    fn from(pool: Pool) -> Self {
        Self {
            pool_address: pool.pool_address.to_string(),
            factory: pool.factory,
            pre_factory: pool.pre_factory.unwrap_or_default(),
            reversed: pool.reversed,
            token_base_address: pool.token_base_address.to_string(),
            token_quote_address: pool.token_quote_address.to_string(),
            pool_base_address: pool.pool_base_address.to_string(),
            pool_quote_address: pool.pool_quote_address.to_string(),
            curve_percentage: pool.curve_percentage,
            initial_token_base_reserve: Decimal::from(pool.initial_token_base_reserve),
            initial_token_quote_reserve: Decimal::from(pool.initial_token_quote_reserve),
            slot: pool.slot as i64,
            creator: pool.creator.to_string(),
            hash: pool.hash.to_string(),
            metadata: pool.metadata,
            created_at: pool.created_at,
            updated_at: pool.updated_at,
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
            pre_factory: Some(db_pool.pre_factory),
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
            metadata: db_pool.metadata,
            created_at: db_pool.created_at,
            updated_at: db_pool.updated_at,
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
