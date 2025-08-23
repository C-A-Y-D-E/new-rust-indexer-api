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
    pub pool_address: Vec<u8>,
    pub factory: String,
    pub pre_factory: String,
    pub reversed: bool,
    pub token_base_address: Vec<u8>,
    pub token_quote_address: Vec<u8>,
    pub pool_base_address: Vec<u8>,
    pub pool_quote_address: Vec<u8>,
    pub curve_percentage: Option<Decimal>,
    pub initial_token_base_reserve: Decimal,
    pub initial_token_quote_reserve: Decimal,
    pub slot: i64,
    pub creator: Vec<u8>,
    pub hash: Vec<u8>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsePool {
    pub pool_address: String,
    pub factory: String,
    pub pre_factory: Option<String>,
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
            pool_address: pool.pool_address.to_bytes().to_vec(),
            factory: pool.factory,
            pre_factory: pool.pre_factory.unwrap_or_default(),
            reversed: pool.reversed,
            token_base_address: pool.token_base_address.to_bytes().to_vec(),
            token_quote_address: pool.token_quote_address.to_bytes().to_vec(),
            pool_base_address: pool.pool_base_address.to_bytes().to_vec(),
            pool_quote_address: pool.pool_quote_address.to_bytes().to_vec(),
            curve_percentage: pool.curve_percentage,
            initial_token_base_reserve: Decimal::from(pool.initial_token_base_reserve),
            initial_token_quote_reserve: Decimal::from(pool.initial_token_quote_reserve),
            slot: pool.slot as i64,
            creator: pool.creator.to_bytes().to_vec(),
            hash: pool.hash.as_ref().to_vec(),
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
            pool_address: Pubkey::try_from(db_pool.pool_address)
                .map_err(|_| "parse pool address".to_string())?,
            factory: db_pool.factory,
            pre_factory: Some(db_pool.pre_factory),
            reversed: db_pool.reversed,
            token_base_address: Pubkey::try_from(db_pool.token_base_address)
                .map_err(|_| "parse token base address".to_string())?,
            token_quote_address: Pubkey::try_from(db_pool.token_quote_address)
                .map_err(|_| "parse token quote address".to_string())?,
            pool_base_address: Pubkey::try_from(db_pool.pool_base_address)
                .map_err(|_| "parse pool base address".to_string())?,
            pool_quote_address: Pubkey::try_from(db_pool.pool_quote_address)
                .map_err(|_| "parse pool quote address".to_string())?,
            curve_percentage: db_pool.curve_percentage,
            initial_token_base_reserve: db_pool.initial_token_base_reserve,
            initial_token_quote_reserve: db_pool.initial_token_quote_reserve,
            slot: db_pool.slot as u64,
            creator: Pubkey::try_from(db_pool.creator).map_err(|_| "parse creator".to_string())?,
            hash: Signature::try_from(db_pool.hash).map_err(|_| "parse hash".to_string())?,
            metadata: db_pool.metadata,
            created_at: db_pool.created_at,
            updated_at: db_pool.updated_at,
        })
    }
}

impl TryFrom<DBPool> for ResponsePool {
    type Error = String;

    fn try_from(db_pool: DBPool) -> Result<Self, Self::Error> {
        Ok(Self {
            pool_address: bs58::encode(db_pool.pool_address).into_string(),
            factory: db_pool.factory,
            pre_factory: Some(db_pool.pre_factory),
            reversed: db_pool.reversed,
            token_base_address: bs58::encode(db_pool.token_base_address).into_string(),
            token_quote_address: bs58::encode(db_pool.token_quote_address).into_string(),
            pool_base_address: bs58::encode(db_pool.pool_base_address).into_string(),
            pool_quote_address: bs58::encode(db_pool.pool_quote_address).into_string(),
            curve_percentage: db_pool.curve_percentage.map(|d| d.round_dp(2)),
            initial_token_base_reserve: db_pool.initial_token_base_reserve,
            initial_token_quote_reserve: db_pool.initial_token_quote_reserve,
            slot: db_pool.slot,
            creator: bs58::encode(db_pool.creator).into_string(),
            hash: bs58::encode(db_pool.hash).into_string(),
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
