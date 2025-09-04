use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "swap_type")]
pub enum SwapType {
    BUY,
    SELL,
    ADD,
    REMOVE,
    UNKNOWN,
}

impl SwapType {
    pub fn from_str(s: &str) -> Self {
        match s {
            "BUY" => SwapType::BUY,
            "SELL" => SwapType::SELL,
            "ADD" => SwapType::ADD,
            "REMOVE" => SwapType::REMOVE,
            _ => SwapType::UNKNOWN,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            SwapType::BUY => "BUY".to_string(),
            SwapType::SELL => "SELL".to_string(),
            SwapType::ADD => "ADD".to_string(),
            SwapType::REMOVE => "REMOVE".to_string(),
            SwapType::UNKNOWN => "UNKNOWN".to_string(),
        }
    }
}

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct Swap {
    pub creator: Pubkey,
    pub pool_address: Pubkey,
    pub swap_type: SwapType,
    pub hash: Signature,
    pub base_reserve: Decimal,
    pub quote_reserve: Decimal,
    pub price_sol: Decimal,
    pub base_amount: Decimal,
    pub quote_amount: Decimal,
    pub slot: u64,
    pub created_at: DateTime<Utc>,
    // pub updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]

pub struct DBSwap {
    pub creator: String,
    pub pool_address: String,
    pub base_reserve: Decimal,
    pub quote_reserve: Decimal,
    pub price_sol: Decimal,
    pub swap_type: SwapType,
    pub hash: String,

    pub base_amount: Decimal,
    pub quote_amount: Decimal,
    pub slot: i64,
    pub created_at: DateTime<Utc>,
    // pub updated_at: DateTime<Utc>,
}


impl From<Swap> for DBSwap {
    fn from(swap: Swap) -> Self {
        Self {
            creator: swap.creator.to_string(),
            pool_address: swap.pool_address.to_string(),

            base_reserve: Decimal::from(swap.base_reserve),
            quote_reserve: Decimal::from(swap.quote_reserve),
            price_sol: Decimal::from(swap.price_sol),
            swap_type: swap.swap_type,
            hash: swap.hash.to_string(),
            base_amount: Decimal::from(swap.base_amount),
            quote_amount: Decimal::from(swap.quote_amount),
            slot: swap.slot as i64,
            created_at: swap.created_at,
            // updated_at: swap.updated_at,
        }
    }
}

impl TryFrom<DBSwap> for Swap {
    type Error = String;

    fn try_from(db_swap: DBSwap) -> Result<Self, Self::Error> {
        Ok(Self {
            creator: Pubkey::from_str(&db_swap.creator).map_err(|_| "parse creator".to_string())?,
            pool_address: Pubkey::from_str(&db_swap.pool_address)
                .map_err(|_| "parse pool address".to_string())?,

            price_sol: db_swap.price_sol,
            base_reserve: db_swap.base_reserve,
            quote_reserve: db_swap.quote_reserve,
            swap_type: db_swap.swap_type,
            hash: Signature::from_str(&db_swap.hash).map_err(|_| "parse hash".to_string())?,
            base_amount: db_swap.base_amount,
            quote_amount: db_swap.quote_amount,
            slot: db_swap.slot as u64,
            created_at: db_swap.created_at,
            // updated_at: db_swap.updated_at,
        })
    }
}


