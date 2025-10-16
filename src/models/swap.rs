use std::str::FromStr;

use chrono::{DateTime, Utc};

use clickhouse::{Client, Row, error::Result, sql::Identifier};

use serde::{Deserialize, Serialize};
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

use crate::utils::Decimal18;

#[derive(Debug, sqlx::Type, Clone, Serialize, Deserialize)]
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

#[derive(Debug, FromRow, Clone, Row, Serialize, Deserialize)]
pub struct Swap {
    pub creator: Pubkey,
    pub pool_address: Pubkey,
    pub swap_type: SwapType,
    pub hash: Signature,
    pub base_reserve: Decimal18,
    pub quote_reserve: Decimal18,
    pub price_sol: Decimal18,
    pub base_amount: Decimal18,
    pub quote_amount: Decimal18,
    pub slot: u64,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct DBSwap {
    pub creator: String,
    pub pool_address: String,
    pub hash: String,
    pub base_amount: Decimal18,
    pub quote_amount: Decimal18,
    pub base_reserve: Decimal18,
    pub quote_reserve: Decimal18,
    pub price_sol: Decimal18,
    pub swap_type: String,
    pub slot: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub created_at: DateTime<Utc>,
    // #[serde(with = "clickhouse::serde::chrono::datetime64::secs")]
    // pub updated_at: DateTime<Utc>,
}

impl From<Swap> for DBSwap {
    fn from(swap: Swap) -> Self {
        let now = Utc::now();
        Self {
            creator: swap.creator.to_string(),
            pool_address: swap.pool_address.to_string(),
            base_reserve: swap.base_reserve,
            quote_reserve: swap.quote_reserve,
            price_sol: swap.price_sol,
            swap_type: swap.swap_type.to_string(),
            hash: swap.hash.to_string(),
            base_amount: swap.base_amount,
            quote_amount: swap.quote_amount,
            slot: swap.slot as i64,
            created_at: now,
            // updated_at: now,
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
            swap_type: SwapType::from_str(&db_swap.swap_type),
            hash: Signature::from_str(&db_swap.hash).map_err(|_| "parse hash".to_string())?,
            base_amount: db_swap.base_amount,
            quote_amount: db_swap.quote_amount,
            slot: db_swap.slot as u64,
        })
    }
}
