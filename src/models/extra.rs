use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::{
    models::{pool::DBPool, token::DBToken},
    utils::Decimal18,
};

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct TopTrader {
    creator: String,
    is_sniper: bool,
    base_bought: f64,
    base_sold: f64,
    quote_bought: f64,
    quote_sold: f64,
    holding_base_token: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PairInfo {
    pub pool: DBPool,
    pub base_token: DBToken,
}
#[derive(Debug, Serialize, Deserialize, Row)]
pub struct HolderResponse {
    pub address: String,
    pub account: String,
    pub mint: String,
    pub decimals: u8,
    pub amount: f64,
    pub delegated_amount: i64,
}
