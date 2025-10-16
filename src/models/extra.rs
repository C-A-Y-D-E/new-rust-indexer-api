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
    base_bought: Decimal18,
    base_sold: Decimal18,
    quote_bought: Decimal18,
    quote_sold: Decimal18,
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
    pub amount: Decimal18,
    pub delegated_amount: Decimal18,
}
