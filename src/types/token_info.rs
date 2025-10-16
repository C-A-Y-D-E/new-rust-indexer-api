use clickhouse::Row;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::utils::Decimal18;

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub bundlers_hold_percent: Decimal18,
    pub dev_holds_percent: Decimal18,
    // pub dex_paid: bool,
    // pub insiders_hold_percent: Decimal,
    pub num_holders: i64,
    pub snipers_hold_percent: Decimal18,
    pub top10_holders_percent: Decimal18,
}

#[derive(Row, Deserialize, Serialize, Debug)]
pub struct TokenInfoRow {
    pub top10_amount_raw: Decimal18,
    pub dev_amount_raw: Decimal18,
    pub snipers_amount_raw: Decimal18,
    pub num_holders: u64,
    pub token_supply: Decimal18,
    pub decimals: i8,
    pub bundlers_amount_raw: Decimal18,
}
