use clickhouse::Row;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub bundlers_hold_percent: f64,
    pub dev_holds_percent: f64,
    // pub dex_paid: bool,
    // pub insiders_hold_percent: Decimal,
    pub num_holders: i64,
    pub snipers_hold_percent: f64,
    pub top10_holders_percent: f64,
}

#[derive(Row, Deserialize, Serialize, Debug)]
pub struct TokenInfoRow {
    pub top10_amount_raw: f64,
    pub dev_amount_raw: f64,
    pub snipers_amount_raw: f64,
    pub num_holders: u64,
    pub token_supply: f64,
    pub decimals: i8,
    pub bundlers_amount_raw: f64,
}
