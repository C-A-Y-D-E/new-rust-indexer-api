use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub bundlers_hold_percent: Decimal,
    pub dev_holds_percent: Decimal,
    // pub dex_paid: bool,
    // pub insiders_hold_percent: Decimal,
    pub num_holders: i64,
    pub snipers_hold_percent: Decimal,
    pub top10_holders_percent: Decimal,
}
