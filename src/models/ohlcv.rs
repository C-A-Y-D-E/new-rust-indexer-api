use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::utils::Decimal18;

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct OHLCV {
    pub pool_address: String,
    pub timestamp: u64,
    pub open: Decimal18,
    pub high: Decimal18,
    pub low: Decimal18,
    pub close: Decimal18,
    pub volume_base: Decimal18,
    pub volume_quote: Decimal18,
    pub trades: u64,
}
