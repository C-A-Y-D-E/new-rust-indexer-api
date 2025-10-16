use clickhouse::Row;
use rust_decimal::Decimal;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

use crate::utils::Decimal18;

#[derive(Debug, FromRow, Row, Deserialize, Serialize)]
pub struct PoolReport {
    pub pool_address: String,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub bucket_start: chrono::DateTime<chrono::Utc>,
    pub buy_volume: Decimal18,
    pub buy_count: u64,
    pub sell_volume: Decimal18,
    pub sell_count: u64,
    pub unique_traders: u64,
    pub unique_buyers: u64,
    pub unique_sellers: u64,
}
