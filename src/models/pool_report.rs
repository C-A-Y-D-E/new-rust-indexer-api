use rust_decimal::Decimal;

use chrono::{DateTime, Utc};
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow)]
pub struct PoolReport {
    pub pool_address: Vec<u8>,
    pub bucket_start: DateTime<Utc>,
    // Buy metrics
    pub buy_volume: Decimal,
    pub buy_count: i64,
    pub buyer_count: i64,
    // Sell metrics
    pub sell_volume: Decimal,
    pub sell_count: i64,
    pub seller_count: i64,
    // Combined metrics
    pub trader_count: i64,
    // Price metrics
    pub open_price: Decimal,
    pub close_price: Decimal,
    pub price_change_percent: Decimal,
}
