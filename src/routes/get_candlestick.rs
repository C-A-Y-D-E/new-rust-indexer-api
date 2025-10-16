use std::str::FromStr;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::{error, warn};

use crate::{services::clickhouse::ClickhouseService, types::candlestick::CandlestickQuery};

pub async fn get_candlestick(
    Query(query): Query<CandlestickQuery>,
    State(db): State<ClickhouseService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pool_address = Pubkey::from_str(&query.pool_address).map_err(|e| {
        warn!(?e, "Failed to parse pool in candlestick");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    // Convert start_time and end_time from unix timestamp (i64) to DateTime<Utc>
    // Try to parse start_time and end_time, default to last 7 days if not passed or invalid
    let now = Utc::now();
    let start_time: i64 = match query.start_time {
        Some(ts) => ts,
        None => (now - Duration::days(7)).timestamp(),
    };

    let end_time: i64 = match query.end_time {
        Some(ts) => ts,
        None => now.timestamp(),
    };
    let interval = query.interval.to_string();
    let limit = query.limit;
    let candles = db
        .get_candlestick(
            pool_address.to_string(),
            interval,
            start_time,
            end_time,
            limit,
        )
        .await;
    match candles {
        Ok(candles) => Ok(Json(json!(candles))),
        Err(e) => {
            error!("Error getting candlestick: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
