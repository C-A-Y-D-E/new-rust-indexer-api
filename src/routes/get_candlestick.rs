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

use crate::{services::db::DbService, types::candlestick::CandlestickQuery};

pub async fn get_candlestick(
    Query(query): Query<CandlestickQuery>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pool_address = Pubkey::from_str(&query.pool_address).map_err(|e| {
        warn!(?e, "Failed to parse pool in candlestick");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    // Convert start_time and end_time from unix timestamp (i64) to DateTime<Utc>
    let start_time: DateTime<Utc> = query
        .start_time
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(|| Utc::now() - Duration::days(7));

    let end_time: DateTime<Utc> = query
        .end_time
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);
    let interval = query.interval.to_string();
    let limit = query.limit;
    let candles = db
        .get_candlestick(
            pool_address.to_bytes().to_vec(),
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
