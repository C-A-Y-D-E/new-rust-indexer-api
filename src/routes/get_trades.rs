use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::error;

use crate::services::db::DbService;

#[derive(Debug, Serialize, Deserialize)]

pub struct GetTradesParams {
    pool_address: String,
    start_date: Option<String>,
    end_date: Option<String>,
}

fn parse_ymd_to_utc(date: &str) -> Result<DateTime<Utc>, StatusCode> {
    let d = NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| StatusCode::BAD_REQUEST)?;
    let ndt = d.and_hms_opt(0, 0, 0).ok_or(StatusCode::BAD_REQUEST)?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
}

pub async fn get_trades(
    db: State<DbService>,
    Query(params): Query<GetTradesParams>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let pool_address = Pubkey::from_str_const(&params.pool_address);
    let mut start_date = match params.start_date.as_deref() {
        Some(s) => Some(parse_ymd_to_utc(s)?),
        None => None,
    };
    let mut end_date = match params.end_date.as_deref() {
        Some(s) => Some(parse_ymd_to_utc(s)?),
        None => None,
    };

    // default if BOTH are missing: start = now, end = now + 7d
    if start_date.is_none() && end_date.is_none() {
        let now = Utc::now();
        start_date = Some(now - Duration::days(7));
        end_date = Some(now);
    }
    let trades = db
        .get_pool_swaps(pool_address.to_bytes().to_vec(), start_date, end_date)
        .await;

    match trades {
        Ok(trades) => Ok(Json(json!(trades))),
        Err(e) => {
            error!("Error getting trades: {}", e);
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
