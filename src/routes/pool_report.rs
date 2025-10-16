use std::str::FromStr;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::warn;

use crate::services::clickhouse::ClickhouseService;

#[derive(Debug, Serialize, Deserialize)]
pub enum ReportType {
    #[serde(rename = "1m")]
    OneMinute,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "24h")]
    OneDay,
}

#[derive(Debug, Serialize, Deserialize)]

pub struct PoolReportParams {
    pool_address: String,
    report_type: ReportType,
}
pub async fn get_pool_report(
    Query(params): Query<PoolReportParams>,
    State(db): State<ClickhouseService>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let pool_address = Pubkey::from_str(&params.pool_address).map_err(|e| {
        warn!(?e, "failed to encode pool_address in get_trader_details");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let pool_report = db
        .get_pool_report(pool_address.to_string(), params.report_type)
        .await;

    match pool_report {
        Ok(Some(report)) => Ok(Json(json!(report))),
        Ok(None) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(_) => return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
    }
}
