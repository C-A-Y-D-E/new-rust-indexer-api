use std::str::FromStr;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::warn;

use crate::services::db::DbService;

pub async fn get_holders(
    Path(address): Path<String>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
  
    let mint = Pubkey::from_str(&address).map_err(|e| {
        warn!(?e, "Failed to parse pool in candlestick");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let holders = db.get_holders(mint.to_string()).await;
    match holders {
        Ok(holders) => Ok(Json(json!(holders))),
        Err(e) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
