use std::str::FromStr;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::{debug, warn};

use crate::services::clickhouse::ClickhouseService;

pub async fn get_token_info(
    db: State<ClickhouseService>,
    Path(address): Path<String>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let pool_address = Pubkey::from_str(&address).map_err(|_| {
        warn!("failed to parse pool address from token_info {}", address);
        StatusCode::BAD_REQUEST
    })?;

    let token_info = db.get_token_info(pool_address.to_string()).await;
    match token_info {
        Ok(token_info) => Ok(Json(json!(token_info))),
        Err(_e) => Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
    }
}
