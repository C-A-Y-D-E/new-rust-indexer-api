use std::str::FromStr;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::{error, warn};

use crate::services::clickhouse::ClickhouseService;

pub async fn get_top_traders(
    Path(address): Path<String>,
    State(db): State<ClickhouseService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pool_address = Pubkey::from_str(&address).map_err(|_| {
        warn!("failed to parse pool address from token_info {}", address);
        StatusCode::BAD_REQUEST
    })?;
    let top_traders = db.get_top_traders(pool_address.to_string()).await;
    match top_traders {
        Ok(top_traders) => Ok(Json(json!(top_traders))),
        Err(e) => {
            error!("Error getting top traders: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
