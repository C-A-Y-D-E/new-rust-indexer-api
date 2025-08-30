use std::str::FromStr;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::{error, warn};

use crate::services::db::DbService;

#[derive(Debug, Serialize, Deserialize)]
pub struct TraderParams {
    #[serde(rename = "makerAddress")]
    creator: String,
    #[serde(rename = "poolAddress")]
    pool_address: String,
}

pub async fn get_trader_details(
    Query(query): Query<TraderParams>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let creator = Pubkey::from_str(&query.creator).map_err(|e| {
        warn!(?e, "failed to encode creator in get_trader_details");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let pool_address = Pubkey::from_str(&query.pool_address).map_err(|e| {
        warn!(?e, "failed to encode pool_address in get_trader_details");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    match db
        .get_trader_details(
            creator.to_bytes().to_vec(),
            pool_address.to_bytes().to_vec(),
        )
        .await
    {
        Ok(data) => Ok(Json(json!(data))),
        Err(e) => {
            error!("Error getting get trader details: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
