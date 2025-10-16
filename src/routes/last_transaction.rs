use std::str::FromStr;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use serde::{Deserialize, Serialize};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::warn;

use crate::{models::swap::SwapType, services::clickhouse::ClickhouseService};

#[derive(Debug, Serialize, Deserialize)]
struct LastTransactionResponse {
    pub pool_address: String,
    pub creator: String,
    pub base_reserve: Decimal,
    pub quote_reserve: Decimal,
    pub price_sol: Decimal,
    pub swap_type: SwapType,
    pub hash: String,
    pub base_amount: Decimal,
    pub quote_amount: Decimal,
    pub slot: u64,
    pub created_at: DateTime<Utc>,
    // pub updated_at: DateTime<Utc>,
}

pub async fn get_last_transaction(
    db: State<ClickhouseService>,
    Path(address): Path<String>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let pool_address = Pubkey::from_str(&address).map_err(|_| {
        warn!(
            "failed to parse pool address from last transasction {}",
            address
        );
        StatusCode::BAD_REQUEST
    })?;

    let last_transaction = db.get_last_transaction(pool_address.to_string()).await;
    match last_transaction {
        Ok(Some(swap)) => Ok(Json(json!(swap))),
        Ok(None) => {
            return Err(axum::http::StatusCode::NOT_FOUND);
        }
        Err(_) => {
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
    // return Err(axum::http::StatusCode::NOT_FOUND);
}
