use axum::{
    Json,
    extract::{Path, State},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use serde::{Deserialize, Serialize};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;

use crate::{models::swap::SwapType, services::db::DbService};

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
    pub updated_at: DateTime<Utc>,
}

pub async fn get_last_transaction(
    db: State<DbService>,
    path: Path<String>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    println!("pool_address: {}", path.0);
    let pool_address = Pubkey::from_str_const(&path.0);
    println!("pool_address: {:?}", pool_address);

    let last_transaction = db
        .get_last_transaction(pool_address.to_bytes().to_vec())
        .await;
    match last_transaction {
        Ok(Some(swap)) => {
            let response = LastTransactionResponse {
                pool_address: pool_address.to_string(),
                creator: swap.creator.to_string(),
                base_reserve: swap.base_reserve,
                quote_reserve: swap.quote_reserve,
                price_sol: swap.price_sol,
                swap_type: swap.swap_type,
                hash: swap.hash.to_string(),
                base_amount: swap.base_amount,
                quote_amount: swap.quote_amount,
                slot: swap.slot,
                created_at: swap.created_at,
                updated_at: swap.updated_at,
            };
            Ok(Json(json!(response)))
        }
        Ok(None) => {
            return Err(axum::http::StatusCode::NOT_FOUND);
        }
        Err(_) => {
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}
