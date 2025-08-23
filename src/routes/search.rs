use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;

use crate::{
    defaults::{SOL_TOKEN, USDC_TOKEN},
    models::sniper::{DevHolding, SniperSummary},
    services::db::QuoteTokenData,
};
use crate::{models::swap::SwapType, services::db::DbService};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use spl_token::solana_program::pubkey::Pubkey;

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    search: String,
}

pub async fn search_pools(
    data: State<DbService>,
    query: Query<SearchParams>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let search_term = query.search.trim_matches('"');
    if search_term.len() == 44 {
        let pool_address = Pubkey::from_str_const(search_term).to_bytes().to_vec();
        let pool_and_token_data = data.get_pool_and_token_data(pool_address).await;
        match pool_and_token_data {
            Ok(pool_and_token_data) => Ok(Json(json!({ "data": pool_and_token_data }))),

            Err(e) => {
                error!("Error getting pool and token data: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    } else {
        let tokens = data.search_tokens(search_term.to_string()).await;
        match tokens {
            Ok(tokens) => {
                let mut results = Vec::new();
                for token in tokens {
                    let pool_and_token_data = data
                        .get_pool_and_token_data(token.mint_address.to_bytes().to_vec())
                        .await;
                    match pool_and_token_data {
                        Ok(pool_and_token_data) => results.push(pool_and_token_data),
                        Err(e) => {
                            error!("Error getting pool and token data: {}", e);
                            continue;
                        }
                    }
                }
                Ok(Json(json!({ "data": results })))
            }
            Err(e) => {
                error!("Error searching tokens: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
    // Ok(Json(json!({})))
}
