use std::{str::FromStr, thread::park};

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::{debug, info, warn};

use crate::{
    defaults::{SOL_TOKEN, USDC_TOKEN},
    models::{pool::ResponsePool, token::ResponseToken},
    services::db::DbService,
};

pub async fn get_pair_info(
    Path(address): Path<String>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pool_address = Pubkey::from_str(&address).map_err(|_| {
        warn!("failed to parse pool address from token_info {}", address);
        StatusCode::BAD_REQUEST
    })?;
    let pair_info = db.get_pair_info(pool_address.to_bytes().to_vec()).await;

    match pair_info {
        Ok(pair_info) => {
            let pool: ResponsePool = ResponsePool::try_from(pair_info.pool).map_err(|e| {
                warn!(?e, "failed to convert pool");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            let base_token = ResponseToken::try_from(pair_info.base_token)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let quote_token = {
                if pool.token_quote_address == SOL_TOKEN.address {
                    SOL_TOKEN
                } else {
                    USDC_TOKEN
                }
            };

            Ok(Json(json!({
                "pool": pool,
                "base_token":base_token,
                "quote_token":quote_token
            })))
        }
        Err(e) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
