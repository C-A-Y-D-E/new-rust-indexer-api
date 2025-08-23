use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use spl_token::solana_program::pubkey::Pubkey;
use tracing::{debug, info};

use crate::services::db::DbService;

pub async fn get_pair_info(
    Path(address): Path<String>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pool_address1 = bs58::decode(&address).into_vec().unwrap();
    info!("pool_address1: {:?}", pool_address1);
    let pool_address2 = Pubkey::from_str_const(&address);
    info!("pool_address2: {:?}", pool_address2);
    let pair_info = db.get_pair_info(pool_address2.to_bytes().to_vec()).await;
    match pair_info {
        Ok(Some(pair_info)) => Ok(Json(json!(pair_info))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
