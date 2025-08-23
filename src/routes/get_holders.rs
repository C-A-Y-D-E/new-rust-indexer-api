use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;

use crate::services::db::DbService;

pub async fn get_holders(
    Path(address): Path<String>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mint = bs58::decode(&address).into_vec().unwrap();
    let holders = db.get_holders(mint).await;
    match holders {
        Ok(holders) => Ok(Json(json!(holders))),
        Err(e) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
