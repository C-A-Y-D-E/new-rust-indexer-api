use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use tracing::warn;

use crate::services::db::DbService;

pub async fn get_holders(
    Path(address): Path<String>,
    State(db): State<DbService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mint = bs58::decode(&address).into_vec().map_err(|e| {
        warn!(?e, "failed to convert pool");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let holders = db.get_holders(mint).await;
    match holders {
        Ok(holders) => Ok(Json(json!(holders))),
        Err(e) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
