use axum::{Json, extract::{Path, State}, http::StatusCode};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;

use crate::services::clickhouse::ClickhouseService;

#[derive(Debug, Serialize, Deserialize, Row)]
struct PortfolioRow {
    pub pool_address: String,
    pub token_address: String,
    pub tokens_bought: f64,
    pub tokens_sold: f64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub native_spent: f64,
    pub native_received: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub protocol: String,
    pub token_name: Option<String>,
    pub token_symbol: Option<String>,
    pub token_image: Option<String>,
    pub token_decimals: i8,
    pub liquidity_native: f64,
    pub liquidity_token: f64,
    pub price_native: f64,
    pub remaining_tokens: f64,
}

pub async fn portfolio(
    Path(user_address): Path<String>,
    State(db): State<ClickhouseService>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let query = r#"
        WITH user_pools AS (
            SELECT DISTINCT pool_address
            FROM swaps
            WHERE creator = ?
            AND swap_type IN ('BUY', 'SELL')
        ),
        user_swaps AS (
            SELECT 
                s.pool_address,
                sumIf(s.base_amount, s.swap_type = 'BUY') AS tokens_bought,
                sumIf(s.base_amount, s.swap_type = 'SELL') AS tokens_sold,
                countIf(s.swap_type = 'BUY') AS buy_count,
                countIf(s.swap_type = 'SELL') AS sell_count,
                sumIf(s.quote_amount, s.swap_type = 'BUY') AS native_spent,
                sumIf(s.quote_amount, s.swap_type = 'SELL') AS native_received,
                min(s.created_at) AS created_at,
                max(s.created_at) AS updated_at
            FROM swaps s
            INNER JOIN user_pools up ON s.pool_address = up.pool_address
            WHERE s.creator = ?
            AND s.swap_type IN ('BUY', 'SELL')
            GROUP BY s.pool_address
        ),
        latest_swaps AS (
            SELECT 
                s.pool_address,
                argMax(s.quote_reserve, s.created_at) AS liquidity_native,
                argMax(s.base_reserve, s.created_at) AS liquidity_token,
                argMax(s.price_sol, s.created_at) AS price_native
            FROM swaps s
            INNER JOIN user_pools up ON s.pool_address = up.pool_address
            GROUP BY s.pool_address
        ),
        pool_info AS (
            SELECT 
                p.pool_address,
                p.token_base_address,
                p.factory AS protocol
            FROM pools p
            INNER JOIN user_pools up ON p.pool_address = up.pool_address
        ),
        token_info AS (
            SELECT 
                t.mint_address,
                t.name AS token_name,
                t.symbol AS token_symbol,
                t.image AS token_image,
                t.decimals AS token_decimals
            FROM tokens t
            INNER JOIN pool_info pi ON t.mint_address = pi.token_base_address
        ),
        remaining_tokens AS (
            SELECT 
                pi.pool_address,
                COALESCE(a.amount, 0) / pow(10, COALESCE(ti.token_decimals, 0)) AS remaining_tokens
            FROM pool_info pi
            LEFT JOIN accounts a ON a.mint = pi.token_base_address AND a.owner = ?
            LEFT JOIN token_info ti ON ti.mint_address = pi.token_base_address
        )
        SELECT 
            us.pool_address AS pool_address,
            pi.token_base_address AS token_address,
            us.tokens_bought,
            us.tokens_sold,
            us.buy_count,
            us.sell_count,
            us.native_spent,
            us.native_received,
            us.created_at,
            us.updated_at,
            pi.protocol,
            ti.token_name,
            ti.token_symbol,
            ti.token_image,
            ti.token_decimals,
            COALESCE(ls.liquidity_native, 0) AS liquidity_native,
            COALESCE(ls.liquidity_token, 0) AS liquidity_token,
            COALESCE(ls.price_native, 0) AS price_native,
            COALESCE(rt.remaining_tokens, 0) AS remaining_tokens
        FROM user_swaps us
        JOIN pool_info pi ON pi.pool_address = us.pool_address
        LEFT JOIN latest_swaps ls ON ls.pool_address = us.pool_address
        LEFT JOIN token_info ti ON ti.mint_address = pi.token_base_address
        LEFT JOIN remaining_tokens rt ON rt.pool_address = us.pool_address
        WHERE COALESCE(rt.remaining_tokens, 0) > 0
        ORDER BY us.updated_at DESC
    "#;

    let rows: Vec<PortfolioRow> = db.client.query(query)
        .bind(&user_address)
        .bind(&user_address)
        .bind(&user_address)
        .fetch_all()
        .await
        .map_err(|e| {
            info!("Portfolio query failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let active_positions: Vec<serde_json::Value> = rows.into_iter().map(|row| {
        json!({
            "tokenAddress": row.token_address,
            "tokensBought": row.tokens_bought,
            "buyCount": row.buy_count,
            "createdAt": row.created_at,
            "extra": null,
            "liquidityNative": row.liquidity_native,
            "liquidityToken": row.liquidity_token,
            "nativeReceived": row.native_received,
            "nativeSpent": row.native_spent,
            "nativeToken": "sol",
            "pairAddress": row.pool_address,
            "priceNative": row.price_native,
            "protocol": row.protocol,
            "remainingTokens": row.remaining_tokens,
            "sellCount": row.sell_count,
            "tokenDecimals": row.token_decimals,
            "tokenImage": row.token_image,
            "tokenName": row.token_name,
            "tokenTicker": row.token_symbol,
            "tokensSold": row.tokens_sold,
            "updatedAt": row.updated_at,
            "usdReceived": 0,
            "usdSpent": 0,
        })
    }).collect();

    Ok(Json(json!({
        "activePositions": active_positions
    })))
}