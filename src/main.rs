use crate::{
    models::{
        pool::{DBPool, ResponsePool},
        swap::{DBSwap, ResponseSwap},
    },
    routes::{
        get_candlestick::get_candlestick, get_holders::get_holders, get_pair_info::get_pair_info,
        get_token_info::get_token_info, get_top_traders::get_top_traders,
        get_trader_details::get_trader_details, get_trades::get_trades,
        last_transaction::get_last_transaction, pool_report::get_pool_report, pulse::pulse,
        search::search_pools,
    },
    services::db::{DbService, PoolEvent},
    types::pulse::{DevWalletFunding, PulseDataResponse},
    websocket::{new_pool_event::on_new_pool_event, new_swap_event::PoolManager, on_connect},
};
use axum::{
    Router,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use rust_decimal::prelude::ToPrimitive;
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

use rust_decimal::{Decimal, prelude::FromPrimitive};
use socketioxide::SocketIo;
use sqlx::{Postgres, postgres::PgListener};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::FmtSubscriber;
mod defaults;
mod models;
mod routes;
mod services;
mod types;
mod utils;
mod websocket;

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let db = DbService::init().await;
    tracing::subscriber::set_global_default(FmtSubscriber::default())?;

    let (layer, io) = SocketIo::new_layer();

    // Store a reference to the Socket.IO instance
    let io_clone = io.clone();
    let db_clone = db.clone();
    // Spawn the pool event listener in a separate task
    tokio::spawn(async move {
        let mut listener = PgListener::connect_with(&db_clone.pool).await.unwrap();
        listener
            .listen_all(vec!["pool_inserted", "swap_inserted"])
            .await
            .unwrap();
        let mut stream = listener.into_stream();
        let recent_pools = db_clone.get_24hr_recent_pools().await.unwrap();
        let mut pool_manager = PoolManager::new(recent_pools);
        while let Ok(Some(notification)) = stream.try_next().await {
            if notification.channel() == "pool_inserted" {
                match on_new_pool_event(&notification, &db_clone).await {
                    Ok(pulse_data) => {
                        let _ = io_clone.emit("new-pair", &pulse_data.0).await;
                        pool_manager.update_pools(pulse_data.1).await;
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            }
            if notification.channel() == "swap_inserted" {
                let raw_notification =
                    serde_json::from_str::<ResponseSwap>(notification.payload()).unwrap();
                let swap = DBSwap::from(raw_notification);
                let r = ResponseSwap::try_from(swap.clone()).unwrap();
                // println!("raw_notification: {:?}", swap);
                let _ = io_clone.emit(format!("s:{}", r.pool_address), &r).await;
                let pool_address = swap.pool_address;
                let batch = pool_manager.verify_and_add_pool(pool_address).await;

                if batch.is_some() && !batch.as_ref().unwrap().is_empty() {
                    let pulse_data = db_clone.get_batch_pool_data(&batch.unwrap()).await;
                    match pulse_data {
                        Ok(pulse_data) => {
                            let _ = io_clone.emit("update-pulse", &pulse_data).await;
                        }
                        Err(e) => {
                            println!("Error: {:?}", e);
                        }
                    }
                }
            }
        }
    });
    // Connection to the socket start
    io.ns("/", on_connect);
    // Connection to the socket end
    println!("Starting server");
    let app = Router::new()
        .route("/", get(root))
        .route("/pools", get(search_pools))
        .route("/candlestick", get(get_candlestick))
        .route("/pair-info/{pool_address}", get(get_pair_info))
        .route("/top-traders/{pool_address}", get(get_top_traders))
        .route("/holders/{token_address}", get(get_holders))
        .route("/trades", get(get_trades))
        .route(
            "/get-last-transaction/{pool_address}",
            get(get_last_transaction),
        )
        .route("/pool-report", get(get_pool_report))
        .route("/pulse", post(pulse))
        .route("/token-info/{pool_address}", get(get_token_info))
        .route("/trader-details", get(get_trader_details))
        .with_state(db)
        .layer(
            // Cors layer
            ServiceBuilder::new()
                .layer(CorsLayer::permissive())
                // Socket layer
                .layer(layer),
        );
    info!("Server is running on port 3001");
    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3001")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}

// db.listen_to_swap_events(|swap_event| {
//     println!("Swap event: {:?}", swap_event);
// })
// .await
// .unwrap();
