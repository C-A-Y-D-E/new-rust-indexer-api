use crate::{
    models::{pool::DBPool, swap::DBSwap},
    routes::{
        // get_trades::get_trades,
        get_candlestick::get_candlestick,
        get_holders::get_holders,
        get_pair_info::get_pair_info,
        get_token_info::get_token_info,
        get_top_traders::{self, get_top_traders},
        get_trader_details::{self, get_trader_details},
        get_trades::get_trades,
        last_transaction::get_last_transaction,
        pool_report::get_pool_report,
        pulse::pulse,
        search::search_pools, // search::search_pools,
    },
    services::{clickhouse::ClickhouseService, redis::subscribe_and_process},
    websocket::{new_pool_event::on_new_pool_event, on_connect},
};
use axum::{
    Router,
    routing::{get, post},
};
use futures_util::StreamExt;

use socketioxide::SocketIo;

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
    // let db = DbService::init().await;
    let clickhouse = ClickhouseService::init().await;
    // let redis = RedisService::init().await;
    tracing::subscriber::set_global_default(FmtSubscriber::default())?;

    let (layer, io) = SocketIo::new_layer();
    let io_clone = io.clone();
    let clickhouse_clone = clickhouse.clone();
    tokio::spawn(async move {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/".to_string());
        let client = redis::Client::open(redis_url).unwrap();
        let mut pubsub = client.get_async_pubsub().await.unwrap();

        pubsub.subscribe("swap_created").await.unwrap();
        pubsub.subscribe("pool_created").await.unwrap();

        let mut stream = pubsub.on_message();

        while let Some(msg) = stream.next().await {
            let channel: String = msg.get_channel_name().to_string();
            let payload: String = msg.get_payload().unwrap();

            match channel.as_str() {
                "swap_created" => {
                    if let Ok(data) = serde_json::from_str::<DBSwap>(&payload) {
                        let _ = io_clone
                            .emit(format!("s:{}", data.pool_address), &data)
                            .await;
                    }
                }
                "pool_created" => {
                    if let Ok(data) = serde_json::from_str::<DBPool>(&payload) {
                        println!("data: {:?}", data);
                        match on_new_pool_event(data, &clickhouse_clone).await {
                            Ok(pulse_data) => {
                                let _ = io_clone.emit("new-pair", &pulse_data).await;
                            }
                            Err(error) => {
                                println!("Error: {:?}", error.to_string());
                            }
                        }
                    }
                }
                _ => {}
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
        .with_state(clickhouse)
        .layer(
            // Cors layer
            ServiceBuilder::new()
                .layer(CorsLayer::permissive())
                // Socket layer
                .layer(layer),
        );
    info!("Server is running on ports 3001");
    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}

// db.listen_to_swap_events(|swap_event| {
//     println!("Swap event: {:?}", swap_event);
// })
// .await
// .unwrap();
