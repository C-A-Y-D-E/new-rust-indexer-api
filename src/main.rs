use crate::{
    models::{pool::DBPool, swap::DBSwap},
    routes::{
        // get_trades::get_trades,
        get_candlestick::get_candlestick,
        get_holders::get_holders,
        get_pair_info::get_pair_info,
        get_token_info::get_token_info,
        get_top_traders::get_top_traders,
        get_trader_details::get_trader_details,
        get_trades::get_trades,
        last_transaction::get_last_transaction,
        pool_report::get_pool_report,
        pulse::pulse,
        search::search_pools, // search::search_pools,
    },
    services::clickhouse::ClickhouseService,
    websocket::{new_pool_event::on_new_pool_event, on_connect},
};
use axum::{
    Router,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};

use socketioxide::SocketIo;

use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::FmtSubscriber;
use serde_json::json;
mod defaults;
mod models;
mod routes;
mod services;
mod types;
mod utils;
mod websocket;


#[derive(clickhouse::Row, serde::Deserialize)]
struct PoolCreatedAt {
    #[allow(dead_code)]
    pool_address: String,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    created_at: DateTime<Utc>,
}

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
    // Share DB to websocket module
    websocket::store::set_clickhouse_service(clickhouse.clone());
    
    // Shared state for tracking pools that need updates
    let updated_pools: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    
    let io_clone = io.clone();
    let clickhouse_clone = clickhouse.clone();
    let updated_pools_clone = updated_pools.clone();
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
                        // Get pool creation time from materialized view (faster!)
                        let query = "SELECT pool_address, created_at FROM pools_recent_lean_mv WHERE pool_address = ? LIMIT 1";
                        if let Ok(pool_info) = clickhouse_clone.client
                            .query(query)
                            .bind(&data.pool_address)
                            .fetch_one::<PoolCreatedAt>()
                            .await 
                        {
                            let now = Utc::now();
                            let twenty_four_hours_ago = now - chrono::Duration::hours(24);
                            
                            // Only track pools created in last 24 hours (less than 24h old)
                            if pool_info.created_at >= twenty_four_hours_ago {
                                // Keep individual swap event for specific pool channel
                                let _ = io_clone
                                    .emit(format!("s:{}", data.pool_address), &data)
                                    .await;
                                
                                // Add pool to batch update queue
                                if let Ok(mut pools) = updated_pools_clone.lock() {
                                    pools.insert(data.pool_address.clone());
                                }
                            }
                        }
                    }
                }
                "pool_created" => {
                    if let Ok(data) = serde_json::from_str::<DBPool>(&payload) {
                        let now = Utc::now();
                        let twenty_four_hours_ago = now - chrono::Duration::hours(24);
                        
                        // Only process pools created in last 24 hours (less than 24h old)
                        if data.created_at >= twenty_four_hours_ago {
                            // Emit immediate "new-pair" event for new pools
                            match on_new_pool_event(data.clone(), &clickhouse_clone).await {
                                Ok(pulse_data) => {
                                    let _ = io_clone.emit("new-pair", &pulse_data).await;
                                }
                                Err(error) => {
                                    println!("Error processing new pool: {:?}", error.to_string());
                                }
                            }
                            
                            // Add pool to batch update queue
                            if let Ok(mut pools) = updated_pools_clone.lock() {
                                pools.insert(data.pool_address.clone());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    });

    // Periodic batch emission task - sends all updated pools every 2 seconds
    let io_batch = io.clone();
    let clickhouse_batch = clickhouse.clone();
    let updated_pools_batch = updated_pools.clone();
    tokio::spawn(async move {
        let mut interval_timer = interval(Duration::from_millis(200));
        interval_timer.tick().await; // Skip first immediate tick
        
        loop {
            interval_timer.tick().await;
            
            // Drain the pool addresses that need updates
            let pool_addresses: Vec<String> = {
                if let Ok(mut pools) = updated_pools_batch.lock() {
                    pools.drain().collect()
                } else {
                    Vec::new()
                }
            };
            
            if pool_addresses.is_empty() {
                continue;
            }
            
            println!("Batch processing {} pools", pool_addresses.len());
            
            // Fetch full data for each pool
            let mut pulse_data_list = Vec::new();
            for pool_address in pool_addresses {
                // Try materialized view first (fast), fallback to main pools table
                let pool_query = "SELECT * FROM pools_recent_lean_mv WHERE pool_address = ? LIMIT 1";
                let pool_result = clickhouse_batch.client
                    .query(pool_query)
                    .bind(&pool_address)
                    .fetch_one::<DBPool>()
                    .await;
                
                let pool = match pool_result {
                    Ok(p) => p,
                    Err(_) => {
                        // Fallback to main pools table if not in materialized view
                        let fallback_query = "SELECT * FROM pools WHERE pool_address = ? LIMIT 1";
                        match clickhouse_batch.client
                            .query(fallback_query)
                            .bind(&pool_address)
                            .fetch_one::<DBPool>()
                            .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                println!("Error fetching pool data for {}: {:?}", pool_address, e);
                                continue;
                            }
                        }
                    }
                };
                
                match on_new_pool_event(pool, &clickhouse_batch).await {
                    Ok(pulse_data) => {
                        pulse_data_list.push(pulse_data);
                    }
                    Err(e) => {
                        println!("Error processing pool {}: {:?}", pool_address, e);
                    }
                }
            }
            
            // Emit batched update if we have any successful results
            if !pulse_data_list.is_empty() {
                println!("Emitting batch of {} pools", pulse_data_list.len());
                let _ = io_batch
                    .emit(
                        "update_pulse_v2",
                        &json!({
                            "channel": "update_pulse_v2",
                            "data": {
                                "isSnapshot": false,
                                "content": pulse_data_list
                            }
                        }),
                    )
                    .await;
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
