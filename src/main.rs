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
        // pulse::pulse,
        search::search_pools, // search::search_pools,
    },
    services::clickhouse::ClickhouseService,
    websocket::on_connect,
};
use axum::{
    Router,
    routing::{get, post},
};

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
    tracing::subscriber::set_global_default(FmtSubscriber::default())?;

    let (layer, io) = SocketIo::new_layer();

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
        // .route("/pulse", post(pulse))
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
