use crate::{
    models::pool::{DBPool, ResponsePool},
    routes::{
        get_candlestick::get_candlestick, get_holders::get_holders, get_pair_info::get_pair_info,
        get_top_traders::get_top_traders, get_trades::get_trades,
        last_transaction::get_last_transaction, pool_report::get_pool_report, pulse::pulse,
        search::search_pools,
    },
    services::db::{DbService, PoolEvent},
    types::pulse::{DevWalletFunding, PulseDataResponse},
    websocket::on_connect,
};
use axum::{
    Router,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use rust_decimal::prelude::ToPrimitive;
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

    // db.listen_to_pool_events(|pool_event| {
    //     let data = ResponsePool::try_from(pool_event).unwrap();
    //     println!("Pool event: {:?}", data);
    // })
    // .await
    // .unwrap();

    let (layer, io) = SocketIo::new_layer();

    // Store a reference to the Socket.IO instance
    let io_clone = io.clone();
    let db_clone = db.clone();
    // Spawn the pool event listener in a separate task
    tokio::spawn(async move {
        let mut listener = PgListener::connect_with(&db_clone.pool).await.unwrap();
        listener.listen_all(vec!["pool_inserted"]).await.unwrap();
        let mut stream = listener.into_stream();
        while let Ok(Some(notification)) = stream.try_next().await {
            if notification.channel() == "pool_inserted" {
                if let Ok(raw_notification) =
                    serde_json::from_str::<PoolEvent>(notification.payload())
                {
                    let db_pool = DBPool {
                        pool_address: hex::decode(&raw_notification.pool_address)
                            .map_err(|_| "parse pool address".to_string())
                            .unwrap(),
                        creator: hex::decode(&raw_notification.creator)
                            .map_err(|_| "parse creator".to_string())
                            .unwrap(),
                        token_base_address: hex::decode(&raw_notification.token_base_address)
                            .map_err(|_| "parse token base address".to_string())
                            .unwrap(),
                        token_quote_address: hex::decode(&raw_notification.token_quote_address)
                            .map_err(|_| "parse token quote address".to_string())
                            .unwrap(),
                        pool_base_address: hex::decode(&raw_notification.pool_base_address)
                            .map_err(|_| "parse pool base address".to_string())
                            .unwrap(),
                        pool_quote_address: hex::decode(&raw_notification.pool_quote_address)
                            .map_err(|_| "parse pool quote address".to_string())
                            .unwrap(),
                        slot: raw_notification.slot,

                        factory: raw_notification.factory,
                        pre_factory: raw_notification.pre_factory.unwrap_or_default(),
                        initial_token_base_reserve: Decimal::from(
                            raw_notification.initial_token_base_reserve,
                        ),
                        initial_token_quote_reserve: Decimal::from(
                            raw_notification.initial_token_quote_reserve,
                        ),
                        curve_percentage: raw_notification
                            .curve_percentage
                            .map(|x| Decimal::from_f64(x.to_f64().unwrap()).unwrap()),
                        reversed: raw_notification.reversed,
                        hash: hex::decode(&raw_notification.hash)
                            .map_err(|_| "parse hash".to_string())
                            .unwrap(),
                        metadata: raw_notification.metadata,
                        created_at: raw_notification.created_at,
                        updated_at: raw_notification.updated_at,
                    };
                    // let data = ResponsePool::try_from(db_pool).unwrap();
                    // println!("Pool event: {:?}", data);
                    let query = r#"
                        WITH tok AS (
                            SELECT t.mint_address, t.name, t.symbol, t.image, t.decimals, 
                                   t.website, t.twitter, t.telegram, t.supply::numeric AS token_supply, 
                                   power(10::numeric, t.decimals)::numeric AS scale_factor
                            FROM tokens t WHERE t.mint_address = $1::bytea
                        ),
                        vol_24h AS (
                            SELECT COALESCE(buy_volume + sell_volume, 0) AS volume_sol, 
                                   COALESCE(buy_count + sell_count, 0)::int8 AS num_txns,
                                   COALESCE(buy_count, 0)::int8 AS num_buys,
                                    COALESCE(sell_count, 0)::int8 AS num_sells
                            FROM swaps_24h WHERE pool_address = $2::bytea
                        ),
                        latest_swap AS (
                            SELECT DISTINCT ON (s.pool_address)
                                   s.pool_address,
                                   s.base_reserve AS latest_base_reserve,
                                   s.quote_reserve AS latest_quote_reserve,
                                   s.price_sol AS latest_price_sol
                            FROM swaps s
                            WHERE s.pool_address = $2::bytea
                            ORDER BY s.pool_address, s.created_at DESC
                        ),

                        
                        
                        holders_base AS (
                            SELECT COUNT(DISTINCT a.owner) AS num_holders
                            FROM accounts a WHERE a.mint = $1::bytea 
                            AND a.owner NOT IN ($2::bytea, $5::bytea, $6::bytea)
                        ),
                        top10_holders AS (
                            SELECT COALESCE(SUM(amount), 0) AS top10_amount_raw
                            FROM (
                                SELECT a.amount,
                                       ROW_NUMBER() OVER (ORDER BY a.amount DESC) as rn
                                FROM accounts a 
                                WHERE a.mint = 1::bytea 
                                AND a.owner NOT IN ($2::bytea, $5::bytea, $6::bytea)
                            ) ranked
                            WHERE rn <= 10
                        ),
                        dev_hold AS (
                            SELECT COALESCE(MAX(a.amount), 0) AS dev_amount_raw
                            FROM accounts a WHERE a.mint = $7::bytea AND a.owner = $8::bytea AND a.owner <> $2::bytea
                        ),
                        snipers_holds AS (
                            SELECT COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
                            FROM swaps s WHERE s.pool_address = $2::bytea AND s.swap_type = 'BUY' 
                            AND s.creator NOT IN ($5::bytea, $4::bytea, $8::bytea)
                        ),
                        migration AS (
                            SELECT COUNT(*) FILTER (WHERE p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
                            FROM pools p2 WHERE p2.creator = $8::bytea
                        ),
                        dev_wallet_funding AS (
                            SELECT DISTINCT ON (ts.destination)
                                ts.source,
                                ts.destination,
                                ts.amount,
                                ts.hash,
                                ts.created_at
                            FROM transfer_sol ts
                            WHERE ts.destination = $8::bytea
                            ORDER BY ts.destination, ts.created_at ASC
                        )
                        SELECT 
                            t.name, t.symbol, t.image, t.decimals, t.website, t.twitter, t.telegram, t.token_supply,
                            v.volume_sol, v.num_txns, v.num_buys, v.num_sells,
                            h.num_holders, d.dev_amount_raw, sh.snipers_amount_raw,
                            m.migration_count, f.source as funding_wallet_address, f.destination as wallet_address, f.amount as amount_sol, f.hash as transfer_hash, f.created_at as funded_at,
                            COALESCE(((d.dev_amount_raw / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0), 0) AS dev_holds_percent,
                            COALESCE((sh.snipers_amount_raw * 100.0) / nullif(t.token_supply,0), 0) AS snipers_holding_percent,
                            COALESCE(((th.top10_amount_raw / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0), 0) AS top10_holders_percent,
                            COALESCE(ls.latest_price_sol * t.token_supply, 0) AS market_cap_sol,
                            COALESCE(ls.latest_quote_reserve, 0) AS liquidity_sol,
                            COALESCE(ls.latest_base_reserve, 0) AS liquidity_token,
                            COALESCE(ls.latest_price_sol, 0) AS current_price_sol
                            
                        FROM tok t
                        CROSS JOIN vol_24h v
                        CROSS JOIN holders_base h
                        CROSS JOIN top10_holders th
                        CROSS JOIN dev_hold d
                        CROSS JOIN snipers_holds sh
                        CROSS JOIN migration m
                        LEFT JOIN latest_swap ls ON ls.pool_address = $2::bytea
                        LEFT JOIN dev_wallet_funding f ON true
                    "#;

                    let result = sqlx::query(query)
                        .bind(&db_pool.token_base_address) // $1: token mint address
                        .bind(&db_pool.pool_address) // $2: pool address
                        .bind(&db_pool.pool_base_address) // $5: pool base address
                        .bind(&db_pool.pool_quote_address) // $6: pool quote address
                        .bind(&db_pool.creator) // $8: creator
                        .fetch_one(&db_clone.pool)
                        .await
                        .map_err(|e| {
                            println!("Error: {:?}", e);
                            e
                        })
                        .unwrap();
                    println!("Result: {:?}", result);
                    let pulse_data = PulseDataResponse {
                        pair_address: bs58::encode(db_pool.pool_address).into_string(),
                        token_address: bs58::encode(db_pool.token_base_address).into_string(),
                        creator: bs58::encode(db_pool.creator).into_string(),
                        token_name: result.get::<Option<String>, _>("name"),
                        token_symbol: result.get::<Option<String>, _>("symbol"),
                        token_image: result.get::<Option<String>, _>("image"),
                        token_decimals: result.get::<i16, _>("decimals") as u8,

                        protocol: db_pool.factory,
                        website: result.get::<Option<String>, _>("website"),
                        twitter: result.get::<Option<String>, _>("twitter"),
                        telegram: result.get::<Option<String>, _>("telegram"),
                        top10_holders_percent: result.get::<Decimal, _>("top10_holders_percent"),

                        dev_holds_percent: result.get::<Decimal, _>("dev_holds_percent"),

                        snipers_holds_percent: result.get::<Decimal, _>("snipers_holding_percent"),

                        volume_sol: result.get::<Decimal, _>("volume_sol"),

                        market_cap_sol: result.get::<Decimal, _>("market_cap_sol"),

                        liquidity_sol: result.get::<Decimal, _>("liquidity_sol"),

                        liquidity_token: result.get::<Decimal, _>("liquidity_token"),

                        bonding_curve_percent: db_pool.curve_percentage.unwrap_or(Decimal::from(0)),

                        supply: result.get::<Decimal, _>("token_supply"),

                        num_txns: result.get::<i64, _>("num_txns"),
                        num_buys: result.get::<i64, _>("num_buys"),
                        num_sells: result.get::<i64, _>("num_sells"),
                        num_holders: result.get::<i64, _>("num_holders"),
                        created_at: db_pool.created_at,
                        migration_count: result.get::<i64, _>("migration_count"),
                        dev_wallet_funding: if let Some(funding_wallet) =
                            result.get::<Option<Vec<u8>>, _>("funding_wallet_address")
                        {
                            Some(DevWalletFunding {
                                funding_wallet_address: bs58::encode(funding_wallet).into_string(),
                                wallet_address: bs58::encode(
                                    result.get::<Vec<u8>, _>("wallet_address"),
                                )
                                .into_string(),
                                amount_sol: result.get("amount_sol"),
                                hash: bs58::encode(result.get::<Vec<u8>, _>("transfer_hash"))
                                    .into_string(),
                                funded_at: result.get::<DateTime<Utc>, _>("funded_at"),
                            })
                        } else {
                            None
                        },
                    };

                    let _ = io_clone.emit("new-pair", &pulse_data).await;
                } else {
                    eprintln!(
                        "Failed to parse pool event JSON: {}",
                        notification.payload()
                    );
                }
            }
        }
        // db_clone
        //     .listen_to_pool_events(move |pool_event| {
        //         let data = ResponsePool::try_from(pool_event).unwrap();
        //         println!("Pool event: {:?}", data);

        //         // Broadcast to all clients in the "pool-events" room
        //         let _ = io_clone.emit("new-pair", &data).await;
        //     })
        //     .await
        //     .unwrap();
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
