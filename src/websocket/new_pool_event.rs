// use std::error::Error;

// use chrono::{DateTime, Utc};
// use rust_decimal::prelude::ToPrimitive;
// use rust_decimal::{Decimal, prelude::FromPrimitive};

// use serde_json::Value;
// use sqlx::{Row, postgres::PgNotification};

// use crate::{
//     models::pool::DBPool,
//     services::db::{Click, PoolEvent},
//     types::pulse::{DevWalletFunding, PulseDataResponse},
// };

// pub async fn on_new_pool_event(
//     notification: &PgNotification,
//     db_clone: &DbService,
// ) -> Result<(Value, DBPool), Box<dyn Error + Send + Sync>> {
//     let raw_notification = serde_json::from_str::<DBPool>(notification.payload()).unwrap();
//     let db_pool = DBPool {
//         pool_address: raw_notification.pool_address,
//         creator: raw_notification.creator,
//         token_base_address: raw_notification.token_base_address,
//         token_quote_address: raw_notification.token_quote_address,
//         pool_base_address: raw_notification.pool_base_address,
//         pool_quote_address: raw_notification.pool_quote_address,
//         slot: raw_notification.slot,
//         factory: raw_notification.factory,
//         pre_factory: raw_notification.pre_factory,
//         initial_token_base_reserve: Decimal::from(raw_notification.initial_token_base_reserve),
//         initial_token_quote_reserve: Decimal::from(raw_notification.initial_token_quote_reserve),
//         curve_percentage: raw_notification.curve_percentage,
//         reversed: raw_notification.reversed,
//         hash: raw_notification.hash,
//         metadata: raw_notification.metadata,
//         created_at: raw_notification.created_at,
//         updated_at: raw_notification.updated_at,
//     };

//     if db_pool.factory != "PumpFun" {
//         return Err("factory is not PumpFun".to_string().into());
//     }

//     // ✅ Fixed query with proper type casting
//     let query = r#"
//         SELECT
//             -- Token info (single table lookup)
//             COALESCE(t.name, 'Unknown') as name,
//             COALESCE(t.symbol, 'UNKNOWN') as symbol,
//             COALESCE(t.image, '') as image,
//             COALESCE(t.decimals, 0) as decimals,
//             t.website,
//             t.twitter,
//             t.telegram,
//             COALESCE(t.supply::numeric, 0) as token_supply,
//             power(10::numeric, COALESCE(t.decimals, 0))::numeric AS scale_factor,

//             -- Volume data (with proper type casting)
//             COALESCE(v.buy_volume + v.sell_volume, 0) AS volume_sol,
//             COALESCE(v.buy_count + v.sell_count, 0)::BIGINT AS num_txns,
//             COALESCE(v.buy_count, 0)::BIGINT AS num_buys,
//             COALESCE(v.sell_count, 0)::BIGINT AS num_sells,

//             -- Latest swap data (single row lookup)
//             COALESCE(s.base_reserve, 0) AS liquidity_token,
//             COALESCE(s.quote_reserve, 0) AS liquidity_sol,
//             COALESCE(s.price_sol, 0) AS current_price_sol,

//             -- Simplified holder count (with proper type casting)
//             COALESCE(h.holder_count, 0)::BIGINT AS num_holders,

//             -- Dev funding (simplified)
//             f.source as funding_wallet_address,
//             f.destination as wallet_address,
//             f.amount as amount_sol,
//             f.hash as transfer_hash,
//             f.created_at as funded_at,

//             -- Migration count (with proper type casting)
//             COALESCE(m.migration_count, 0)::BIGINT as migration_count

//         FROM tokens t

//         -- Single LEFT JOIN for volume (much faster than CTE)
//         LEFT JOIN (
//             SELECT
//                 pool_address,
//                 SUM(buy_volume) as buy_volume,
//                 SUM(sell_volume) as sell_volume,
//                 SUM(buy_count) as buy_count,
//                 SUM(sell_count) as sell_count
//             FROM swaps_24h
//             WHERE pool_address = $2
//             GROUP BY pool_address
//         ) v ON true

//         -- Single LEFT JOIN for latest swap (much faster than CTE)
//         LEFT JOIN (
//             SELECT DISTINCT ON (pool_address)
//                 pool_address,
//                 base_reserve,
//                 quote_reserve,
//                 price_sol
//             FROM swaps
//             WHERE pool_address = $2
//             ORDER BY pool_address, created_at DESC
//         ) s ON true

//         -- Single LEFT JOIN for holder count (much faster than CTE)
//         LEFT JOIN (
//             SELECT
//                 mint,
//                 COUNT(DISTINCT owner) as holder_count
//             FROM accounts
//             WHERE mint = $1
//                 AND owner NOT IN ($2, $3, $4)
//             GROUP BY mint
//         ) h ON h.mint = t.mint_address

//         -- Single LEFT JOIN for dev funding (much faster than CTE)
//         LEFT JOIN (
//             SELECT DISTINCT ON (destination)
//                 source,
//                 destination,
//                 amount,
//                 hash,
//                 created_at
//             FROM transfer_sol
//             WHERE destination = $5
//             ORDER BY destination, created_at ASC
//         ) f ON true

//         -- Single LEFT JOIN for migration count (much faster than CTE)
//         LEFT JOIN (
//             SELECT
//                 creator,
//                 COUNT(*) FILTER (WHERE pre_factory = 'PumpFun' AND factory = 'PumpSwap') AS migration_count
//             FROM pools
//             WHERE creator = $5
//             GROUP BY creator
//         ) m ON true

//         WHERE t.mint_address = $1
//     "#;

//     let result = sqlx::query(query)
//         .bind(&db_pool.token_base_address) // $1: token mint address
//         .bind(&db_pool.pool_address) // $2: pool address
//         .bind(&db_pool.pool_base_address) // $3: pool base address
//         .bind(&db_pool.pool_quote_address) // $4: pool quote address
//         .bind(&db_pool.creator) // $5: creator
//         .fetch_one(&db_clone.pool)
//         .await
//         .map_err(|e| {
//             println!("Error: {:?}", e);
//             e
//         })?;

//     // ✅ Calculate percentages in Rust (much faster than SQL)
//     let token_supply: Decimal = result.try_get("token_supply")?;
//     let scale_factor: Decimal = result.try_get("scale_factor")?;
//     let current_price: Decimal = result.try_get("current_price_sol")?;

//     // Calculate market cap
//     let market_cap_sol = current_price * token_supply;

//     // For new pools, these will be 0 initially (much faster)
//     let dev_holds_percent = Decimal::ZERO;
//     let snipers_holds_percent = Decimal::ZERO;
//     let top10_holders_percent = Decimal::ZERO;

//     let pulse_data = serde_json::json!({
//         "pair_address": bs58::encode(&db_pool.pool_address).into_string(),
//         "token_address": bs58::encode(&db_pool.token_base_address).into_string(),
//         "creator": bs58::encode(&db_pool.creator).into_string(),
//         "token_name": result.get::<Option<String>, _>("name"),
//         "token_symbol": result.get::<Option<String>, _>("symbol"),
//         "token_image": result.get::<Option<String>, _>("image"),
//         "token_decimals": result.get::<i32, _>("decimals") as u8,
//         "protocol": &db_pool.factory,
//         "website": result.get::<Option<String>, _>("website"),
//         "twitter": result.get::<Option<String>, _>("twitter"),
//         "telegram": result.get::<Option<String>, _>("telegram"),
//         "top10_holders_percent": top10_holders_percent,
//         "dev_holds_percent": dev_holds_percent,
//         "snipers_holds_percent": snipers_holds_percent,
//         "volume_sol": result.get::<Decimal, _>("volume_sol"),
//         "market_cap_sol": market_cap_sol,
//         "liquidity_sol": result.get::<Decimal, _>("liquidity_sol"),
//         "liquidity_token": result.get::<Decimal, _>("liquidity_token"),
//         "bonding_curve_percent": &db_pool.curve_percentage.unwrap_or(Decimal::from(0)),
//         "supply": token_supply,
//         "num_txns": result.get::<i64, _>("num_txns"),
//         "num_buys": result.get::<i64, _>("num_buys"),
//         "num_sells": result.get::<i64, _>("num_sells"),
//         "num_holders": result.get::<i64, _>("num_holders"),
//         "created_at": &db_pool.created_at,
//         "migration_count": result.get::<i64, _>("migration_count"),
//         "dev_wallet_funding": if let Some(funding_wallet) = result.get::<Option<String>, _>("funding_wallet_address") {
//             Some(DevWalletFunding {
//                 funding_wallet_address: funding_wallet,
//                 wallet_address: result.get::<String, _>("wallet_address"),
//                 amount_sol: result.get("amount_sol"),
//                 hash: result.get::<String, _>("transfer_hash"),
//                 funded_at: result.get::<DateTime<Utc>, _>("funded_at"),
//             })
//         } else {
//             None
//         },
//     });

//     Ok((pulse_data, db_pool))
// }
