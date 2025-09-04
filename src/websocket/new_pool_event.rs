use std::error::Error;

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, prelude::FromPrimitive};

use serde_json::Value;
use sqlx::{Row, postgres::PgNotification};

use crate::{
    models::pool::DBPool,
    services::db::{DbService, PoolEvent},
    types::pulse::{DevWalletFunding, PulseDataResponse},
};

pub async fn on_new_pool_event(
    notification: &PgNotification,
    db_clone: &DbService,
) -> Result<(Value, DBPool), Box<dyn Error + Send + Sync>> {
    let raw_notification = serde_json::from_str::<DBPool>(notification.payload()).unwrap();
    let db_pool = DBPool {
        pool_address: raw_notification.pool_address,

        creator:raw_notification.creator,

        token_base_address: raw_notification.token_base_address
            ,
        token_quote_address: raw_notification.token_quote_address,

        pool_base_address:raw_notification.pool_base_address
            ,
        pool_quote_address:raw_notification.pool_quote_address
           ,

        slot: raw_notification.slot,

        factory: raw_notification.factory,
        pre_factory: raw_notification.pre_factory,
        initial_token_base_reserve: Decimal::from(raw_notification.initial_token_base_reserve),
        initial_token_quote_reserve: Decimal::from(raw_notification.initial_token_quote_reserve),
        curve_percentage: raw_notification.curve_percentage,
        reversed: raw_notification.reversed,
        hash: raw_notification.hash,

        metadata: raw_notification.metadata,
        created_at: raw_notification.created_at,
        updated_at: raw_notification.updated_at,
    };

    if db_pool.factory != "PumpFun" {
        return Err("factory is not PumpFun".to_string().into());
    }

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
            AND a.owner NOT IN ($2::bytea, $3::bytea, $4::bytea)
        ),
        top10_holders AS (
            SELECT COALESCE(SUM(amount), 0) AS top10_amount_raw
            FROM (
                SELECT a.amount,
                       ROW_NUMBER() OVER (ORDER BY a.amount DESC) as rn
                FROM accounts a 
                WHERE a.mint = $1::bytea 
                AND a.owner NOT IN ($2::bytea, $3::bytea, $4::bytea)
            ) ranked
            WHERE rn <= 10
        ),
        dev_hold AS (
            SELECT COALESCE(MAX(a.amount), 0) AS dev_amount_raw
            FROM accounts a WHERE a.mint = $1::bytea AND a.owner = $5::bytea AND a.owner <> $2::bytea
        ),
        snipers_holds AS (
            SELECT COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
            FROM swaps s WHERE s.pool_address = $2::bytea AND s.swap_type = 'BUY' 
            AND s.creator NOT IN ($5::bytea, $3::bytea, $4::bytea)
        ),
        migration AS (
            SELECT COUNT(*) FILTER (WHERE p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
            FROM pools p2 WHERE p2.creator = $5::bytea
        ),
        dev_wallet_funding AS (
            SELECT DISTINCT ON (ts.destination)
                ts.source,
                ts.destination,
                ts.amount,
                ts.hash,
                ts.created_at
            FROM transfer_sol ts
            WHERE ts.destination = $5::bytea
            ORDER BY ts.destination, ts.created_at ASC
        )
        SELECT 
            COALESCE(t.name, 'Unknown') as name,
            COALESCE(t.symbol, 'UNKNOWN') as symbol,
            COALESCE(t.image, '') as image,
            COALESCE(t.decimals, 0) as decimals,
            t.website, t.twitter, t.telegram, 
            COALESCE(t.token_supply, 0) as token_supply,
            COALESCE(v.volume_sol, 0) as volume_sol, 
            COALESCE(v.num_txns, 0) as num_txns, 
            COALESCE(v.num_buys, 0) as num_buys, 
            COALESCE(v.num_sells, 0) as num_sells,
            COALESCE(h.num_holders, 0) as num_holders, 
            COALESCE(d.dev_amount_raw, 0) as dev_amount_raw, 
            COALESCE(sh.snipers_amount_raw, 0) as snipers_amount_raw,
            COALESCE(m.migration_count, 0) as migration_count, 
            f.source as funding_wallet_address, 
            f.destination as wallet_address, 
            f.amount as amount_sol, 
            f.hash as transfer_hash, 
            f.created_at as funded_at,
            COALESCE(((COALESCE(d.dev_amount_raw, 0) / nullif(COALESCE(t.scale_factor, 1), 0)) * 100.0) / nullif(COALESCE(t.token_supply, 1), 0), 0) AS dev_holds_percent,
            COALESCE((COALESCE(sh.snipers_amount_raw, 0) * 100.0) / nullif(COALESCE(t.token_supply, 1), 0), 0) AS snipers_holding_percent,
            COALESCE(((COALESCE(th.top10_amount_raw, 0) / nullif(COALESCE(t.scale_factor, 1), 0)) * 100.0) / nullif(COALESCE(t.token_supply, 1), 0), 0) AS top10_holders_percent,
            COALESCE(COALESCE(ls.latest_price_sol, 0) * COALESCE(t.token_supply, 0), 0) AS market_cap_sol,
            COALESCE(ls.latest_quote_reserve, 0) AS liquidity_sol,
            COALESCE(ls.latest_base_reserve, 0) AS liquidity_token,
            COALESCE(ls.latest_price_sol, 0) AS current_price_sol
            
        FROM (SELECT 1 as dummy) dummy
        LEFT JOIN tok t ON true
        LEFT JOIN vol_24h v ON true
        LEFT JOIN holders_base h ON true
        LEFT JOIN top10_holders th ON true
        LEFT JOIN dev_hold d ON true
        LEFT JOIN snipers_holds sh ON true
        LEFT JOIN migration m ON true
        LEFT JOIN latest_swap ls ON ls.pool_address = $2::bytea
        LEFT JOIN dev_wallet_funding f ON true
    "#;

    let result = sqlx::query(query)
        .bind(&db_pool.token_base_address) // $1: token mint address
        .bind(&db_pool.pool_address) // $2: pool address
        .bind(&db_pool.pool_base_address) // $3: pool base address
        .bind(&db_pool.pool_quote_address) // $4: pool quote address
        .bind(&db_pool.creator) // $5: creator
        .fetch_one(&db_clone.pool)
        .await
        .map_err(|e| {
            println!("Error: {:?}", e);
            e
        });
    if result.is_err() {
        return Err("Failed to get pulse data".to_string().into());
    }
    let result = result.unwrap();

    // println!("Result: {:?}", result);
    let pulse_data = serde_json::json!( {
        "pair_address": bs58::encode(&db_pool.pool_address).into_string(),
        "token_address": bs58::encode(&db_pool.token_base_address).into_string(),
        "creator": bs58::encode(&db_pool.creator).into_string(),
        "token_name": result.get::<Option<String>, _>("name"),
        "token_symbol": result.get::<Option<String>, _>("symbol"),
        "token_image": result.get::<Option<String>, _>("image"),
        "token_decimals": result.get::<i32, _>("decimals") as u8,

        "protocol": &db_pool.factory,
        "website": result.get::<Option<String>, _>("website"),
        "twitter": result.get::<Option<String>, _>("twitter"),
        "telegram": result.get::<Option<String>, _>("telegram"),
        "top10_holders_percent": result.get::<Decimal, _>("top10_holders_percent"),

        "dev_holds_percent": result.get::<Decimal, _>("dev_holds_percent"),

        "snipers_holds_percent": result.get::<Decimal, _>("snipers_holding_percent"),

        "volume_sol": result.get::<Decimal, _>("volume_sol"),

        "market_cap_sol": result.get::<Decimal, _>("market_cap_sol"),

        "liquidity_sol": result.get::<Decimal, _>("liquidity_sol"),

        "liquidity_token": result.get::<Decimal, _>("liquidity_token"),

        "bonding_curve_percent": &db_pool.curve_percentage.unwrap_or(Decimal::from(0)),

        "supply": result.get::<Decimal, _>("token_supply"),

        "num_txns": result.get::<i64, _>("num_txns"),
        "num_buys": result.get::<i64, _>("num_buys"),
        "num_sells": result.get::<i64, _>("num_sells"),
        "num_holders": result.get::<i64, _>("num_holders"),
        "created_at": &db_pool.created_at,
        "migration_count": result.get::<i64, _>("migration_count"),
        "dev_wallet_funding": if let Some(funding_wallet) =
            result.get::<Option<Vec<u8>>, _>("funding_wallet_address")
        {
            Some(DevWalletFunding {
                funding_wallet_address: bs58::encode(funding_wallet).into_string(),
                wallet_address: bs58::encode(result.get::<Vec<u8>, _>("wallet_address"))
                    .into_string(),
                amount_sol: result.get("amount_sol"),
                hash: bs58::encode(result.get::<Vec<u8>, _>("transfer_hash")).into_string(),
                funded_at: result.get::<DateTime<Utc>, _>("funded_at"),
            })
        } else {
            None
        },
    });
    Ok((pulse_data, db_pool))
}
