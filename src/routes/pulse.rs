use axum::{
    Json,
    extract::{FromRequest, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Number, json};
use sqlx::{PgPool, Row};
use tracing::info;

use crate::{
    models::pool::{DBPool, Pool, ResponsePool},
    services::db::DbService,
    types::{
        filter::{PulseFilter, PulseTable},
        pulse::{DevWalletFunding, PulseDataResponse},
    },
    utils::{calculate_market_cap, calculate_percentage},
};

pub async fn pulse(
    State(db): State<DbService>,
    Json(input): Json<PulseFilter>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let filters = input.filters;
    println!("{:?}", filters);
    let table = input.table;
    println!("{:?}", table);
    match table {
        PulseTable::NewPairs => {
            let mut query = String::new();

            query.push_str(
                r#"
WITH all_pools AS (
  SELECT
    p.pool_address,
    p.creator,
    p.token_base_address,
    p.token_quote_address,
    p.pool_base_address,
    p.pool_quote_address,
    p.factory,
    p.created_at,
    p.initial_token_base_reserve,
    p.initial_token_quote_reserve,
    p.curve_percentage
  FROM pools p
 WHERE p.created_at >= now() - interval '24 hours'
 AND p.curve_percentage < 50


"#,
            );
            if let Some(min_age) = filters.age.min {
                // min_age is minutes from now, so we want pools created at least min_age minutes ago
                query.push_str(&format!(
                    " AND p.created_at <= now() - interval '{} minutes'",
                    min_age
                ));
            }
            if let Some(max_age) = filters.age.max {
                // max_age is minutes from now, so we want pools created at most max_age minutes ago
                query.push_str(&format!(
                    " AND p.created_at >= now() - interval '{} minutes'",
                    max_age
                ));
            }
            // Apply factory filters
            let mut factory_conditions = Vec::new();
            if filters.factories.pump_fun {
                factory_conditions.push("p.factory = 'PumpFun'");
            }
            if filters.factories.pump_swap {
                factory_conditions.push("p.factory = 'PumpSwap'");
            }

            if !factory_conditions.is_empty() {
                query.push_str(" AND (");
                query.push_str(&factory_conditions.join(" OR "));
                query.push_str(")");
            }
            query.push_str(r#"
          ),
          
tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply::numeric AS token_supply,
    power(10::numeric, t.decimals)::numeric AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    ls.latest_base_reserve,
    ls.latest_quote_reserve,
    ls.latest_price_sol
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      s.base_reserve  AS latest_base_reserve,
      s.quote_reserve AS latest_quote_reserve,
      s.price_sol     AS latest_price_sol
    FROM swaps s
    WHERE s.pool_address = r.pool_address
    ORDER BY s.created_at DESC
    LIMIT 1
  ) ls ON TRUE
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM all_pools r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  GROUP BY r.pool_address
),
top10_holders AS (
  SELECT pool_address, SUM(amount) AS top10_amount_raw
  FROM (
    SELECT r.pool_address, a.amount,
           ROW_NUMBER() OVER (PARTITION BY r.pool_address ORDER BY a.amount DESC) AS rn
    FROM all_pools r
    JOIN accounts a
      ON a.mint = r.token_base_address
     AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  ) x
  WHERE rn <= 10
  GROUP BY pool_address
),
dev_hold AS (
  SELECT
    r.pool_address,
    coalesce(max(a.amount), 0) AS dev_amount_raw
  FROM all_pools r
  LEFT JOIN accounts a
    ON a.mint  = r.token_base_address
   AND a.owner = r.creator
   AND a.owner <> r.pool_address
  GROUP BY r.pool_address
),
snipers_holds AS (
  SELECT s.pool_address,
         COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
  FROM swaps s
  JOIN all_pools r ON r.pool_address = s.pool_address
  WHERE s.swap_type = 'BUY'
    AND s.creator <> r.pool_address
    AND s.creator <> r.pool_base_address
    AND s.creator <> r.pool_quote_address
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    df.source,
    df.destination,
    df.amount,
    df.hash,
    df.created_at
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      ts.source,
      ts.destination,
      ts.amount,
      ts.hash,
      ts.created_at
    FROM transfer_sol ts
    WHERE ts.destination = r.creator
    ORDER BY ts.created_at ASC
    LIMIT 1
  ) df ON TRUE
),
migration AS (
  SELECT r.creator,
         count(*) FILTER (WHERE p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM all_pools r
  LEFT JOIN pools p2 ON p2.creator = r.creator
  GROUP BY r.creator
),
vol_24h AS (         -- choose one source
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         SUM(s.buy_count)::int8            AS num_buys,
         SUM(s.sell_count)::int8           AS num_sells,
         SUM(s.buy_count + s.sell_count)::int8 AS num_txns
  FROM swaps_5m s
  JOIN all_pools r USING (pool_address)
  WHERE  s.bucket_start <  now() - interval '5 minutes'
  GROUP BY s.pool_address
)
SELECT
  r.pool_address,
  r.creator,
  r.token_base_address,
  r.token_quote_address,
  r.factory,
  r.created_at,
  r.initial_token_base_reserve,
  r.initial_token_quote_reserve,
  coalesce(r.curve_percentage, 0) AS bonding_curve_percent,

  -- token meta
  t.name, t.symbol, t.image, t.decimals, t.website, t.twitter, t.telegram, t.mint_address,
  t.token_supply,
  t.scale_factor,

  -- liquidity/price (fallback to initial if no swaps yet)
  coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) AS liquidity_sol,
  coalesce(ls.latest_base_reserve,  r.initial_token_base_reserve)  AS liquidity_token,
  coalesce(ls.latest_price_sol, 0)                                 AS current_price_sol,

  -- holders
  coalesce(h.num_holders, 0)                                       AS num_holders,

  -- raw amounts for calculation in Rust
  coalesce(th.top10_amount_raw, 0)                                 AS top10_amount_raw,
  coalesce(d.dev_amount_raw, 0)                                    AS dev_amount_raw,
  coalesce(sh.snipers_amount_raw, 0)                               AS snipers_amount_raw,

  -- migrations
  coalesce(m.migration_count, 0)                                   AS migration_count,

  coalesce(v.volume_sol, 0) AS volume_sol,
coalesce(v.num_txns,   0) AS num_txns,
coalesce(v.num_buys,   0) AS num_buys,
coalesce(v.num_sells,  0) AS num_sells,

  -- dev wallet funding (first transfer)
  df.source       AS funding_wallet_address,
  df.destination  AS wallet_address,
  df.amount       AS amount_sol,
  df.hash         AS transfer_hash,
  df.created_at   AS funded_at

FROM all_pools r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator
          "#);
            // Apply remaining filters (excluding age since it's already applied above)
            let mut where_conditions = Vec::new();

            // Top 10 holders filter
            if let Some(min_top10) = filters.top10_holders.min {
                where_conditions.push(format!(
                        "((coalesce(th.top10_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                        min_top10
                    ));
            }
            if let Some(max_top10) = filters.top10_holders.max {
                where_conditions.push(format!(
                        "((coalesce(th.top10_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                        max_top10
                    ));
            }

            // Dev holding filter
            if let Some(min_dev) = filters.dev_holding.min {
                where_conditions.push(format!(
                        "((coalesce(d.dev_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                        min_dev
                    ));
            }
            if let Some(max_dev) = filters.dev_holding.max {
                where_conditions.push(format!(
                        "((coalesce(d.dev_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                        max_dev
                    ));
            }
            if let Some(min_snipers) = filters.snipers_holding.min {
                where_conditions.push(format!(
                    "((coalesce(sh.snipers_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                    min_snipers
                ));
            }
            if let Some(max_snipers) = filters.snipers_holding.max {
                where_conditions.push(format!(
                    "((coalesce(sh.snipers_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                    max_snipers
                ));
            }

            // Holders count filter
            if let Some(min_holders) = filters.holders.min {
                where_conditions.push(format!("coalesce(h.num_holders, 0) >= {}", min_holders));
            }
            if let Some(max_holders) = filters.holders.max {
                where_conditions.push(format!("coalesce(h.num_holders, 0) <= {}", max_holders));
            }

            // Bonding curve filter
            if let Some(min_bonding) = filters.bonding_curve.min {
                where_conditions.push(format!("r.curve_percentage >= {}", min_bonding));
            }
            if let Some(max_bonding) = filters.bonding_curve.max {
                where_conditions.push(format!("r.curve_percentage <= {}", max_bonding));
            }

            // Liquidity filter
            if let Some(min_liquidity) = filters.liquidity.min {
                where_conditions.push(format!(
                    "coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) >= {}",
                    min_liquidity
                ));
            }
            if let Some(max_liquidity) = filters.liquidity.max {
                where_conditions.push(format!(
                    "coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) <= {}",
                    max_liquidity
                ));
            }

            // Volume filter
            if let Some(min_volume) = filters.volume.min {
                where_conditions.push(format!("coalesce(v.volume_sol, 0) >= {}", min_volume));
            }
            if let Some(max_volume) = filters.volume.max {
                where_conditions.push(format!("coalesce(v.volume_sol, 0) <= {}", max_volume));
            }

            // Market cap filter
            if let Some(min_market_cap) = filters.market_cap.min {
                where_conditions.push(format!(
                    "(coalesce(ls.latest_price_sol, 0) * t.token_supply) >= {}",
                    min_market_cap
                ));
            }
            if let Some(max_market_cap) = filters.market_cap.max {
                where_conditions.push(format!(
                    "(coalesce(ls.latest_price_sol, 0) * t.token_supply) <= {}",
                    max_market_cap
                ));
            }

            // Transactions filter
            if let Some(min_txns) = filters.txns.min {
                where_conditions.push(format!("coalesce(v.num_txns, 0) >= {}", min_txns));
            }
            if let Some(max_txns) = filters.txns.max {
                where_conditions.push(format!("coalesce(v.num_txns, 0) <= {}", max_txns));
            }

            // Num buys filter
            if let Some(min_buys) = filters.num_buys.min {
                where_conditions.push(format!("coalesce(v.num_buys, 0) >= {}", min_buys));
            }
            if let Some(max_buys) = filters.num_buys.max {
                where_conditions.push(format!("coalesce(v.num_buys, 0) <= {}", max_buys));
            }

            // Num sells filter
            if let Some(min_sells) = filters.num_sells.min {
                where_conditions.push(format!("coalesce(v.num_sells, 0) >= {}", min_sells));
            }
            if let Some(max_sells) = filters.num_sells.max {
                where_conditions.push(format!("coalesce(v.num_sells, 0) <= {}", max_sells));
            }

            // Migration count filter
            if let Some(min_migrations) = filters.num_migrations.min {
                where_conditions.push(format!(
                    "coalesce(m.migration_count, 0) >= {}",
                    min_migrations
                ));
            }
            if let Some(max_migrations) = filters.num_migrations.max {
                where_conditions.push(format!(
                    "coalesce(m.migration_count, 0) <= {}",
                    max_migrations
                ));
            }

            // Social media filters
            if filters.twitter {
                where_conditions.push("t.twitter IS NOT NULL AND t.twitter != ''".to_string());
            }
            if filters.website {
                where_conditions.push("t.website IS NOT NULL AND t.website != ''".to_string());
            }
            if filters.telegram {
                where_conditions.push("t.telegram IS NOT NULL AND t.telegram != ''".to_string());
            }
            if filters.at_least_one_social {
                where_conditions.push("(t.twitter IS NOT NULL AND t.twitter != '') OR (t.website IS NOT NULL AND t.website != '') OR (t.telegram IS NOT NULL AND t.telegram != '')".to_string());
            }

            // Search keywords filter
            if !filters.search_keywords.is_empty() {
                let search_conditions: Vec<String> = filters.search_keywords
                        .iter()
                        .map(|keyword| {
                            format!(
                                "(LOWER(t.name) LIKE LOWER('%{}%') OR LOWER(t.symbol) LIKE LOWER('%{}%'))",
                                keyword, keyword
                            )
                        })
                        .collect();
                where_conditions.push(format!("({})", search_conditions.join(" OR ")));
            }

            // Exclude keywords filter
            if !filters.exclude_keywords.is_empty() {
                let exclude_conditions: Vec<String> = filters.exclude_keywords
                        .iter()
                        .map(|keyword| {
                            format!(
                                "(LOWER(t.name) NOT LIKE LOWER('%{}%') AND LOWER(t.symbol) NOT LIKE LOWER('%{}%'))",
                                keyword, keyword
                            )
                        })
                        .collect();
                where_conditions.push(format!("({})", exclude_conditions.join(" AND ")));
            }

            // Add WHERE clause if we have conditions
            // if !where_conditions.is_empty() {
            //     query.push_str(" WHERE ");
            //     query.push_str(&where_conditions.join(" AND "));
            // }

            // Close the CTE and add basic SELECT
            query.push_str(
                r#"
        
                      ORDER BY bonding_curve_percent DESC
                      LIMIT 10;
                      "#,
            );

            // let explain_query = format!("EXPLAIN (ANALYZE, BUFFERS) {}", query);
            // println!("{}", query);

            let pools = sqlx::query(&query).fetch_all(&db.pool).await.map_err(|e| {
                info!("DB query failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            let mut data = Vec::new();
            for pool in pools.into_iter() {
                let top10_holders_percent = calculate_percentage(
                    pool.get::<Decimal, _>("top10_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let dev_holds_percent = calculate_percentage(
                    pool.get::<Decimal, _>("dev_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let snipers_holds_percent = calculate_percentage(
                    pool.get::<Decimal, _>("snipers_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let market_cap_sol = calculate_market_cap(
                    pool.get::<Decimal, _>("current_price_sol"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let pulse_data: PulseDataResponse = PulseDataResponse {
                    pair_address: bs58::encode(pool.get::<Vec<u8>, _>("pool_address"))
                        .into_string(),
                    liquidity_sol: pool.get::<Decimal, _>("liquidity_sol"),
                    liquidity_token: pool.get("liquidity_token"),
                    token_address: bs58::encode(pool.get::<Vec<u8>, _>("mint_address"))
                        .into_string(),
                    bonding_curve_percent: pool.get::<Decimal, _>("bonding_curve_percent"),
                    token_name: pool.get("name"),
                    token_symbol: pool.get("symbol"),
                    token_decimals: pool.get::<i16, _>("decimals") as u8,
                    creator: bs58::encode(pool.get::<Vec<u8>, _>("creator")).into_string(),
                    protocol: pool.get("factory"),
                    website: pool.get("website"),
                    twitter: pool.get("twitter"),
                    telegram: pool.get("telegram"),
                    // discord: pool.get("discord"),
                    top10_holders_percent: top10_holders_percent,
                    dev_holds_percent: dev_holds_percent,
                    snipers_holds_percent: snipers_holds_percent,
                    volume_sol: pool.get::<Decimal, _>("volume_sol"),
                    market_cap_sol: market_cap_sol,
                    created_at: pool.get::<DateTime<Utc>, _>("created_at"),
                    migration_count: pool.get("migration_count"),
                    num_txns: pool.get("num_txns"),
                    num_buys: pool.get("num_buys"),
                    num_sells: pool.get("num_sells"),
                    num_holders: pool.get("num_holders"),
                    supply: pool.get("token_supply"),
                    token_image: pool.get("image"),
                    dev_wallet_funding: if let Some(funding_wallet) =
                        pool.get::<Option<Vec<u8>>, _>("funding_wallet_address")
                    {
                        Some(DevWalletFunding {
                            funding_wallet_address: bs58::encode(funding_wallet).into_string(),
                            wallet_address: bs58::encode(pool.get::<Vec<u8>, _>("wallet_address"))
                                .into_string(),
                            amount_sol: pool.get("amount_sol"),
                            hash: bs58::encode(pool.get::<Vec<u8>, _>("transfer_hash"))
                                .into_string(),
                            funded_at: pool.get::<DateTime<Utc>, _>("funded_at"),
                        })
                    } else {
                        None
                    },
                };
                data.push(pulse_data);
            }
            Ok(Json(json!({ "pools": data })))
        }
        PulseTable::FinalStretch => {
            let mut query = String::new();

            query.push_str(
                r#"
WITH all_pools AS (
  SELECT
    p.pool_address,
    p.creator,
    p.token_base_address,
    p.token_quote_address,
    p.pool_base_address,
    p.pool_quote_address,
    p.factory,
    p.created_at,
    p.initial_token_base_reserve,
    p.initial_token_quote_reserve,
    p.curve_percentage
  FROM pools p
 WHERE p.created_at >= now() - interval '24 hours'
   AND p.curve_percentage < 100
"#,
            );
            if let Some(min_age) = filters.age.min {
                // min_age is minutes from now, so we want pools created at least min_age minutes ago
                query.push_str(&format!(
                    " AND p.created_at <= now() - interval '{} minutes'",
                    min_age
                ));
            }
            if let Some(max_age) = filters.age.max {
                // max_age is minutes from now, so we want pools created at most max_age minutes ago
                query.push_str(&format!(
                    " AND p.created_at >= now() - interval '{} minutes'",
                    max_age
                ));
            }
            // Apply factory filters
            let mut factory_conditions = Vec::new();
            if filters.factories.pump_fun {
                factory_conditions.push("p.factory = 'PumpFun'");
            }
            if filters.factories.pump_swap {
                factory_conditions.push("p.factory = 'PumpSwap'");
            }

            if !factory_conditions.is_empty() {
                query.push_str(" AND (");
                query.push_str(&factory_conditions.join(" OR "));
                query.push_str(")");
            }
            query.push_str(r#"
          ),
          
tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply::numeric AS token_supply,
    power(10::numeric, t.decimals)::numeric AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    ls.latest_base_reserve,
    ls.latest_quote_reserve,
    ls.latest_price_sol
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      s.base_reserve  AS latest_base_reserve,
      s.quote_reserve AS latest_quote_reserve,
      s.price_sol     AS latest_price_sol
    FROM swaps s
    WHERE s.pool_address = r.pool_address
    ORDER BY s.created_at DESC
    LIMIT 1
  ) ls ON TRUE
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM all_pools r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  GROUP BY r.pool_address
),
top10_holders AS (
  SELECT pool_address, SUM(amount) AS top10_amount_raw
  FROM (
    SELECT r.pool_address, a.amount,
           ROW_NUMBER() OVER (PARTITION BY r.pool_address ORDER BY a.amount DESC) AS rn
    FROM all_pools r
    JOIN accounts a
      ON a.mint = r.token_base_address
     AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  ) x
  WHERE rn <= 10
  GROUP BY pool_address
),
dev_hold AS (
  SELECT
    r.pool_address,
    coalesce(max(a.amount), 0) AS dev_amount_raw
  FROM all_pools r
  LEFT JOIN accounts a
    ON a.mint  = r.token_base_address
   AND a.owner = r.creator
   AND a.owner <> r.pool_address
  GROUP BY r.pool_address
),
snipers_holds AS (
  SELECT s.pool_address,
         COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
  FROM swaps s
  JOIN all_pools r ON r.pool_address = s.pool_address
  WHERE s.swap_type = 'BUY'
    AND s.creator <> r.pool_address
    AND s.creator <> r.pool_base_address
    AND s.creator <> r.pool_quote_address
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    df.source,
    df.destination,
    df.amount,
    df.hash,
    df.created_at
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      ts.source,
      ts.destination,
      ts.amount,
      ts.hash,
      ts.created_at
    FROM transfer_sol ts
    WHERE ts.destination = r.creator
    ORDER BY ts.created_at ASC
    LIMIT 1
  ) df ON TRUE
),
migration AS (
  SELECT r.creator,
         count(*) FILTER (WHERE p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM all_pools r
  LEFT JOIN pools p2 ON p2.creator = r.creator
  GROUP BY r.creator
),

vol_24h AS (         -- choose one source
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         SUM(s.buy_count)::int8            AS num_buys,
         SUM(s.sell_count)::int8           AS num_sells,
         SUM(s.buy_count + s.sell_count)::int8 AS num_txns
  FROM swaps_5m s
  JOIN all_pools r USING (pool_address)
  WHERE  s.bucket_start <  now() - interval '5 minutes'
  GROUP BY s.pool_address
)
SELECT
  r.pool_address,
  r.creator,
  r.token_base_address,
  r.token_quote_address,
  r.factory,
  r.created_at,
  r.initial_token_base_reserve,
  r.initial_token_quote_reserve,
  coalesce(r.curve_percentage, 0) AS bonding_curve_percent,

  -- token meta
  t.name, t.symbol, t.image, t.decimals, t.website, t.twitter, t.telegram, t.mint_address,
  t.token_supply,
  t.scale_factor,

  -- liquidity/price (fallback to initial if no swaps yet)
  coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) AS liquidity_sol,
  coalesce(ls.latest_base_reserve,  r.initial_token_base_reserve)  AS liquidity_token,
  coalesce(ls.latest_price_sol, 0)                                 AS current_price_sol,

  -- holders
  coalesce(h.num_holders, 0)                                       AS num_holders,

  -- raw amounts for calculation in Rust
  coalesce(th.top10_amount_raw, 0)                                 AS top10_amount_raw,
  coalesce(d.dev_amount_raw, 0)                                    AS dev_amount_raw,
  coalesce(sh.snipers_amount_raw, 0)                               AS snipers_amount_raw,

  -- migrations
  coalesce(m.migration_count, 0)                                   AS migration_count,

  coalesce(v.volume_sol, 0) AS volume_sol,
coalesce(v.num_txns,   0) AS num_txns,
coalesce(v.num_buys,   0) AS num_buys,
coalesce(v.num_sells,  0) AS num_sells,

  -- dev wallet funding (first transfer)
  df.source       AS funding_wallet_address,
  df.destination  AS wallet_address,
  df.amount       AS amount_sol,
  df.hash         AS transfer_hash,
  df.created_at   AS funded_at

FROM all_pools r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator
          "#);
            // Apply remaining filters (excluding age since it's already applied above)
            let mut where_conditions = Vec::new();

            // Top 10 holders filter
            if let Some(min_top10) = filters.top10_holders.min {
                where_conditions.push(format!(
                        "((coalesce(th.top10_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                        min_top10
                    ));
            }
            if let Some(max_top10) = filters.top10_holders.max {
                where_conditions.push(format!(
                        "((coalesce(th.top10_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                        max_top10
                    ));
            }

            // Dev holding filter
            if let Some(min_dev) = filters.dev_holding.min {
                where_conditions.push(format!(
                        "((coalesce(d.dev_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                        min_dev
                    ));
            }
            if let Some(max_dev) = filters.dev_holding.max {
                where_conditions.push(format!(
                        "((coalesce(d.dev_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                        max_dev
                    ));
            }
            if let Some(min_snipers) = filters.snipers_holding.min {
                where_conditions.push(format!(
                    "((coalesce(sh.snipers_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                    min_snipers
                ));
            }
            if let Some(max_snipers) = filters.snipers_holding.max {
                where_conditions.push(format!(
                    "((coalesce(sh.snipers_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                    max_snipers
                ));
            }

            // Holders count filter
            if let Some(min_holders) = filters.holders.min {
                where_conditions.push(format!("coalesce(h.num_holders, 0) >= {}", min_holders));
            }
            if let Some(max_holders) = filters.holders.max {
                where_conditions.push(format!("coalesce(h.num_holders, 0) <= {}", max_holders));
            }

            // Bonding curve filter
            if let Some(min_bonding) = filters.bonding_curve.min {
                where_conditions.push(format!("r.curve_percentage >= {}", min_bonding));
            }
            if let Some(max_bonding) = filters.bonding_curve.max {
                where_conditions.push(format!("r.curve_percentage <= {}", max_bonding));
            }

            // Liquidity filter
            if let Some(min_liquidity) = filters.liquidity.min {
                where_conditions.push(format!(
                    "coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) >= {}",
                    min_liquidity
                ));
            }
            if let Some(max_liquidity) = filters.liquidity.max {
                where_conditions.push(format!(
                    "coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) <= {}",
                    max_liquidity
                ));
            }

            // Volume filter
            if let Some(min_volume) = filters.volume.min {
                where_conditions.push(format!("coalesce(v.volume_sol, 0) >= {}", min_volume));
            }
            if let Some(max_volume) = filters.volume.max {
                where_conditions.push(format!("coalesce(v.volume_sol, 0) <= {}", max_volume));
            }

            // Market cap filter
            if let Some(min_market_cap) = filters.market_cap.min {
                where_conditions.push(format!(
                    "(coalesce(ls.latest_price_sol, 0) * t.token_supply) >= {}",
                    min_market_cap
                ));
            }
            if let Some(max_market_cap) = filters.market_cap.max {
                where_conditions.push(format!(
                    "(coalesce(ls.latest_price_sol, 0) * t.token_supply) <= {}",
                    max_market_cap
                ));
            }

            // Transactions filter
            if let Some(min_txns) = filters.txns.min {
                where_conditions.push(format!("coalesce(v.num_txns, 0) >= {}", min_txns));
            }
            if let Some(max_txns) = filters.txns.max {
                where_conditions.push(format!("coalesce(v.num_txns, 0) <= {}", max_txns));
            }

            // Num buys filter
            if let Some(min_buys) = filters.num_buys.min {
                where_conditions.push(format!("coalesce(v.num_buys, 0) >= {}", min_buys));
            }
            if let Some(max_buys) = filters.num_buys.max {
                where_conditions.push(format!("coalesce(v.num_buys, 0) <= {}", max_buys));
            }

            // Num sells filter
            if let Some(min_sells) = filters.num_sells.min {
                where_conditions.push(format!("coalesce(v.num_sells, 0) >= {}", min_sells));
            }
            if let Some(max_sells) = filters.num_sells.max {
                where_conditions.push(format!("coalesce(v.num_sells, 0) <= {}", max_sells));
            }

            // Migration count filter
            if let Some(min_migrations) = filters.num_migrations.min {
                where_conditions.push(format!(
                    "coalesce(m.migration_count, 0) >= {}",
                    min_migrations
                ));
            }
            if let Some(max_migrations) = filters.num_migrations.max {
                where_conditions.push(format!(
                    "coalesce(m.migration_count, 0) <= {}",
                    max_migrations
                ));
            }

            // Social media filters
            if filters.twitter {
                where_conditions.push("t.twitter IS NOT NULL AND t.twitter != ''".to_string());
            }
            if filters.website {
                where_conditions.push("t.website IS NOT NULL AND t.website != ''".to_string());
            }
            if filters.telegram {
                where_conditions.push("t.telegram IS NOT NULL AND t.telegram != ''".to_string());
            }
            if filters.at_least_one_social {
                where_conditions.push("(t.twitter IS NOT NULL AND t.twitter != '') OR (t.website IS NOT NULL AND t.website != '') OR (t.telegram IS NOT NULL AND t.telegram != '')".to_string());
            }

            // Search keywords filter
            if !filters.search_keywords.is_empty() {
                let search_conditions: Vec<String> = filters.search_keywords
                        .iter()
                        .map(|keyword| {
                            format!(
                                "(LOWER(t.name) LIKE LOWER('%{}%') OR LOWER(t.symbol) LIKE LOWER('%{}%'))",
                                keyword, keyword
                            )
                        })
                        .collect();
                where_conditions.push(format!("({})", search_conditions.join(" OR ")));
            }

            // Exclude keywords filter
            if !filters.exclude_keywords.is_empty() {
                let exclude_conditions: Vec<String> = filters.exclude_keywords
                        .iter()
                        .map(|keyword| {
                            format!(
                                "(LOWER(t.name) NOT LIKE LOWER('%{}%') AND LOWER(t.symbol) NOT LIKE LOWER('%{}%'))",
                                keyword, keyword
                            )
                        })
                        .collect();
                where_conditions.push(format!("({})", exclude_conditions.join(" AND ")));
            }

            // Add WHERE clause if we have conditions
            if !where_conditions.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&where_conditions.join(" AND "));
            }

            // Close the CTE and add basic SELECT
            query.push_str(
                r#"
                      ORDER BY bonding_curve_percent DESC
                      LIMIT 10;
                      "#,
            );

            // let explain_query = format!("EXPLAIN (ANALYZE, BUFFERS) {}", query);

            let pools = sqlx::query(&query).fetch_all(&db.pool).await.map_err(|e| {
                info!("DB query failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            println!("{:?}", pools);

            let mut data = Vec::new();
            for pool in pools.into_iter() {
                let top10_holders_percent = calculate_percentage(
                    pool.get::<Decimal, _>("top10_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let dev_holds_percent = calculate_percentage(
                    pool.get::<Decimal, _>("dev_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let snipers_holds_percent = calculate_percentage(
                    pool.get::<Decimal, _>("snipers_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let market_cap_sol = calculate_market_cap(
                    pool.get::<Decimal, _>("current_price_sol"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let pulse_data: PulseDataResponse = PulseDataResponse {
                    pair_address: bs58::encode(pool.get::<Vec<u8>, _>("pool_address"))
                        .into_string(),
                    liquidity_sol: pool.get::<Decimal, _>("liquidity_sol"),
                    liquidity_token: pool.get("liquidity_token"),
                    token_address: bs58::encode(pool.get::<Vec<u8>, _>("mint_address"))
                        .into_string(),
                    bonding_curve_percent: pool.get::<Decimal, _>("bonding_curve_percent"),
                    token_name: pool.get("name"),
                    token_symbol: pool.get("symbol"),
                    token_decimals: pool.get::<i16, _>("decimals") as u8,
                    creator: bs58::encode(pool.get::<Vec<u8>, _>("creator")).into_string(),
                    protocol: pool.get("factory"),
                    website: pool.get("website"),
                    twitter: pool.get("twitter"),
                    telegram: pool.get("telegram"),
                    // discord: pool.get("discord"),
                    top10_holders_percent: top10_holders_percent,
                    dev_holds_percent: dev_holds_percent,
                    snipers_holds_percent: snipers_holds_percent,
                    volume_sol: pool.get::<Decimal, _>("volume_sol"),
                    market_cap_sol: market_cap_sol,
                    created_at: pool.get::<DateTime<Utc>, _>("created_at"),
                    migration_count: pool.get("migration_count"),
                    num_txns: pool.get("num_txns"),
                    num_buys: pool.get("num_buys"),
                    num_sells: pool.get("num_sells"),
                    num_holders: pool.get("num_holders"),
                    supply: pool.get("token_supply"),
                    token_image: pool.get("image"),
                    dev_wallet_funding: if let Some(funding_wallet) =
                        pool.get::<Option<Vec<u8>>, _>("funding_wallet_address")
                    {
                        Some(DevWalletFunding {
                            funding_wallet_address: bs58::encode(funding_wallet).into_string(),
                            wallet_address: bs58::encode(pool.get::<Vec<u8>, _>("wallet_address"))
                                .into_string(),
                            amount_sol: pool.get("amount_sol"),
                            hash: bs58::encode(pool.get::<Vec<u8>, _>("transfer_hash"))
                                .into_string(),
                            funded_at: pool.get::<DateTime<Utc>, _>("funded_at"),
                        })
                    } else {
                        None
                    },
                };
                data.push(pulse_data);
            }
            Ok(Json(json!({ "pools": data })))
        }
        PulseTable::Migrated => {
            let mut query = String::new();

            query.push_str(
                r#"
WITH all_pools AS (
  SELECT
    p.pool_address,
    p.creator,
    p.token_base_address,
    p.token_quote_address,
    p.pool_base_address,
    p.pool_quote_address,
    p.factory,
    p.created_at,
    p.initial_token_base_reserve,
    p.initial_token_quote_reserve,
    p.curve_percentage
  FROM pools p
  WHERE p.created_at >= now() - interval '24 hours'
    AND p.pre_factory <> ''
    AND p.factory <> ''"#,
            );
            if let Some(min_age) = filters.age.min {
                // min_age is minutes from now, so we want pools created at least min_age minutes ago
                query.push_str(&format!(
                    " AND p.created_at <= now() - interval '{} minutes'",
                    min_age
                ));
            }
            if let Some(max_age) = filters.age.max {
                // max_age is minutes from now, so we want pools created at most max_age minutes ago
                query.push_str(&format!(
                    " AND p.created_at >= now() - interval '{} minutes'",
                    max_age
                ));
            }
            // Apply factory filters
            let mut factory_conditions = Vec::new();
            if filters.factories.pump_fun {
                factory_conditions.push("p.factory = 'PumpFun'");
            }
            if filters.factories.pump_swap {
                factory_conditions.push("p.factory = 'PumpSwap'");
            }

            if !factory_conditions.is_empty() {
                query.push_str(" AND (");
                query.push_str(&factory_conditions.join(" OR "));
                query.push_str(")");
            }
            query.push_str(r#"
          ),
         tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply::numeric AS token_supply,
    power(10::numeric, t.decimals)::numeric AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    ls.latest_base_reserve,
    ls.latest_quote_reserve,
    ls.latest_price_sol
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      s.base_reserve  AS latest_base_reserve,
      s.quote_reserve AS latest_quote_reserve,
      s.price_sol     AS latest_price_sol
    FROM swaps s
    WHERE s.pool_address = r.pool_address
    ORDER BY s.created_at DESC
    LIMIT 1
  ) ls ON TRUE
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM all_pools r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  GROUP BY r.pool_address
),
top10_holders AS (
  SELECT pool_address, SUM(amount) AS top10_amount_raw
  FROM (
    SELECT r.pool_address, a.amount,
           ROW_NUMBER() OVER (PARTITION BY r.pool_address ORDER BY a.amount DESC) AS rn
    FROM all_pools r
    JOIN accounts a
      ON a.mint = r.token_base_address
     AND a.owner <> ALL (ARRAY[r.pool_address, r.pool_base_address, r.pool_quote_address])
  ) x
  WHERE rn <= 10
  GROUP BY pool_address
),
dev_hold AS (
  SELECT
    r.pool_address,
    coalesce(max(a.amount), 0) AS dev_amount_raw
  FROM all_pools r
  LEFT JOIN accounts a
    ON a.mint  = r.token_base_address
   AND a.owner = r.creator
   AND a.owner <> r.pool_address
  GROUP BY r.pool_address
),
snipers_holds AS (
  SELECT s.pool_address,
         COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
  FROM swaps s
  JOIN all_pools r ON r.pool_address = s.pool_address
  WHERE s.swap_type = 'BUY'
    AND s.creator <> r.pool_address
    AND s.creator <> r.pool_base_address
    AND s.creator <> r.pool_quote_address
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    df.source,
    df.destination,
    df.amount,
    df.hash,
    df.created_at
  FROM all_pools r
  LEFT JOIN LATERAL (
    SELECT
      ts.source,
      ts.destination,
      ts.amount,
      ts.hash,
      ts.created_at
    FROM transfer_sol ts
    WHERE ts.destination = r.creator
    ORDER BY ts.created_at ASC
    LIMIT 1
  ) df ON TRUE
),
migration AS (
  SELECT r.creator,
         count(*) FILTER (WHERE p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM all_pools r
  LEFT JOIN pools p2 ON p2.creator = r.creator
  GROUP BY r.creator
),

vol_24h AS (         -- choose one source
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         SUM(s.buy_count)::int8            AS num_buys,
         SUM(s.sell_count)::int8           AS num_sells,
         SUM(s.buy_count + s.sell_count)::int8 AS num_txns
  FROM swaps_5m s
  JOIN all_pools r USING (pool_address)
  WHERE  s.bucket_start <  now() - interval '5 minutes'
  GROUP BY s.pool_address
)
SELECT
  r.pool_address,
  r.creator,
  r.token_base_address,
  r.token_quote_address,
  r.factory,
  r.created_at,
  r.initial_token_base_reserve,
  r.initial_token_quote_reserve,
  coalesce(r.curve_percentage, 0) AS bonding_curve_percent,

  -- token meta
  t.name, t.symbol, t.image, t.decimals, t.website, t.twitter, t.telegram, t.mint_address,
  t.token_supply,
  t.scale_factor,

  -- liquidity/price (fallback to initial if no swaps yet)
  coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) AS liquidity_sol,
  coalesce(ls.latest_base_reserve,  r.initial_token_base_reserve)  AS liquidity_token,
  coalesce(ls.latest_price_sol, 0)                                 AS current_price_sol,

  -- holders
  coalesce(h.num_holders, 0)                                       AS num_holders,

  -- raw amounts for calculation in Rust
  coalesce(th.top10_amount_raw, 0)                                 AS top10_amount_raw,
  coalesce(d.dev_amount_raw, 0)                                    AS dev_amount_raw,
  coalesce(sh.snipers_amount_raw, 0)                               AS snipers_amount_raw,

  -- migrations
  coalesce(m.migration_count, 0)                                   AS migration_count,

  coalesce(v.volume_sol, 0) AS volume_sol,
coalesce(v.num_txns,   0) AS num_txns,
coalesce(v.num_buys,   0) AS num_buys,
coalesce(v.num_sells,  0) AS num_sells,

  -- dev wallet funding (first transfer)
  df.source       AS funding_wallet_address,
  df.destination  AS wallet_address,
  df.amount       AS amount_sol,
  df.hash         AS transfer_hash,
  df.created_at   AS funded_at

FROM all_pools r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator
          "#);
            // Apply remaining filters (excluding age since it's already applied above)
            let mut where_conditions = Vec::new();

            // Top 10 holders filter
            if let Some(min_top10) = filters.top10_holders.min {
                where_conditions.push(format!(
                        "((coalesce(th.top10_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                        min_top10
                    ));
            }
            if let Some(max_top10) = filters.top10_holders.max {
                where_conditions.push(format!(
                        "((coalesce(th.top10_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                        max_top10
                    ));
            }

            // Dev holding filter
            if let Some(min_dev) = filters.dev_holding.min {
                where_conditions.push(format!(
                        "((coalesce(d.dev_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                        min_dev
                    ));
            }
            if let Some(max_dev) = filters.dev_holding.max {
                where_conditions.push(format!(
                        "((coalesce(d.dev_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                        max_dev
                    ));
            }
            if let Some(min_snipers) = filters.snipers_holding.min {
                where_conditions.push(format!(
                    "((coalesce(sh.snipers_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) >= {}",
                    min_snipers
                ));
            }
            if let Some(max_snipers) = filters.snipers_holding.max {
                where_conditions.push(format!(
                    "((coalesce(sh.snipers_amount_raw,0) / nullif(t.scale_factor,0)) * 100.0) / nullif(t.token_supply,0) <= {}",
                    max_snipers
                ));
            }

            // Holders count filter
            if let Some(min_holders) = filters.holders.min {
                where_conditions.push(format!("coalesce(h.num_holders, 0) >= {}", min_holders));
            }
            if let Some(max_holders) = filters.holders.max {
                where_conditions.push(format!("coalesce(h.num_holders, 0) <= {}", max_holders));
            }

            // Liquidity filter
            if let Some(min_liquidity) = filters.liquidity.min {
                where_conditions.push(format!(
                    "coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) >= {}",
                    min_liquidity
                ));
            }
            if let Some(max_liquidity) = filters.liquidity.max {
                where_conditions.push(format!(
                    "coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) <= {}",
                    max_liquidity
                ));
            }

            // Volume filter
            if let Some(min_volume) = filters.volume.min {
                where_conditions.push(format!("coalesce(v.volume_sol, 0) >= {}", min_volume));
            }
            if let Some(max_volume) = filters.volume.max {
                where_conditions.push(format!("coalesce(v.volume_sol, 0) <= {}", max_volume));
            }

            // Market cap filter
            if let Some(min_market_cap) = filters.market_cap.min {
                where_conditions.push(format!(
                    "(coalesce(ls.latest_price_sol, 0) * t.token_supply) >= {}",
                    min_market_cap
                ));
            }
            if let Some(max_market_cap) = filters.market_cap.max {
                where_conditions.push(format!(
                    "(coalesce(ls.latest_price_sol, 0) * t.token_supply) <= {}",
                    max_market_cap
                ));
            }

            // Transactions filter
            if let Some(min_txns) = filters.txns.min {
                where_conditions.push(format!("coalesce(v.num_txns, 0) >= {}", min_txns));
            }
            if let Some(max_txns) = filters.txns.max {
                where_conditions.push(format!("coalesce(v.num_txns, 0) <= {}", max_txns));
            }

            // Num buys filter
            if let Some(min_buys) = filters.num_buys.min {
                where_conditions.push(format!("coalesce(v.num_buys, 0) >= {}", min_buys));
            }
            if let Some(max_buys) = filters.num_buys.max {
                where_conditions.push(format!("coalesce(v.num_buys, 0) <= {}", max_buys));
            }

            // Num sells filter
            if let Some(min_sells) = filters.num_sells.min {
                where_conditions.push(format!("coalesce(v.num_sells, 0) >= {}", min_sells));
            }
            if let Some(max_sells) = filters.num_sells.max {
                where_conditions.push(format!("coalesce(v.num_sells, 0) <= {}", max_sells));
            }

            // Migration count filter
            if let Some(min_migrations) = filters.num_migrations.min {
                where_conditions.push(format!(
                    "coalesce(m.migration_count, 0) >= {}",
                    min_migrations
                ));
            }
            if let Some(max_migrations) = filters.num_migrations.max {
                where_conditions.push(format!(
                    "coalesce(m.migration_count, 0) <= {}",
                    max_migrations
                ));
            }

            // Social media filters
            if filters.twitter {
                where_conditions.push("t.twitter IS NOT NULL AND t.twitter != ''".to_string());
            }
            if filters.website {
                where_conditions.push("t.website IS NOT NULL AND t.website != ''".to_string());
            }
            if filters.telegram {
                where_conditions.push("t.telegram IS NOT NULL AND t.telegram != ''".to_string());
            }
            if filters.at_least_one_social {
                where_conditions.push("(t.twitter IS NOT NULL AND t.twitter != '') OR (t.website IS NOT NULL AND t.website != '') OR (t.telegram IS NOT NULL AND t.telegram != '')".to_string());
            }

            // Search keywords filter
            if !filters.search_keywords.is_empty() {
                let search_conditions: Vec<String> = filters.search_keywords
                        .iter()
                        .map(|keyword| {
                            format!(
                                "(LOWER(t.name) LIKE LOWER('%{}%') OR LOWER(t.symbol) LIKE LOWER('%{}%'))",
                                keyword, keyword
                            )
                        })
                        .collect();
                where_conditions.push(format!("({})", search_conditions.join(" OR ")));
            }

            // Exclude keywords filter
            if !filters.exclude_keywords.is_empty() {
                let exclude_conditions: Vec<String> = filters.exclude_keywords
                        .iter()
                        .map(|keyword| {
                            format!(
                                "(LOWER(t.name) NOT LIKE LOWER('%{}%') AND LOWER(t.symbol) NOT LIKE LOWER('%{}%'))",
                                keyword, keyword
                            )
                        })
                        .collect();
                where_conditions.push(format!("({})", exclude_conditions.join(" AND ")));
            }

            // Add WHERE clause if we have conditions
            if !where_conditions.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&where_conditions.join(" AND "));
            }

            // Close the CTE and add basic SELECT
            query.push_str(
                r#"

                      ORDER BY created_at DESC
                      LIMIT 10;
                      "#,
            );

            // let explain_query = format!("EXPLAIN (ANALYZE, BUFFERS) {}", query);

            let pools = sqlx::query(&query).fetch_all(&db.pool).await.map_err(|e| {
                info!("DB query failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            println!("{:?}", pools);

            let mut data = Vec::new();
            for pool in pools.into_iter() {
                let top10_holders_percent = calculate_percentage(
                    pool.get::<Decimal, _>("top10_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let dev_holds_percent = calculate_percentage(
                    pool.get::<Decimal, _>("dev_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let snipers_holds_percent = calculate_percentage(
                    pool.get::<Decimal, _>("snipers_amount_raw"),
                    pool.get::<Decimal, _>("scale_factor"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let market_cap_sol = calculate_market_cap(
                    pool.get::<Decimal, _>("current_price_sol"),
                    pool.get::<Decimal, _>("token_supply"),
                );
                let pulse_data: PulseDataResponse = PulseDataResponse {
                    pair_address: bs58::encode(pool.get::<Vec<u8>, _>("pool_address"))
                        .into_string(),
                    liquidity_sol: pool.get::<Decimal, _>("liquidity_sol"),
                    liquidity_token: pool.get("liquidity_token"),
                    token_address: bs58::encode(pool.get::<Vec<u8>, _>("mint_address"))
                        .into_string(),
                    bonding_curve_percent: pool.get::<Decimal, _>("bonding_curve_percent"),
                    token_name: pool.get("name"),
                    token_symbol: pool.get("symbol"),
                    token_decimals: pool.get::<i16, _>("decimals") as u8,
                    creator: bs58::encode(pool.get::<Vec<u8>, _>("creator")).into_string(),
                    protocol: pool.get("factory"),
                    website: pool.get("website"),
                    twitter: pool.get("twitter"),
                    telegram: pool.get("telegram"),
                    // discord: pool.get("discord"),
                    top10_holders_percent: top10_holders_percent,
                    dev_holds_percent: dev_holds_percent,
                    snipers_holds_percent: snipers_holds_percent,
                    volume_sol: pool.get::<Decimal, _>("volume_sol"),
                    market_cap_sol: market_cap_sol,
                    created_at: pool.get::<DateTime<Utc>, _>("created_at"),
                    migration_count: pool.get("migration_count"),
                    num_txns: pool.get("num_txns"),
                    num_buys: pool.get("num_buys"),
                    num_sells: pool.get("num_sells"),
                    num_holders: pool.get("num_holders"),
                    supply: pool.get("token_supply"),
                    token_image: pool.get("image"),
                    dev_wallet_funding: if let Some(funding_wallet) =
                        pool.get::<Option<Vec<u8>>, _>("funding_wallet_address")
                    {
                        Some(DevWalletFunding {
                            funding_wallet_address: bs58::encode(funding_wallet).into_string(),
                            wallet_address: bs58::encode(pool.get::<Vec<u8>, _>("wallet_address"))
                                .into_string(),
                            amount_sol: pool.get("amount_sol"),
                            hash: bs58::encode(pool.get::<Vec<u8>, _>("transfer_hash"))
                                .into_string(),
                            funded_at: pool.get::<DateTime<Utc>, _>("funded_at"),
                        })
                    } else {
                        None
                    },
                };
                data.push(pulse_data);
            }
            Ok(Json(json!({ "pools": data })))
        }
    }
}
