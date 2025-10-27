use axum::{Json, extract::State, http::StatusCode};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::Deserialize;
use serde_json::json;
use tracing::info;

use crate::{
    services::clickhouse::ClickhouseService,
    types::{
        filter::{PulseFilter, PulseTable},
        pulse::{DevWalletFunding, PulseDataResponse},
    },
    utils::{calculate_market_cap, calculate_percentage},
};

// ClickHouse result row structure
#[allow(dead_code)]
#[derive(Debug, Deserialize, Row)]
struct PulseRow {
    pool_address: String,
    creator: String,
    token_base_address: String,
    token_quote_address: String,
    factory: String,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    created_at: DateTime<Utc>,
    initial_token_base_reserve: f64,
    initial_token_quote_reserve: f64,
    bonding_curve_percent: f32,

    // Token metadata
    name: Option<String>,
    symbol: Option<String>,
    image: Option<String>,
    decimals: i8,
    website: Option<String>,
    twitter: Option<String>,
    telegram: Option<String>,
    mint_address: String,
    token_supply: f64,
    scale_factor: f64,

    // Liquidity/price
    liquidity_sol: f64,
    liquidity_token: f64,
    current_price_sol: f64,

    // Holders
    num_holders: u64,

    // Raw amounts for calculations
    top10_amount_raw: i64,
    dev_amount_raw: i64,
    snipers_amount_raw: f64,

    // Migrations
    migration_count: u64,

    // Volume metrics
    volume_sol: f64,
    num_txns: i64,
    num_buys: i64,
    num_sells: i64,

    // Dev wallet funding
    funding_wallet_address: Option<String>,
    wallet_address: Option<String>,
    amount_sol: Option<f64>,
    transfer_hash: Option<String>,
    #[serde(with = "clickhouse::serde::chrono::datetime::option")]
    funded_at: Option<DateTime<Utc>>,
}

pub async fn pulse(
    State(db): State<ClickhouseService>,
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
    p.initial_token_quote_reserve
  FROM pools p
 WHERE p.created_at >= now() - INTERVAL 24 HOUR
),
pool_curve AS (
  SELECT 
    pool_address,
    argMax(curve_percentage, updated_at) as curve_percentage
  FROM pool_curve_updates FINAL
  GROUP BY pool_address
),
pools_with_curve AS (
  SELECT r.*, coalesce(pcu.curve_percentage, 0) as curve_percentage
  FROM all_pools r
  LEFT JOIN pool_curve pcu ON pcu.pool_address = r.pool_address
  WHERE coalesce(pcu.curve_percentage, 0) < 50
),
r AS (
  SELECT * FROM pools_with_curve
  WHERE 1=1
"#,
            );
            if let Some(min_age) = filters.age.min {
                query.push_str(&format!(
                    " AND created_at <= now() - INTERVAL {} MINUTE",
                    min_age
                ));
            }
            if let Some(max_age) = filters.age.max {
                query.push_str(&format!(
                    " AND created_at >= now() - INTERVAL {} MINUTE",
                    max_age
                ));
            }
            // Apply factory filters
            let mut factory_conditions = Vec::new();
            if filters.factories.pump_fun {
                factory_conditions.push("factory = 'PumpFun'");
            }
            if filters.factories.pump_swap {
                factory_conditions.push("factory = 'PumpSwap'");
            }

            if !factory_conditions.is_empty() {
                query.push_str(" AND (");
                query.push_str(&factory_conditions.join(" OR "));
                query.push_str(")");
            }

            query.push_str(
                r#"
          ),

tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply AS token_supply,
    pow(10, t.decimals) AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    s.base_reserve AS latest_base_reserve,
    s.quote_reserve AS latest_quote_reserve,
    s.price_sol AS latest_price_sol
  FROM pools_with_curve r
  LEFT JOIN (
    SELECT
      pool_address,
      argMax(base_reserve, created_at) AS base_reserve,
      argMax(quote_reserve, created_at) AS quote_reserve,
      argMax(price_sol, created_at) AS price_sol
    FROM swaps
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY pool_address
  ) s ON s.pool_address = r.pool_address
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM pools_with_curve r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> r.pool_address
   AND a.owner <> r.pool_base_address
   AND a.owner <> r.pool_quote_address
   AND a.updated_at >= now() - INTERVAL 24 HOUR
  GROUP BY r.pool_address
),
top10_holders AS (
  SELECT pool_address, SUM(amount) AS top10_amount_raw
  FROM (
    SELECT r.pool_address, a.amount,
           ROW_NUMBER() OVER (PARTITION BY r.pool_address ORDER BY a.amount DESC) AS rn
    FROM pools_with_curve r
    JOIN accounts a
      ON a.mint = r.token_base_address
     AND a.owner <> r.pool_address
     AND a.owner <> r.pool_base_address
     AND a.owner <> r.pool_quote_address
     AND a.updated_at >= now() - INTERVAL 24 HOUR
  ) x
  WHERE rn <= 10
  GROUP BY pool_address
),
dev_hold AS (
  SELECT
    r.pool_address,
    coalesce(max(a.amount), 0) AS dev_amount_raw
  FROM pools_with_curve r
  LEFT JOIN accounts a
    ON a.mint  = r.token_base_address
   AND a.owner = r.creator
   AND a.owner <> r.pool_address

   AND a.updated_at >= now() - INTERVAL 24 HOUR
  GROUP BY r.pool_address
),
snipers_holds AS (
  SELECT s.pool_address,
         COALESCE(SUM(s.base_amount), 0) AS snipers_amount_raw
  FROM swaps s
  JOIN pools_with_curve r ON r.pool_address = s.pool_address
  WHERE s.swap_type = 'BUY'
    AND s.creator <> r.pool_address
    AND s.creator <> r.pool_base_address
    AND s.creator <> r.pool_quote_address
    AND s.created_at >= now() - INTERVAL 24 HOUR
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    ts.source,
    ts.destination,
    ts.amount,
    ts.hash,
    ts.earliest_transfer_at AS created_at
  FROM pools_with_curve r
  LEFT JOIN (
    SELECT
      destination,
      argMin(source, created_at) AS source,
      argMin(amount, created_at) AS amount,
      argMin(hash, created_at) AS hash,
      min(created_at) AS earliest_transfer_at
    FROM transfer_sol
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY destination
  ) ts ON ts.destination = r.creator
),
migration AS (
  SELECT r.creator,
         countIf(p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM pools_with_curve r
  LEFT JOIN pools p2 ON p2.creator = r.creator AND p2.created_at >= now() - INTERVAL 24 HOUR
  GROUP BY r.creator
),
vol_24h AS (
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         CAST(SUM(s.buy_count) AS Int64) AS num_buys,
         CAST(SUM(s.sell_count) AS Int64) AS num_sells,
         CAST(SUM(s.buy_count + s.sell_count) AS Int64) AS num_txns
  FROM pool_report_5m s
  JOIN pools_with_curve r ON r.pool_address = s.pool_address
  WHERE  s.bucket_start >= now() - INTERVAL 24 HOUR
    AND s.bucket_start < now() - INTERVAL 5 MINUTE
  GROUP BY s.pool_address
)
SELECT
  r.pool_address AS pool_address,
  r.creator AS creator,
  r.token_base_address AS token_base_address,
  r.token_quote_address AS token_quote_address,
  r.factory AS factory,
  r.created_at AS created_at,
  r.initial_token_base_reserve AS initial_token_base_reserve,
  r.initial_token_quote_reserve AS initial_token_quote_reserve,
  r.curve_percentage AS bonding_curve_percent,

  -- token meta
  t.name AS name,
  t.symbol AS symbol,
  t.image AS image,
  t.decimals AS decimals,
  t.website AS website,
  t.twitter AS twitter,
  t.telegram AS telegram,
  t.mint_address AS mint_address,
  t.token_supply AS token_supply,
  t.scale_factor AS scale_factor,

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
  nullIf(df.source, '')       AS funding_wallet_address,
  nullIf(df.destination, '')  AS wallet_address,
  if(df.source = '', NULL, df.amount) AS amount_sol,
  nullIf(df.hash, '')         AS transfer_hash,
  if(df.source = '', NULL, df.created_at) AS funded_at

FROM pools_with_curve r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator
          "#,
            );
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
                    "(coalesce(sh.snipers_amount_raw,0) * 100.0) / nullif(t.token_supply,0) >= {}",
                    min_snipers
                ));
            }
            if let Some(max_snipers) = filters.snipers_holding.max {
                where_conditions.push(format!(
                    "(coalesce(sh.snipers_amount_raw,0) * 100.0) / nullif(t.token_supply,0) <= {}",
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
                where_conditions.push(format!("bonding_curve_percent >= {}", min_bonding));
            }
            if let Some(max_bonding) = filters.bonding_curve.max {
                where_conditions.push(format!("bonding_curve_percent <= {}", max_bonding));
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
ORDER BY created_at DESC
LIMIT 10
"#,
            );

            // println!("{}", query);

            let pools: Vec<PulseRow> = db.client.query(&query).fetch_all().await.map_err(|e| {
                info!("DB query failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            println!("pools: {:?}", pools);

            let mut data = Vec::new();
            for pool in pools.into_iter() {
                let top10_decimal_adjusted = (pool.top10_amount_raw as f64) / pool.scale_factor;
                let top10_holders_percent =
                    calculate_percentage(top10_decimal_adjusted, pool.token_supply);
                let dev_decimal_adjusted = (pool.dev_amount_raw as f64) / pool.scale_factor;
                let dev_holds_percent =
                    calculate_percentage(dev_decimal_adjusted, pool.token_supply);
                let snipers_holds_percent =
                    calculate_percentage(pool.snipers_amount_raw, pool.token_supply);
                let market_cap_sol =
                    calculate_market_cap(pool.current_price_sol, pool.token_supply);
                let pulse_data: PulseDataResponse = PulseDataResponse {
                    pair_address: pool.pool_address,
                    liquidity_sol: pool.liquidity_sol,
                    liquidity_token: pool.liquidity_token,
                    token_address: pool.mint_address,
                    bonding_curve_percent: pool.bonding_curve_percent,
                    token_name: pool.name,
                    token_symbol: pool.symbol,
                    token_decimals: pool.decimals as u8,
                    creator: pool.creator,
                    protocol: pool.factory,
                    website: pool.website,
                    twitter: pool.twitter,
                    telegram: pool.telegram,
                    top10_holders_percent,
                    dev_holds_percent,
                    snipers_holds_percent,
                    volume_sol: pool.volume_sol,
                    market_cap_sol,
                    created_at: pool.created_at,
                    migration_count: pool.migration_count as i64,
                    num_txns: pool.num_txns,
                    num_buys: pool.num_buys,
                    num_sells: pool.num_sells,
                    num_holders: pool.num_holders as i64,
                    supply: pool.token_supply,
                    token_image: pool.image,
                    dev_wallet_funding: if let Some(funding_wallet) = pool.funding_wallet_address {
                        Some(DevWalletFunding {
                            funding_wallet_address: funding_wallet,
                            wallet_address: pool.wallet_address.unwrap_or_default(),
                            amount_sol: pool.amount_sol.unwrap_or_default(),
                            hash: pool.transfer_hash.unwrap_or_default(),
                            funded_at: pool.funded_at.unwrap_or(Utc::now()),
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
    p.initial_token_quote_reserve
  FROM pools p
 WHERE p.created_at >= now() - INTERVAL 24 HOUR
),
pool_curve AS (
  SELECT 
    pool_address,
    argMax(curve_percentage, updated_at) as curve_percentage
  FROM pool_curve_updates FINAL
  GROUP BY pool_address
),
pools_with_curve AS (
  SELECT r.*, coalesce(pcu.curve_percentage, 0) as curve_percentage
  FROM all_pools r
  LEFT JOIN pool_curve pcu ON pcu.pool_address = r.pool_address
  WHERE coalesce(pcu.curve_percentage, 0) < 100
),
r AS (
  SELECT * FROM pools_with_curve
  WHERE 1=1
"#,
            );
            if let Some(min_age) = filters.age.min {
                query.push_str(&format!(
                    " AND created_at <= now() - INTERVAL {} MINUTE",
                    min_age
                ));
            }
            if let Some(max_age) = filters.age.max {
                query.push_str(&format!(
                    " AND created_at >= now() - INTERVAL {} MINUTE",
                    max_age
                ));
            }
            // Apply factory filters
            let mut factory_conditions = Vec::new();
            if filters.factories.pump_fun {
                factory_conditions.push("factory = 'PumpFun'");
            }
            if filters.factories.pump_swap {
                factory_conditions.push("factory = 'PumpSwap'");
            }

            if !factory_conditions.is_empty() {
                query.push_str(" AND (");
                query.push_str(&factory_conditions.join(" OR "));
                query.push_str(")");
            }

            query.push_str(
                r#"
          ),

tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply AS token_supply,
    pow(10, t.decimals) AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    s.base_reserve AS latest_base_reserve,
    s.quote_reserve AS latest_quote_reserve,
    s.price_sol AS latest_price_sol
  FROM all_pools r
  LEFT JOIN (
    SELECT
      pool_address,
      argMax(base_reserve, created_at) AS base_reserve,
      argMax(quote_reserve, created_at) AS quote_reserve,
      argMax(price_sol, created_at) AS price_sol
    FROM swaps
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY pool_address
  ) s ON s.pool_address = r.pool_address
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM all_pools r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> r.pool_address
   AND a.owner <> r.pool_base_address
   AND a.owner <> r.pool_quote_address
   AND a.updated_at >= now() - INTERVAL 24 HOUR
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
     AND a.owner <> r.pool_address
     AND a.owner <> r.pool_base_address
     AND a.owner <> r.pool_quote_address
     AND a.updated_at >= now() - INTERVAL 24 HOUR
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
   AND a.updated_at >= now() - INTERVAL 24 HOUR
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
    AND s.created_at >= now() - INTERVAL 24 HOUR
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    ts.source,
    ts.destination,
    ts.amount,
    ts.hash,
    ts.earliest_transfer_at AS created_at
  FROM all_pools r
  LEFT JOIN (
    SELECT
      destination,
      argMin(source, created_at) AS source,
      argMin(amount, created_at) AS amount,
      argMin(hash, created_at) AS hash,
      min(created_at) AS earliest_transfer_at
    FROM transfer_sol
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY destination
  ) ts ON ts.destination = r.creator
),
migration AS (
  SELECT r.creator,
         countIf(p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM all_pools r
  LEFT JOIN pools p2 ON p2.creator = r.creator AND p2.created_at >= now() - INTERVAL 24 HOUR
  GROUP BY r.creator
),

vol_24h AS (
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         CAST(SUM(s.buy_count) AS Int64) AS num_buys,
         CAST(SUM(s.sell_count) AS Int64) AS num_sells,
         CAST(SUM(s.buy_count + s.sell_count) AS Int64) AS num_txns
  FROM pool_report_5m s
  JOIN all_pools r ON r.pool_address = s.pool_address
  WHERE  s.bucket_start < now() - INTERVAL 5 MINUTE
  GROUP BY s.pool_address
)
SELECT
  r.pool_address AS pool_address,
  r.creator AS creator,
  r.token_base_address AS token_base_address,
  r.token_quote_address AS token_quote_address,
  r.factory AS factory,
  r.created_at AS created_at,
  r.initial_token_base_reserve AS initial_token_base_reserve,
  r.initial_token_quote_reserve AS initial_token_quote_reserve,
  r.curve_percentage AS bonding_curve_percent,

  -- token meta
  t.name AS name,
  t.symbol AS symbol,
  t.image AS image,
  t.decimals AS decimals,
  t.website AS website,
  t.twitter AS twitter,
  t.telegram AS telegram,
  t.mint_address AS mint_address,
  t.token_supply AS token_supply,
  t.scale_factor AS scale_factor,

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
  nullIf(df.source, '')       AS funding_wallet_address,
  nullIf(df.destination, '')  AS wallet_address,
  if(df.source = '', NULL, df.amount) AS amount_sol,
  nullIf(df.hash, '')         AS transfer_hash,
  if(df.source = '', NULL, df.created_at) AS funded_at

FROM pools_with_curve r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator
          "#,
            );
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
                    "(coalesce(sh.snipers_amount_raw,0) * 100.0) / nullif(t.token_supply,0) >= {}",
                    min_snipers
                ));
            }
            if let Some(max_snipers) = filters.snipers_holding.max {
                where_conditions.push(format!(
                    "(coalesce(sh.snipers_amount_raw,0) * 100.0) / nullif(t.token_supply,0) <= {}",
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
                where_conditions.push(format!("bonding_curve_percent >= {}", min_bonding));
            }
            if let Some(max_bonding) = filters.bonding_curve.max {
                where_conditions.push(format!("bonding_curve_percent <= {}", max_bonding));
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
LIMIT 10
"#,
            );

            let pools: Vec<PulseRow> = db.client.query(&query).fetch_all().await.map_err(|e| {
                info!("DB query failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            let mut data = Vec::new();
            for pool in pools.into_iter() {
                let top10_decimal_adjusted = (pool.top10_amount_raw as f64) / pool.scale_factor;
                let top10_holders_percent =
                    calculate_percentage(top10_decimal_adjusted, pool.token_supply);
                let dev_decimal_adjusted = (pool.dev_amount_raw as f64) / pool.scale_factor;
                let dev_holds_percent =
                    calculate_percentage(dev_decimal_adjusted, pool.token_supply);
                let snipers_holds_percent =
                    calculate_percentage(pool.snipers_amount_raw, pool.token_supply);
                let market_cap_sol =
                    calculate_market_cap(pool.current_price_sol, pool.token_supply);
                let pulse_data: PulseDataResponse = PulseDataResponse {
                    pair_address: pool.pool_address,
                    liquidity_sol: pool.liquidity_sol,
                    liquidity_token: pool.liquidity_token,
                    token_address: pool.mint_address,
                    bonding_curve_percent: pool.bonding_curve_percent,
                    token_name: pool.name,
                    token_symbol: pool.symbol,
                    token_decimals: pool.decimals as u8,
                    creator: pool.creator,
                    protocol: pool.factory,
                    website: pool.website,
                    twitter: pool.twitter,
                    telegram: pool.telegram,
                    top10_holders_percent,
                    dev_holds_percent,
                    snipers_holds_percent,
                    volume_sol: pool.volume_sol,
                    market_cap_sol,
                    created_at: pool.created_at,
                    migration_count: pool.migration_count as i64,
                    num_txns: pool.num_txns,
                    num_buys: pool.num_buys,
                    num_sells: pool.num_sells,
                    num_holders: pool.num_holders as i64,
                    supply: pool.token_supply,
                    token_image: pool.image,
                    dev_wallet_funding: if let Some(funding_wallet) = pool.funding_wallet_address {
                        Some(DevWalletFunding {
                            funding_wallet_address: funding_wallet,
                            wallet_address: pool.wallet_address.unwrap_or_default(),
                            amount_sol: pool.amount_sol.unwrap_or_default(),
                            hash: pool.transfer_hash.unwrap_or_default(),
                            funded_at: pool.funded_at.unwrap_or(Utc::now()),
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
    p.pre_factory
  FROM pools p
  WHERE p.created_at >= now() - INTERVAL 24 HOUR
    AND isNotNull(p.pre_factory)
    AND p.pre_factory <> ''
    AND p.factory <> ''
),
pool_curve AS (
  SELECT 
    pool_address,
    argMax(curve_percentage, updated_at) as curve_percentage
  FROM pool_curve_updates FINAL
  GROUP BY pool_address
),
pools_with_curve AS (
  SELECT r.*, coalesce(pcu.curve_percentage, 0) as curve_percentage
  FROM all_pools r
  LEFT JOIN pool_curve pcu ON pcu.pool_address = r.pool_address
),
r AS (
  SELECT * FROM pools_with_curve
  WHERE 1=1
"#,
            );
            if let Some(min_age) = filters.age.min {
                query.push_str(&format!(
                    " AND created_at <= now() - INTERVAL {} MINUTE",
                    min_age
                ));
            }
            if let Some(max_age) = filters.age.max {
                query.push_str(&format!(
                    " AND created_at >= now() - INTERVAL {} MINUTE",
                    max_age
                ));
            }
            // Apply factory filters
            let mut factory_conditions = Vec::new();
            if filters.factories.pump_fun {
                factory_conditions.push("factory = 'PumpFun'");
            }
            if filters.factories.pump_swap {
                factory_conditions.push("factory = 'PumpSwap'");
            }

            if !factory_conditions.is_empty() {
                query.push_str(" AND (");
                query.push_str(&factory_conditions.join(" OR "));
                query.push_str(")");
            }

            query.push_str(
                r#"
          ),
         tok AS (
  SELECT
    t.mint_address,
    t.name, t.symbol, t.image, t.decimals,
    t.website, t.twitter, t.telegram,
    t.supply AS token_supply,
    pow(10, t.decimals) AS scale_factor
  FROM tokens t
),
latest_swap AS (
  SELECT
    r.pool_address,
    s.base_reserve AS latest_base_reserve,
    s.quote_reserve AS latest_quote_reserve,
    s.price_sol AS latest_price_sol
  FROM all_pools r
  LEFT JOIN (
    SELECT
      pool_address,
      argMax(base_reserve, created_at) AS base_reserve,
      argMax(quote_reserve, created_at) AS quote_reserve,
      argMax(price_sol, created_at) AS price_sol
    FROM swaps
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY pool_address
  ) s ON s.pool_address = r.pool_address
),
holders_base AS (
  SELECT
    r.pool_address,
    COUNT(DISTINCT a.owner) AS num_holders
  FROM all_pools r
  JOIN accounts a
    ON a.mint = r.token_base_address
   AND a.owner <> r.pool_address
   AND a.owner <> r.pool_base_address
   AND a.owner <> r.pool_quote_address
   AND a.updated_at >= now() - INTERVAL 24 HOUR
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
     AND a.owner <> r.pool_address
     AND a.owner <> r.pool_base_address
     AND a.owner <> r.pool_quote_address
     AND a.updated_at >= now() - INTERVAL 24 HOUR
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
   AND a.updated_at >= now() - INTERVAL 24 HOUR
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
    AND s.created_at >= now() - INTERVAL 24 HOUR
  GROUP BY s.pool_address
),
dev_wallet_funding AS (
  SELECT
    r.pool_address,
    ts.source,
    ts.destination,
    ts.amount,
    ts.hash,
    ts.earliest_transfer_at AS created_at
  FROM all_pools r
  LEFT JOIN (
    SELECT
      destination,
      argMin(source, created_at) AS source,
      argMin(amount, created_at) AS amount,
      argMin(hash, created_at) AS hash,
      min(created_at) AS earliest_transfer_at
    FROM transfer_sol
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY destination
  ) ts ON ts.destination = r.creator
),
migration AS (
  SELECT r.creator,
         countIf(p2.pre_factory = 'PumpFun' AND p2.factory = 'PumpSwap') AS migration_count
  FROM all_pools r
  LEFT JOIN pools p2 ON p2.creator = r.creator AND p2.created_at >= now() - INTERVAL 24 HOUR
  GROUP BY r.creator
),

vol_24h AS (
  SELECT s.pool_address,
         SUM(s.buy_volume + s.sell_volume) AS volume_sol,
         CAST(SUM(s.buy_count) AS Int64) AS num_buys,
         CAST(SUM(s.sell_count) AS Int64) AS num_sells,
         CAST(SUM(s.buy_count + s.sell_count) AS Int64) AS num_txns
  FROM pool_report_5m s
  JOIN all_pools r ON r.pool_address = s.pool_address
  WHERE  s.bucket_start < now() - INTERVAL 5 MINUTE
  GROUP BY s.pool_address
)
SELECT
  r.pool_address AS pool_address,
  r.creator AS creator,
  r.token_base_address AS token_base_address,
  r.token_quote_address AS token_quote_address,
  r.factory AS factory,
  r.created_at AS created_at,
  r.initial_token_base_reserve AS initial_token_base_reserve,
  r.initial_token_quote_reserve AS initial_token_quote_reserve,
  r.curve_percentage AS bonding_curve_percent,

  -- token meta
  t.name AS name,
  t.symbol AS symbol,
  t.image AS image,
  t.decimals AS decimals,
  t.website AS website,
  t.twitter AS twitter,
  t.telegram AS telegram,
  t.mint_address AS mint_address,
  t.token_supply AS token_supply,
  t.scale_factor AS scale_factor,

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
  nullIf(df.source, '')       AS funding_wallet_address,
  nullIf(df.destination, '')  AS wallet_address,
  if(df.source = '', NULL, df.amount) AS amount_sol,
  nullIf(df.hash, '')         AS transfer_hash,
  if(df.source = '', NULL, df.created_at) AS funded_at

FROM pools_with_curve r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
LEFT JOIN tok              t  ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap      ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base     h  ON h.pool_address  = r.pool_address
LEFT JOIN top10_holders    th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold         d  ON d.pool_address  = r.pool_address
LEFT JOIN snipers_holds    sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration        m  ON m.creator       = r.creator
          "#,
            );
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
                    "(coalesce(sh.snipers_amount_raw,0) * 100.0) / nullif(t.token_supply,0) >= {}",
                    min_snipers
                ));
            }
            if let Some(max_snipers) = filters.snipers_holding.max {
                where_conditions.push(format!(
                    "(coalesce(sh.snipers_amount_raw,0) * 100.0) / nullif(t.token_supply,0) <= {}",
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
LIMIT 10
"#,
            );

            let pools: Vec<PulseRow> = db.client.query(&query).fetch_all().await.map_err(|e| {
                info!("DB query failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            let mut data = Vec::new();
            for pool in pools.into_iter() {
                let top10_decimal_adjusted = (pool.top10_amount_raw as f64) / pool.scale_factor;
                let top10_holders_percent =
                    calculate_percentage(top10_decimal_adjusted, pool.token_supply);
                let dev_decimal_adjusted = (pool.dev_amount_raw as f64) / pool.scale_factor;
                let dev_holds_percent =
                    calculate_percentage(dev_decimal_adjusted, pool.token_supply);
                let snipers_holds_percent =
                    calculate_percentage(pool.snipers_amount_raw, pool.token_supply);
                let market_cap_sol =
                    calculate_market_cap(pool.current_price_sol, pool.token_supply);
                let pulse_data: PulseDataResponse = PulseDataResponse {
                    pair_address: pool.pool_address,
                    liquidity_sol: pool.liquidity_sol,
                    liquidity_token: pool.liquidity_token,
                    token_address: pool.mint_address,
                    bonding_curve_percent: pool.bonding_curve_percent,
                    token_name: pool.name,
                    token_symbol: pool.symbol,
                    token_decimals: pool.decimals as u8,
                    creator: pool.creator,
                    protocol: pool.factory,
                    website: pool.website,
                    twitter: pool.twitter,
                    telegram: pool.telegram,
                    top10_holders_percent,
                    dev_holds_percent,
                    snipers_holds_percent,
                    volume_sol: pool.volume_sol,
                    market_cap_sol,
                    created_at: pool.created_at,
                    migration_count: pool.migration_count as i64,
                    num_txns: pool.num_txns,
                    num_buys: pool.num_buys,
                    num_sells: pool.num_sells,
                    num_holders: pool.num_holders as i64,
                    supply: pool.token_supply,
                    token_image: pool.image,
                    dev_wallet_funding: if let Some(funding_wallet) = pool.funding_wallet_address {
                        Some(DevWalletFunding {
                            funding_wallet_address: funding_wallet,
                            wallet_address: pool.wallet_address.unwrap_or_default(),
                            amount_sol: pool.amount_sol.unwrap_or_default(),
                            hash: pool.transfer_hash.unwrap_or_default(),
                            funded_at: pool.funded_at.unwrap_or(Utc::now()),
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
