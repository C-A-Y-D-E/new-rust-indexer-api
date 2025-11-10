use std::error::Error;

use chrono::Utc;

use crate::routes::pulse::PulseRow;
use crate::services::clickhouse::ClickhouseService;
use crate::utils::{calculate_market_cap, calculate_percentage};
use crate::{
    models::pool::DBPool,
    types::pulse::{DevWalletFunding, PulseDataResponse},
};

pub async fn on_new_pool_event(
    db_pool: DBPool,
    db_service: &ClickhouseService,
<<<<<<< HEAD
) -> Result<(PulseDataResponse), Box<dyn Error + Send + Sync>> {
    if db_pool.factory != "PumpFun" && db_pool.factory != "PumpSwap" {
        return Err("factory is not PumpFun".to_string().into());
    }
=======
) -> Result<PulseDataResponse, Box<dyn Error + Send + Sync>> {
  if db_pool.factory != "PumpFun" && db_pool.factory != "PumpSwap" {
      return Err("factory is not PumpFun".to_string().into());
  }
>>>>>>> 264312fb44e9cfcbc0a1e7cd1bb7342be5ffbce8

  // ✅ Fixed query with proper type casting
  let query = "

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
  WHERE p.pool_address = ?
    AND p.created_at >= now() - INTERVAL 1 HOUR
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
    WHERE created_at >= now() - INTERVAL 1 HOUR
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
   AND a.updated_at >= now() - INTERVAL 1 HOUR
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
     AND a.updated_at >= now() - INTERVAL 1 HOUR
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
   AND a.updated_at >= now() - INTERVAL 1 HOUR
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
    AND s.created_at >= now() - INTERVAL 1 HOUR
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
    WHERE created_at >= now() - INTERVAL 1 HOUR
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
  SELECT 
    pool_address,
    SUM(CASE WHEN swap_type = 'BUY' THEN quote_amount ELSE 0 END) + 
    SUM(CASE WHEN swap_type = 'SELL' THEN quote_amount ELSE 0 END) AS volume_sol,
    CAST(COUNT(CASE WHEN swap_type = 'BUY' THEN 1 END) AS Int64) AS num_buys,
    CAST(COUNT(CASE WHEN swap_type = 'SELL' THEN 1 END) AS Int64) AS num_sells,
    CAST(COUNT(*) AS Int64) AS num_txns
  FROM swaps
  WHERE pool_address IN (SELECT pool_address FROM pools_with_curve)
    AND created_at >= now() - INTERVAL 1 HOUR
  GROUP BY pool_address
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
  coalesce(ls.latest_quote_reserve, r.initial_token_quote_reserve) AS liquidity_sol,
  coalesce(ls.latest_base_reserve, r.initial_token_base_reserve) AS liquidity_token,
  coalesce(ls.latest_price_sol, 0) AS current_price_sol,
  coalesce(h.num_holders, 0) AS num_holders,
  coalesce(th.top10_amount_raw, 0) AS top10_amount_raw,
  coalesce(d.dev_amount_raw, 0) AS dev_amount_raw,
  coalesce(sh.snipers_amount_raw, 0) AS snipers_amount_raw,
  coalesce(m.migration_count, 0) AS migration_count,
  coalesce(v.volume_sol, 0) AS volume_sol,
  coalesce(v.num_txns, 0) AS num_txns,
  coalesce(v.num_buys, 0) AS num_buys,
  coalesce(v.num_sells, 0) AS num_sells,
  nullIf(df.source, '') AS funding_wallet_address,
  nullIf(df.destination, '') AS wallet_address,
  if(df.source = '', NULL, df.amount) AS amount_sol,
  nullIf(df.hash, '') AS transfer_hash,
  if(df.source = '', NULL, df.created_at) AS funded_at
FROM pools_with_curve r
LEFT JOIN vol_24h v ON v.pool_address = r.pool_address
JOIN tok t ON t.mint_address = r.token_base_address
LEFT JOIN latest_swap ls ON ls.pool_address = r.pool_address
LEFT JOIN holders_base h ON h.pool_address = r.pool_address
LEFT JOIN top10_holders th ON th.pool_address = r.pool_address
LEFT JOIN dev_hold d ON d.pool_address = r.pool_address
LEFT JOIN snipers_holds sh ON sh.pool_address = r.pool_address
LEFT JOIN dev_wallet_funding df ON df.pool_address = r.pool_address
LEFT JOIN migration m ON m.creator = r.creator
  ";

   
    let pool = db_service
        .client
        .query(query)
        .bind(&db_pool.pool_address)
        .fetch_one::<PulseRow>()
        .await?;
    
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
    Ok(pulse_data)
}
