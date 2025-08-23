use crate::models::swap::SwapType;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevHolding {
    pub dev_bought: bool,
    pub dev_sold: bool,
    pub sell_price_sol: Option<Decimal>,
    pub dev_bought_amount: Decimal,
    pub dev_holding_amount: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sniper {
    pub pool_address: String,
    pub first_swap_hash: String,
    pub first_swap_slot: u64,
    pub first_swap_type: SwapType,
    pub first_swap_base_amount: Decimal,
    pub first_swap_quote_amount: Decimal,
    pub total_supply_sniped: Decimal,
    pub total_wallet_sniped: i64,
    pub total_add_swaps: i64,
    pub total_add_volume: Decimal,
    pub first_swap_timestamp: chrono::DateTime<chrono::Utc>,
    pub last_add_swap_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub dev_holding: DevHolding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SniperSummary {
    pub pool_address: String,
    pub total_supply_sniped: Decimal,
    pub total_wallet_sniped: i64,
    pub total_add_swaps: i64,
    pub total_add_volume: Decimal,
    pub first_swap_timestamp: chrono::DateTime<chrono::Utc>,
    pub last_add_swap_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub dev_holding: DevHolding,
}
