use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PulseFilter {
    pub filters: Filters,
    pub table: PulseTable,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PulseTable {
    NewPairs,
    FinalStretch,
    Migrated,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filters {
    pub factories: FactoryFilters,
    pub search_keywords: Vec<String>,
    pub exclude_keywords: Vec<String>,
    // pub dex_paid: bool,
    #[serde(
        default = "default_age_filter",
        deserialize_with = "validate_age_filter"
    )]
    pub age: RangeFilter<i64>,
    #[serde(
        default = "default_hundred_filter",
        deserialize_with = "validate_hundred_filter"
    )]
    pub top10_holders: RangeFilter<Decimal>,
    #[serde(
        default = "default_hundred_filter",
        deserialize_with = "validate_hundred_filter"
    )]
    pub dev_holding: RangeFilter<Decimal>,
    #[serde(
        default = "default_hundred_filter",
        deserialize_with = "validate_hundred_filter"
    )]
    pub snipers_holding: RangeFilter<Decimal>,
    // pub insiders: RangeFilter<i64>,
    // pub bundle: RangeFilter<i64>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub holders: RangeFilter<i64>,
    // pub bot_users: RangeFilter<i64>,
    #[serde(
        default = "default_hundred_filter",
        deserialize_with = "validate_hundred_filter"
    )]
    pub bonding_curve: RangeFilter<Decimal>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub liquidity: RangeFilter<Decimal>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub volume: RangeFilter<Decimal>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub market_cap: RangeFilter<Decimal>,
    // pub fees: RangeFilter<Decimal>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub txns: RangeFilter<i64>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub num_buys: RangeFilter<i64>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub num_sells: RangeFilter<i64>,
    #[serde(
        default = "default_max_amount_filter",
        deserialize_with = "validate_max_amount_filter"
    )]
    pub num_migrations: RangeFilter<i64>,
    pub twitter: bool,
    pub website: bool,
    pub telegram: bool,
    pub at_least_one_social: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FactoryFilters {
    pub pump_fun: bool,
    pub pump_swap: bool,
    // pub launch_lab: bool,
    // pub meteora_dbc: bool,
    // pub launch_a_coin: bool,
    // pub bonk: bool,
    // pub boop: bool,
    // pub meteora_amm: bool,
    // pub meteora_amm_v2: bool,
    // pub moonshot: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeFilter<T> {
    pub min: Option<T>,
    pub max: Option<T>,
}

impl<T> RangeFilter<T> {
    pub fn is_empty(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }
}

fn default_range_filter<T>(min: T, max: T) -> RangeFilter<T> {
    RangeFilter {
        min: Some(min),
        max: Some(max),
    }
}

// For usage compatibility, you can define type-specific wrappers if needed:
fn default_age_filter() -> RangeFilter<i64> {
    default_range_filter(0, 1440)
}
fn default_hundred_filter() -> RangeFilter<Decimal> {
    default_range_filter(Decimal::from(0), Decimal::from(100))
}
fn default_max_amount_filter<'de, T>() -> RangeFilter<T>
where
    T: serde::de::Deserialize<'de> + PartialOrd + Clone + From<i64>,
{
    default_range_filter(T::from(0), T::from(1000000000))
}

fn validate_range_filter<'de, D, T>(
    deserializer: D,
    max_limit: T,
    err_msg: &str,
) -> Result<RangeFilter<T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::Deserialize<'de> + PartialOrd + Clone,
{
    let mut filter = RangeFilter::<T>::deserialize(deserializer)?;

    // Apply default if max is missing
    if filter.max.is_none() {
        filter.max = Some(max_limit.clone());
    }

    // Validate that max doesn't exceed limit
    if let Some(ref max) = filter.max {
        if *max > max_limit {
            return Err(serde::de::Error::custom(err_msg));
        }
    }

    Ok(filter)
}

fn validate_age_filter<'de, D>(deserializer: D) -> Result<RangeFilter<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    validate_range_filter(deserializer, 1440, "Age max cannot exceed 1440")
}

fn validate_hundred_filter<'de, D>(deserializer: D) -> Result<RangeFilter<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    validate_range_filter(
        deserializer,
        Decimal::from(100),
        "Bonding curve max cannot exceed 100",
    )
}

fn validate_max_amount_filter<'de, D, T>(deserializer: D) -> Result<RangeFilter<T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::Deserialize<'de> + PartialOrd + Clone + From<i64>,
{
    validate_range_filter(
        deserializer,
        T::from(1000000000),
        "Max amount cannot exceed 1000000000",
    )
}
