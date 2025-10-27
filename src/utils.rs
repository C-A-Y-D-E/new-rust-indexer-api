use fixnum::{
    FixedPoint,
    typenum::{U4, U8, U12, U18},
};

pub type Decimal32 = FixedPoint<i32, U4>; // Decimal(9, 4) = Decimal32(4)
pub type Decimal64 = FixedPoint<i64, U8>; // Decimal(18, 8) = Decimal64(8)
pub type Decimal128 = FixedPoint<i128, U12>; // Decimal(38, 12) = Decimal128(12)
pub type Decimal18 = FixedPoint<i128, U18>; // Decimal(38, 18) = Decimal18(18)

pub fn calculate_market_cap(price_sol: f64, token_supply: f64) -> f64 {
    price_sol * token_supply
}

// f64 variant for places that still operate on floating-point values
pub fn calculate_percentage(amount: f64, scale_factor: f64, token_supply: f64) -> f64 {
    if scale_factor == 0.0 || token_supply == 0.0 {
        return 0.0;
    }
    (amount * scale_factor) / token_supply
}
