use fixnum::{
    FixedPoint,
    typenum::{U4, U8, U12, U18},
};

pub type Decimal32 = FixedPoint<i32, U4>; // Decimal(9, 4) = Decimal32(4)
pub type Decimal64 = FixedPoint<i64, U8>; // Decimal(18, 8) = Decimal64(8)
pub type Decimal128 = FixedPoint<i128, U12>; // Decimal(38, 12) = Decimal128(12)
pub type Decimal18 = FixedPoint<i128, U18>; // Decimal(38, 18) = Decimal18(18)

const SCALE_18: i128 = 1_000_000_000_000_000_000;

fn mul_dec18(a: Decimal18, b: Decimal18) -> Decimal18 {
    let p = (*a.as_bits()) * (*b.as_bits());
    Decimal18::from_bits(p / SCALE_18)
}

fn div_dec18(numer: Decimal18, denom: Decimal18) -> Decimal18 {
    if denom == Decimal18::from_bits(0) {
        return Decimal18::from_bits(0);
    }
    let s = (*numer.as_bits()) * SCALE_18 / (*denom.as_bits());
    Decimal18::from_bits(s)
}
pub fn calculate_percentage(amount: Decimal18, scale_factor: Decimal18, token_supply: Decimal18) -> Decimal18 {
    if scale_factor == Decimal18::from_bits(0) || token_supply == Decimal18::from_bits(0) {
        return Decimal18::from_bits(0);
    }

    // percentage = (amount * scale_factor) / token_supply
    let numerator = mul_dec18(amount, scale_factor);
    div_dec18(numerator, token_supply)
}

pub fn calculate_market_cap(price_sol: Decimal18, token_supply: Decimal18) -> Decimal18 {
    mul_dec18(price_sol, token_supply)
}

// f64 variant for places that still operate on floating-point values
pub fn calculate_percentage_f64(amount: f64, scale_factor: f64, token_supply: f64) -> f64 {
    if scale_factor == 0.0 || token_supply == 0.0 {
        return 0.0;
    }
    (amount * scale_factor) / token_supply
}
