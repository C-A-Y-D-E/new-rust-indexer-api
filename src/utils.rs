use fixnum::{
    FixedPoint,
    typenum::{U4, U8, U12, U18},
};
use rust_decimal::Decimal;
use std::ops::Mul;
use std::str::FromStr; // Add this import

pub type Decimal32 = FixedPoint<i32, U4>; // Decimal(9, 4) = Decimal32(4)
pub type Decimal64 = FixedPoint<i64, U8>; // Decimal(18, 8) = Decimal64(8)
pub type Decimal128 = FixedPoint<i128, U12>; // Decimal(38, 12) = Decimal128(12)
pub type Decimal18 = FixedPoint<i128, U18>; // Decimal(38, 18) = Decimal18(18)

// impl Mul for Decimal18 {
//     type Output = Decimal18;

//     fn mul(self, rhs: Decimal18) -> Decimal18 {
//         let p = self.raw() * rhs.raw();
//         let s = p / 10i128.pow(18);
//         Decimal18::from_bits(s)
//     }
// }
pub fn calculate_market_cap(price_sol: f64, token_supply: f64) -> f64 {
    price_sol * token_supply
}

// f64 variant for places that still operate on floating-point values
pub fn calculate_percentage(amount: f64, token_supply: f64) -> f64 {
    if token_supply == 0.0 {
        return 0.0;
    }
    ((amount) / token_supply) * 100.0
}
