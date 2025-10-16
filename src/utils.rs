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
pub fn calculate_percentage(
    amount: Decimal18,
    scale_factor: Decimal18,
    token_supply: Decimal18,
) -> Decimal18 {
    if scale_factor == Decimal18::from_bits(0) || token_supply == Decimal18::from_bits(0) {
        return Decimal18::from_bits(0);
    }

    let percentage = d_mul(
        d_div(amount, scale_factor),
        d_div(Decimal18::from_bits(100), token_supply),
    );

    percentage
}

pub fn calculate_market_cap(price: Decimal18, supply: Decimal18) -> Decimal18 {
    d_mul(price, supply)
}

const SCALE: i128 = 1_000_000_000_000_000_000; // 1e18
#[inline]
fn round_div_i128(n: i128, d: i128) -> i128 {
    if n >= 0 {
        n.saturating_add(d / 2) / d
    } else {
        n.saturating_sub(d / 2) / d
    } // half-up
}
#[inline]
pub fn d_add(a: Decimal18, b: Decimal18) -> Decimal18 {
    Decimal18::from_bits(a.into_bits().saturating_add(b.into_bits()))
}
#[inline]
pub fn d_sub(a: Decimal18, b: Decimal18) -> Decimal18 {
    Decimal18::from_bits(a.into_bits().saturating_sub(b.into_bits()))
}
#[inline]
pub fn d_mul(a: Decimal18, b: Decimal18) -> Decimal18 {
    let prod = a.into_bits().saturating_mul(b.into_bits()); // scale 36
    let rescaled = round_div_i128(prod, SCALE); // back to 18
    Decimal18::from_bits(rescaled)
}
#[inline]
pub fn d_div(a: Decimal18, b: Decimal18) -> Decimal18 {
    if b.into_bits() == 0 {
        return Decimal18::from_bits(0); // Avoid division by zero
    }
    let num = a.into_bits().saturating_mul(SCALE); // up-scale
    let q = round_div_i128(num, b.into_bits()); // divide
    Decimal18::from_bits(q)
}
