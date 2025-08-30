use rust_decimal::Decimal;

pub fn calculate_percentage(
    amount: Decimal,
    scale_factor: Decimal,
    token_supply: Decimal,
) -> Decimal {
    if scale_factor == Decimal::from(0) || token_supply == Decimal::from(0) {
        return Decimal::from(0);
    }

    let percentage = (amount / scale_factor) * Decimal::from(100) / token_supply;

    // Constrain precision to prevent capacity errors
    if percentage.scale() > 18 {
        percentage.round_dp(18)
    } else {
        percentage
    }
}

pub fn calculate_market_cap(price: Decimal, supply: Decimal) -> Decimal {
    let market_cap = price * supply;

    // Constrain precision
    if market_cap.scale() > 18 {
        market_cap.round_dp(18)
    } else {
        market_cap
    }
}
