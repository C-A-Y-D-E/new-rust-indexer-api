use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct QuoteToken {
    pub address: &'static str,
    pub name: &'static str,
    pub symbol: &'static str,
    pub decimals: u8,
    pub logo: &'static str,
}

pub const SOL_TOKEN: QuoteToken = QuoteToken {
    address: "So11111111111111111111111111111111111111112",
    name: "Solana",
    symbol: "SOL",
    decimals: 9,
    logo: "https://assets.coingecko.com/coins/images/4128/large/solana.png?1640133422",
};

pub const USDC_TOKEN: QuoteToken = QuoteToken {
    address: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    name: "USDC",
    symbol: "USDC",
    decimals: 6,
    logo: "https://assets.coingecko.com/coins/images/6319/large/USD_Coin_icon.png?1547042194",
};
