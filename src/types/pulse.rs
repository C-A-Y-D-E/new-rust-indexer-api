use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

// {
//     "pairAddress": "9Ck8DLv2Us3M6R5CVXNt1yS4mYqKA5GnDRnhiWJgcSmu",
//     "tokenAddress": "5vKjABrQXiUnfxRj8mkUC1ASUooHzRtxKQEVdc8ppump",
//     "devAddress": "Dcd19vVNc8tzNJLyLQUhdR7do7KaqMijgfxxJdcDM4ei",
//     "tokenName": "puf the cloud",
//     "tokenTicker": "puf",
//     "tokenImage": "https://axiomtrading.sfo3.cdn.digitaloceanspaces.com/5vKjABrQXiUnfxRj8mkUC1ASUooHzRtxKQEVdc8ppump.webp",
//     "tokenDecimals": 6,
//     "protocol": "Pump V1",
//     "protocolDetails": {
//         "global": "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf",
//         "creator": "Dcd19vVNc8tzNJLyLQUhdR7do7KaqMijgfxxJdcDM4ei",
//         "feeRecipient": "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",
//         "eventAuthority": "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1",
//         "associatedBondingCurve": "9AZgLb8gtcA2enVdcSxEaXu46C1e6yp2bWkt24BXbNcg"
//     },
//     "website": "https://x.com/pufthecloud_/status/1957791826980597787",
//     "twitter": "https://x.com/pufthecloud_/status/1957791826980597787",
//     "telegram": null,
//     "discord": null,
//     "top10HoldersPercent": 14.8639343373755,
//     "devHoldsPercent": 0,
//     "snipersHoldPercent": 0,
//     "insidersHoldPercent": 0,
//     "bundlersHoldPercent": 0,
//     "volumeSol": 10.956018031000003,
//     "marketCapSol": 34.006774080367705,
//     "feesSol": 0.15740342850000005,
//     "liquiditySol": 33.085919329,
//     "liquidityToken": 972921431.794987,
//     "bondingCurvePercent": 12.62,
//     "supply": 1000000000,
//     "numTxns": 19,
//     "numBuys": 14,
//     "numSells": 4,
//     "numHolders": 8,
//     "numTradingBotUsers": 4,
//     "createdAt": "2025-08-19T13:09:46.260Z",
//     "extra": null,
//     "dexPaid": false,
//     "migrationCount": 1,
//     "twitterHandleHistory": [],
//     "openTrading": "2025-08-19T13:08:46.260Z",
//     "devWalletFunding": {
//         "fundedAt": "2025-07-30T18:03:42.000Z",
//         "amountSol": 1.950934779,
//         "signature": "65tKCPjug2KFr66omH9c9Q3ZWC84X3cLAT4Z3BUZwYCGgiCBoYeBfSZSGFwv5ToGLJAXz8Cdz4KqkKzF87sgAary",
//         "walletAddress": "Dcd19vVNc8tzNJLyLQUhdR7do7KaqMijgfxxJdcDM4ei",
//         "fundingWalletAddress": "55e6m1CKZxwxakf9H3K5U8ArBtRf9tYmyp9rJyuiNqhK"
//     },
//     "kolCount": 0
// }
#[derive(Debug, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct PulseDataResponse {
    pub pair_address: String,
    pub token_address: String,
    pub creator: String,
    pub token_name: Option<String>,
    pub token_symbol: Option<String>,
    pub token_image: Option<String>,
    pub token_decimals: u8,
    pub protocol: String,
    pub website: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    // pub discord: Option<String>,
    pub top10_holders_percent: f64,
    pub dev_holds_percent: f64,
    pub snipers_holds_percent: f64,
    // pub insiders_hold_percent: f64,
    // pub bundlers_hold_percent: f64,
    pub volume_sol: f64,
    pub market_cap_sol: f64,

    // pub fees_sol: f64,
    pub liquidity_sol: f64,
    pub liquidity_token: f64,
    pub bonding_curve_percent: f32,
    pub supply: f64,
    pub num_txns: i64,
    pub num_buys: i64,
    pub num_sells: i64,
    pub num_holders: i64,
    // pub num_trading_bot_users: i64,
    pub created_at: DateTime<Utc>,
    // pub extra: String,
    // pub dex_paid: bool,
    pub migration_count: i64,
    // pub twitter_handle_history: Vec<String>,
    // pub open_trading: String,
    pub dev_wallet_funding: Option<DevWalletFunding>,
    // pub kol_count: i64,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct DevWalletFunding {
    pub funding_wallet_address: String,
    pub wallet_address: String,
    pub amount_sol: f64,
    pub hash: String,
    pub funded_at: DateTime<Utc>,
}
