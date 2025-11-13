use chrono::{DateTime, Utc};
use clickhouse::Row;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use spl_token::{solana_program::pubkey::Pubkey, state::AccountState};
use sqlx::prelude::FromRow;

#[derive(Debug, sqlx::Type, Serialize, Deserialize, Clone)]
#[sqlx(type_name = "account_state")]
pub enum DBAccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

impl DBAccountState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Uninitialized => "Uninitialized",
            Self::Initialized => "Initialized",
            Self::Frozen => "Frozen",
        }
    }
}
impl FromStr for DBAccountState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Uninitialized" => Ok(Self::Uninitialized),
            "Initialized" => Ok(Self::Initialized),
            "Frozen" => Ok(Self::Frozen),
            _ => Err("Invalid state".to_string()),
        }
    }
}
impl From<AccountState> for DBAccountState {
    fn from(account_state: AccountState) -> Self {
        match account_state {
            AccountState::Uninitialized => Self::Uninitialized,
            AccountState::Initialized => Self::Initialized,
            AccountState::Frozen => Self::Frozen,
        }
    }
}
impl From<DBAccountState> for AccountState {
    fn from(db_account_state: DBAccountState) -> Self {
        match db_account_state {
            DBAccountState::Uninitialized => Self::Uninitialized,
            DBAccountState::Initialized => Self::Initialized,
            DBAccountState::Frozen => Self::Frozen,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub account: Pubkey,
    pub mint: Option<Pubkey>,
    pub owner: Option<Pubkey>,
    pub amount: u64,
    pub program: Pubkey,
}

#[derive(Clone, Row, Debug, Serialize, Deserialize)]
pub struct DBTokenAccount {
    pub account: String,
    pub mint: Option<String>,
    pub owner: Option<String>,
    pub amount: i64,
    pub program: Option<String>,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub created_at: DateTime<Utc>, // 14 - DateTime('UTC')
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub updated_at: DateTime<Utc>, // 15 - DateTime('UTC')
}

impl From<Account> for DBTokenAccount {
    fn from(account: Account) -> Self {
        let now = Utc::now();
        DBTokenAccount {
            account: account.account.to_string(),
            mint: account.mint.map(|m| m.to_string()),
            owner: account.owner.map(|o| o.to_string()),
            amount: account.amount as i64,
            program: Some(account.program.to_string()),
            created_at: now,
            updated_at: now,
        }
    }
}

impl TryFrom<DBTokenAccount> for Account {
    type Error = String;

    fn try_from(db_token: DBTokenAccount) -> Result<Self, Self::Error> {
        Ok(Self {
            account: Pubkey::from_str(&db_token.account)
                .map_err(|e| format!("Failed to parse account: {}", e))?,
            mint: Some(
                Pubkey::from_str(&db_token.mint.ok_or("mint is null")?)
                    .map_err(|e| format!("Failed to parse mint: {}", e))?,
            ),
            owner: Some(
                Pubkey::from_str(&db_token.owner.ok_or("owner is null")?)
                    .map_err(|e| format!("Failed to parse owner: {}", e))?,
            ),
            amount: db_token
                .amount
                .try_into()
                .map_err(|_| "amount is negative or too large".to_string())?,

            program: Pubkey::from_str(&db_token.program.ok_or("program is null")?)
                .map_err(|e| format!("Failed to parse program: {}", e))?,
        })
    }
}
