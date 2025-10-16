use std::str::FromStr;

use clickhouse::Row;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize, Deserialize)]
#[warn(dead_code)]
pub struct Account {
    pub account: Pubkey,
    pub mint: Pubkey,
    pub owner: Pubkey,
    pub amount: u64,
    pub delegate: Option<Pubkey>,
    pub state: DBAccountState,
    pub is_native: Option<u64>,
    pub delegated_amount: u64,
    pub close_authority: Option<Pubkey>,
    pub program: Pubkey,
}

#[derive(FromRow, Serialize, Deserialize, Clone, Row)]
pub struct DBTokenAccount {
    pub mint: String,
    pub owner: String,
    pub amount: i64,
    pub delegate: Option<String>,
    pub state: String,
    pub is_native: Option<i64>,
    pub delegated_amount: i64,
    pub close_authority: Option<String>,
    pub account: String,
    pub program: String,
}

impl From<Account> for DBTokenAccount {
    fn from(account: Account) -> Self {
        DBTokenAccount {
            mint: account.mint.to_string(),
            owner: account.owner.to_string(),
            account: account.account.to_string(),
            amount: account.amount as i64,
            delegate: account.delegate.map(|d| d.to_string()).into(),
            state: account.state.as_str().to_string(),
            is_native: account.is_native.map(|n| n as i64).into(),
            delegated_amount: account.delegated_amount as i64,
            close_authority: account.close_authority.map(|ca| ca.to_string()).into(),
            program: account.program.to_string(),
        }
    }
}

impl TryFrom<DBTokenAccount> for Account {
    type Error = String;

    fn try_from(db_token: DBTokenAccount) -> Result<Self, Self::Error> {
        Ok(Self {
            mint: Pubkey::from_str(&db_token.mint).map_err(|_| "parse mint".to_string())?,
            owner: Pubkey::from_str(&db_token.owner).map_err(|_| "parse owner".to_string())?,
            amount: db_token.amount.to_u64().ok_or("amount to u64")?,
            delegate: db_token
                .delegate
                .map(|d| {
                    Pubkey::from_str(&d)
                        .map_err(|_| "parse delegate".to_string())
                        .expect("Failed to parse delegate")
                })
                .into(),
            state: DBAccountState::from_str(&db_token.state)
                .map_err(|_| "parse state".to_string())?,
            is_native: db_token
                .is_native
                .map(|n| n.to_u64().expect("Failed to parse is native"))
                .into(),
            delegated_amount: db_token
                .delegated_amount
                .to_u64()
                .ok_or("delegated amount to u64")?,
            close_authority: db_token
                .close_authority
                .map(|ca| {
                    Pubkey::from_str(&ca)
                        .map_err(|_| "parse close authority".to_string())
                        .expect("Failed to parse close authority")
                })
                .into(),
            account: Pubkey::from_str(&db_token.account)
                .map_err(|_| "parse account".to_string())
                .expect("Failed to parse account"),
            program: Pubkey::from_str(&db_token.program)
                .map_err(|_| "parse program".to_string())
                .expect("Failed to parse program"),
        })
    }
}
