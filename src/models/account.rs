use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use spl_token::{solana_program::pubkey::Pubkey, state::AccountState};
use sqlx::prelude::FromRow;

#[derive(Debug, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "account_state")]
pub enum DBAccountState {
    Uninitialized,
    Initialized,
    Frozen,
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

#[derive(FromRow, Serialize, Deserialize)]
pub struct DBTokenAccount {
    pub mint: Vec<u8>,
    pub owner: Vec<u8>,
    pub amount: Decimal,
    pub delegate: Option<Vec<u8>>,
    pub state: DBAccountState,
    pub is_native: Option<Decimal>,
    pub delegated_amount: Decimal,
    pub close_authority: Option<Vec<u8>>,
    pub account: Vec<u8>,
    pub program: Vec<u8>,
}

#[derive(FromRow, Serialize, Deserialize)]

pub struct HolderResponse {
    pub address: String,
    pub account: String,
    pub amount: Decimal,
    pub mint: String,
    pub state: DBAccountState,
    pub delegated_amount: Decimal,
}

impl From<Account> for DBTokenAccount {
    fn from(account: Account) -> Self {
        DBTokenAccount {
            mint: account.mint.to_bytes().to_vec(),
            owner: account.owner.to_bytes().to_vec(),
            account: account.account.to_bytes().to_vec(),
            amount: Decimal::from(account.amount),
            delegate: account.delegate.map(|d| d.to_bytes().to_vec()).into(),
            state: account.state,
            is_native: account.is_native.map(Decimal::from).into(),
            delegated_amount: Decimal::from(account.delegated_amount),
            close_authority: account
                .close_authority
                .map(|ca| ca.to_bytes().to_vec())
                .into(),
            program: account.program.to_bytes().to_vec(),
        }
    }
}

impl TryFrom<DBTokenAccount> for Account {
    type Error = String;

    fn try_from(db_token: DBTokenAccount) -> Result<Self, Self::Error> {
        Ok(Self {
            mint: Pubkey::try_from(db_token.mint).map_err(|_| "parse mint".to_string())?,
            owner: Pubkey::try_from(db_token.owner).map_err(|_| "parse owner".to_string())?,
            amount: db_token.amount.to_u64().ok_or("amount to u64")?,
            delegate: db_token
                .delegate
                .map(|d| {
                    Pubkey::try_from(d)
                        .map_err(|_| "parse delegate".to_string())
                        .expect("Failed to parse delegate")
                })
                .into(),
            state: db_token.state,
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
                    Pubkey::try_from(ca)
                        .map_err(|_| "parse close authority".to_string())
                        .expect("Failed to parse close authority")
                })
                .into(),
            account: Pubkey::try_from(db_token.account)
                .map_err(|_| "parse account".to_string())
                .expect("Failed to parse account"),
            program: Pubkey::try_from(db_token.program)
                .map_err(|_| "parse program".to_string())
                .expect("Failed to parse program"),
        })
    }
}

impl TryFrom<DBTokenAccount> for HolderResponse {
    type Error = String;

    fn try_from(db_token: DBTokenAccount) -> Result<Self, Self::Error> {
        Ok(Self {
            address: bs58::encode(&db_token.owner).into_string(),
            account: bs58::encode(&db_token.account).into_string(),
            amount: db_token.amount,
            mint: bs58::encode(&db_token.mint).into_string(),
            state: db_token.state,
            delegated_amount: db_token.delegated_amount,
        })
    }
}
