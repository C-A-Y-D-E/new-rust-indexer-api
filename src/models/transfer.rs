use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow)]
pub struct TransferData {
    pub source: Pubkey,
    pub destination: Pubkey,
    pub authority: Pubkey,
    // pub amount: f64,
    pub amount: u64,
}

#[derive(Debug, FromRow)]
pub struct TransferSol {
    pub source: Pubkey,
    pub destination: Pubkey,
    pub amount: Decimal,
    pub hash: Signature,
}

pub struct DbTransferSol {
    pub source: String,
    pub destination: String,
    pub amount: Decimal,
    pub hash: String,
}

impl From<TransferSol> for DbTransferSol {
    fn from(transfer: TransferSol) -> Self {
        DbTransferSol {
            source: transfer.source.to_string(),
            destination: transfer.destination.to_string(),
            amount: Decimal::from(transfer.amount),
            hash: transfer.hash.to_string(),
        }
    }
}

impl TryFrom<DbTransferSol> for TransferSol {
    type Error = String;

    fn try_from(transfer: DbTransferSol) -> Result<Self, Self::Error> {
        Ok(Self {
            source: Pubkey::from_str(&transfer.source).map_err(|_| "parse source".to_string())?,
            destination: Pubkey::from_str(&transfer.destination)
                .map_err(|_| "parse destination".to_string())?,
            amount: transfer.amount,
            hash: Signature::from_str(&transfer.hash).map_err(|_| "parse hash".to_string())?,
        })
    }
}
