use rust_decimal::Decimal;
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow)]
pub struct TransferSol {
    pub source: Pubkey,
    pub destination: Pubkey,
    pub amount: Decimal,
    pub hash: Signature,
}

pub struct DbTransferSol {
    pub source: Vec<u8>,
    pub destination: Vec<u8>,
    pub amount: Decimal,
    pub hash: Vec<u8>,
}

impl From<TransferSol> for DbTransferSol {
    fn from(transfer: TransferSol) -> Self {
        DbTransferSol {
            source: transfer.source.to_bytes().to_vec(),
            destination: transfer.destination.to_bytes().to_vec(),
            amount: Decimal::from(transfer.amount),
            hash: transfer.hash.as_ref().to_vec(),
        }
    }
}

impl TryFrom<DbTransferSol> for TransferSol {
    type Error = String;

    fn try_from(transfer: DbTransferSol) -> Result<Self, Self::Error> {
        Ok(Self {
            source: Pubkey::try_from(transfer.source).map_err(|_| "parse source".to_string())?,
            destination: Pubkey::try_from(transfer.destination)
                .map_err(|_| "parse destination".to_string())?,
            amount: transfer.amount,
            hash: Signature::try_from(transfer.hash).map_err(|_| "parse hash".to_string())?,
        })
    }
}
