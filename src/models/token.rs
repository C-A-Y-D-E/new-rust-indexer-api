use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use solana_signature::Signature;
use spl_token::solana_program::pubkey::Pubkey;
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow, Clone, Serialize, Deserialize)]
pub struct Token {
    pub hash: Signature,
    pub mint_address: Pubkey,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub uri: Option<String>,
    pub mint_authority: Option<Pubkey>,
    pub supply: u64,
    pub freeze_authority: Option<Pubkey>,
    pub slot: u64,
    pub image: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub website: Option<String>,
    pub program_id: Pubkey,
}

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct DBToken {
    pub hash: String,
    pub mint_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i16, // SMALLINT
    pub uri: String,
    pub mint_authority: Option<String>,
    pub supply: Decimal,
    pub freeze_authority: Option<String>,
    pub slot: i64, // Changed from Decimal to i64 (BIGINT)
    pub image: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub website: Option<String>,
    pub program_id: String,
}



impl TryFrom<DBToken> for Token {
    type Error = String;

    fn try_from(db_token: DBToken) -> Result<Self, Self::Error> {
        Ok(Self {
            hash: Signature::from_str(&db_token.hash).map_err(|_| "parse hash".to_string())?,
            mint_address: Pubkey::from_str(&db_token.mint_address)
                .map_err(|_| "parse mint address".to_string())?,
            name: Some(db_token.name),
            symbol: Some(db_token.symbol),
            decimals: Some(db_token.decimals as u8),
            uri: Some(db_token.uri),
            mint_authority: db_token.mint_authority.and_then(|fa| {
                Pubkey::from_str(&fa)
                    .map_err(|_| "parse mint authority".to_string())
                    .ok()
            }),

            freeze_authority: db_token.freeze_authority.and_then(|fa| {
                Pubkey::from_str(&fa)
                    .map_err(|_| "parse freeze authority".to_string())
                    .ok()
            }),

            supply: db_token.supply.to_u64().ok_or("supply to u64")?,
            slot: db_token.slot as u64, // Convert i64 to u64
            image: db_token.image,
            twitter: db_token.twitter,
            telegram: db_token.telegram,
            website: db_token.website,
            program_id: Pubkey::from_str(&db_token.program_id)
                .map_err(|_| "parse program id".to_string())?,
        })
    }
}



#[derive(Debug, Serialize, Deserialize)]
pub struct TokenMetadata {
    pub mint_address: Pubkey,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub image: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub website: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DBTokenMetadata {
    pub mint_address: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub image: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub website: Option<String>,
}

impl From<TokenMetadata> for DBTokenMetadata {
    fn from(token_metadata: TokenMetadata) -> Self {
        Self {
            mint_address: token_metadata.mint_address.to_string(),
            name: token_metadata.name,
            symbol: token_metadata.symbol,
            uri: token_metadata.uri,
            image: token_metadata.image,
            twitter: token_metadata.twitter,
            telegram: token_metadata.telegram,
            website: token_metadata.website,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInitializeMint {
    pub mint_address: Pubkey,
    pub decimals: u8,
    pub mint_authority: Pubkey,
    pub freeze_authority: Option<Pubkey>,
    pub hash: Signature,
    pub slot: u64,
    pub program_id: Pubkey,
}

#[derive(Debug, Serialize, Deserialize)]

pub struct DBTokenInitializeMint {
    pub mint_address: String,
    pub decimals: Decimal,
    pub mint_authority: String,
    pub freeze_authority: Option<String>,
    pub hash: String,
    pub slot: Decimal,
    pub program_id: String,
}

impl From<TokenInitializeMint> for DBTokenInitializeMint {
    fn from(token_initialize_mint: TokenInitializeMint) -> Self {
        Self {
            mint_address: token_initialize_mint.mint_address.to_string(),
            decimals: Decimal::from(token_initialize_mint.decimals),
            mint_authority: token_initialize_mint.mint_authority.to_string(),
            freeze_authority: token_initialize_mint
                .freeze_authority
                .map(|fa| fa.to_string()),
            hash: token_initialize_mint.hash.to_string(),
            slot: Decimal::from(token_initialize_mint.slot),
            program_id: token_initialize_mint.program_id.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenSupplyUpdate {
    pub supply: u64,
    pub mint_address: Pubkey,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DBTokenSupplyUpdate {
    pub supply: Decimal,
    pub mint_address: String,
}

impl From<TokenSupplyUpdate> for DBTokenSupplyUpdate {
    fn from(token_supply_update: TokenSupplyUpdate) -> Self {
        Self {
            supply: Decimal::from(token_supply_update.supply),
            mint_address: token_supply_update.mint_address.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenMintAuthorityUpdate {
    pub mint_address: Pubkey,
    pub mint_authority: Option<Pubkey>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DBTokenMintAuthorityUpdate {
    pub mint_address: String,
    pub mint_authority: Option<String>,
}

impl From<TokenMintAuthorityUpdate> for DBTokenMintAuthorityUpdate {
    fn from(token_mint_authority_update: TokenMintAuthorityUpdate) -> Self {
        Self {
            mint_address: token_mint_authority_update.mint_address.to_string(),
            mint_authority: token_mint_authority_update
                .mint_authority
                .map(|fa| fa.to_string()),
        }
    }
}
