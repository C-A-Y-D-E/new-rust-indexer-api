# Index API

A Rust-based API for searching tokens and pools on Solana.

## Features

- Search tokens by mint address, name, or symbol
- Search pools by pool address
- Get comprehensive data including base token, quote token, pool data, and pool reports
- Get the latest swap information including priceSol, base_reserve, and quote_reserve directly in pool data
- Get all 4 pool report time intervals (5min, 1hr, 6hr, 24hr) when searching for pools
- Support for SOL and USDC as quote tokens

## API Endpoints

### Search

`GET /pools?search=<query>`

Search for tokens and pools by various criteria:

- **mint_address**: Search by token mint address (base58 encoded)
- **pool_address**: Search by pool address (base58 encoded)
- **name**: Search by token name (case-insensitive partial match)
- **symbol**: Search by token symbol (case-insensitive partial match)

#### Response Format

```json
[
  {
    "base_token_data": {
      "mint_address": "string",
      "name": "string",
      "symbol": "string",
      "decimals": 8,
      "uri": "string",
      "mint_authority": "string",
      "supply": 1000000,
      "freeze_authority": "string",
      "slot": 123456789,
      "image": "string",
      "twitter": "string",
      "telegram": "string",
      "website": "string",
      "program_id": "string"
    },
    "quote_token_data": {
      // Same structure as base_token_data
    },
    "pool_data": {
      "pool_address": "string",
      "factory": "string",
      "pre_factory": "string",
      "reversed": false,
      "token_base_address": "string",
      "token_quote_address": "string",
      "pool_base_address": "string",
      "pool_quote_address": "string",
      "curve_percentage": 0.5,
      "initial_token_base_reserve": 1000.0,
      "initial_token_quote_reserve": 100.0,
      "slot": 123456789,
      "creator": "string",
      "hash": "string",
      "metadata": {},
      "price_sol": 0.001,
      "base_reserve": 1000.0,
      "quote_reserve": 100.0
    },
    "pool_report": {
      "pool_address": "string",
      "bucket_start": "2024-01-01T00:00:00Z",
      "buy_volume": 1000.0,
      "buy_count": 50,
      "buyer_count": 25,
      "sell_volume": 500.0,
      "sell_count": 30,
      "seller_count": 20,
      "trader_count": 40,
      "open_price": 1.0,
      "close_price": 1.1,
      "price_change_percent": 10.0
    },
    "multi_pool_report": {
      "pool_address": "string",
      "report_5m": {
        "pool_address": "string",
        "bucket_start": "2024-01-01T00:00:00Z",
        "buy_volume": 100.0,
        "buy_count": 10,
        "buyer_count": 5,
        "sell_volume": 50.0,
        "sell_count": 8,
        "seller_count": 4,
        "trader_count": 8,
        "open_price": 1.0,
        "close_price": 1.05,
        "price_change_percent": 5.0
      },
      "report_1h": {
        "pool_address": "string",
        "bucket_start": "2024-01-01T00:00:00Z",
        "buy_volume": 500.0,
        "buy_count": 25,
        "buyer_count": 15,
        "sell_volume": 300.0,
        "sell_count": 20,
        "seller_count": 12,
        "trader_count": 25,
        "open_price": 1.0,
        "close_price": 1.08,
        "price_change_percent": 8.0
      },
      "report_6h": {
        "pool_address": "string",
        "bucket_start": "2024-01-01T00:00:00Z",
        "buy_volume": 800.0,
        "buy_count": 40,
        "buyer_count": 20,
        "sell_volume": 400.0,
        "sell_count": 25,
        "seller_count": 15,
        "trader_count": 35,
        "open_price": 1.0,
        "close_price": 1.09,
        "price_change_percent": 9.0
      },
      "report_24h": {
        "pool_address": "string",
        "bucket_start": "2024-01-01T00:00:00Z",
        "buy_volume": 1000.0,
        "buy_count": 50,
        "buyer_count": 25,
        "sell_volume": 500.0,
        "sell_count": 30,
        "seller_count": 20,
        "trader_count": 40,
        "open_price": 1.0,
        "close_price": 1.1,
        "price_change_percent": 10.0
      }
    }
  }
]
```

#### Example Usage

```bash
# Search by mint address
curl "http://localhost:8080/pools?search=So11111111111111111111111111111111111111112"

# Search by pool address
curl "http://localhost:8080/pools?search=7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm"

# Search by token name
curl "http://localhost:8080/pools?search=Solana"

# Search by token symbol
curl "http://localhost:8080/pools?search=SOL"
```

## Setup

1. Set up your environment variables:

   ```bash
   DATABASE_URL=postgresql://username:password@localhost:5432/database_name
   ```

2. Run the API:

   ```bash
   cargo run
   ```

The API will be available at `http://localhost:8080`.

## Database Schema

The API expects the following tables:

- `tokens`: Token information
- `pools`: Pool information
- `swaps`: Swap transactions with priceSol data
- `pool_reports_24h`: 24-hour aggregated pool reports
- `candles_1s`: 1-second candle data

## Dependencies

- Actix Web for HTTP server
- SQLx for database operations
- Serde for serialization
- Solana dependencies for Pubkey handling
