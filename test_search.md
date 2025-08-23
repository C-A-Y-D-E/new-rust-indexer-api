# Testing the Search API with priceSol in Pool Data

## Test Cases

### 1. Search by Pool Address (should return pool data with priceSol, base_reserve, quote_reserve)

```bash
curl "http://localhost:8080/pools?search=7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm"
```

Expected response should include:

- `pool_data`: Complete pool information with price_sol, base_reserve, quote_reserve
- `base_token_data`: Token information for the base token
- `quote_token_data`: Token information for the quote token (SOL/USDC)
- `pool_report`: Latest 24-hour trading metrics

### 2. Search by Token Mint Address

```bash
curl "http://localhost:8080/pools?search=So11111111111111111111111111111111111111112"
```

Expected response should include:

- `base_token_data`: SOL token information
- `quote_token_data`: null
- `pool_data`: null
- `pool_report`: null

### 3. Search by Token Name

```bash
curl "http://localhost:8080/pools?search=Solana"
```

Expected response should include:

- `base_token_data`: Token information for matching tokens
- `quote_token_data`: null
- `pool_data`: null
- `pool_report`: null

### 4. Search by Token Symbol

```bash
curl "http://localhost:8080/pools?search=SOL"
```

Expected response should include:

- `base_token_data`: Token information for SOL token
- `quote_token_data`: null
- `pool_data`: null
- `pool_report`: null

## Response Format for Pool Search

When searching by pool address, the response should look like:

```json
[
  {
    "base_token_data": {
      "mint_address": "TokenMintAddress...",
      "name": "Token Name",
      "symbol": "TOKEN",
      "decimals": 8,
      "uri": "https://...",
      "mint_authority": "AuthorityAddress...",
      "supply": 1000000000,
      "freeze_authority": "FreezeAuthorityAddress...",
      "slot": 123456789,
      "image": "https://...",
      "twitter": "https://...",
      "telegram": "https://...",
      "website": "https://...",
      "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    "quote_token_data": {
      "mint_address": "So11111111111111111111111111111111111111112",
      "name": "Solana",
      "symbol": "SOL",
      "decimals": 9,
      "uri": "https://...",
      "mint_authority": "AuthorityAddress...",
      "supply": 1000000000,
      "freeze_authority": "FreezeAuthorityAddress...",
      "slot": 123456789,
      "image": "https://...",
      "twitter": "https://...",
      "telegram": "https://...",
      "website": "https://...",
      "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    "pool_data": {
      "pool_address": "7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm",
      "factory": "raydium",
      "pre_factory": "raydium_amm",
      "reversed": false,
      "token_base_address": "TokenMintAddress...",
      "token_quote_address": "So11111111111111111111111111111111111111112",
      "pool_base_address": "PoolBaseAddress...",
      "pool_quote_address": "PoolQuoteAddress...",
      "curve_percentage": 0.5,
      "initial_token_base_reserve": 1000.0,
      "initial_token_quote_reserve": 100.0,
      "slot": 123456789,
      "creator": "CreatorAddress...",
      "hash": "TransactionHash...",
      "metadata": {},
      "price_sol": 0.001,
      "base_reserve": 1000.0,
      "quote_reserve": 100.0
    },
    "pool_report": {
      "pool_address": "7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm",
      "bucket_start": "2024-01-01T00:00:00Z",
      "buy_volume": 1000.0,
      "buy_count": 50,
      "buyer_count": 25,
      "sell_volume": 500.0,
      "sell_count": 30,
      "seller_count": 20,
      "trader_count": 40,
      "open_price": 0.001,
      "close_price": 0.0011,
      "price_change_percent": 10.0
    }
  }
]
```

## Key Features Tested

1. **priceSol in Pool Data**: The `pool_data.price_sol` field contains the price in SOL from the most recent swap transaction
2. **Base and Quote Reserves**: The `pool_data.base_reserve` and `pool_data.quote_reserve` fields contain the latest reserve amounts
3. **Comprehensive Data**: When searching by pool address, you get complete information including tokens, pool data with swap info, and reports
4. **Multiple Search Types**: Supports searching by mint address, pool address, token name, and token symbol
5. **Error Handling**: Proper error responses for invalid queries

## Notes

- The `price_sol` field represents the price of the base token in SOL from the last swap
- The `base_reserve` and `quote_reserve` fields represent the current reserves from the last swap
- When searching by pool address, you get the most comprehensive data including the latest swap information directly in the pool data
- The API automatically detects whether the search query is a Pubkey address or text
- All addresses are properly converted between base58 string format and byte arrays for database queries
