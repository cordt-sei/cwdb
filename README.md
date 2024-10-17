# CosmWasm NFT Indexer

Simple indexer that queries cw contracts, specifically focusing on CW721, CW404, and other NFT contracts. Gathers contract information, token details, ownership, and related data to store in a local SQLite database.

## Features

- Fetches code IDs and associated contract addresses.
- Identifies contract types for filtering and categorization.
- Fetches tokens from NFT contracts (CW721, CW404, etc.) with paginated querying.
- Tracks progress and resumes indexing from the last checkpoint if interrupted.
- Supports batch insertion for database efficiency.
- Provides optional real-time updates using WebSocket connections. [**WIP**]

## Prerequisites

- Node.js (version >= 14.x)
- SQLite3
- [Preferrably non-rate-limited] RPC endpoints.

## Configuration

The main configuration is in `config.js`:

```javascript
export const config = {
  blockHeight: 94496767,
  paginationLimit: 100,
  numWorkers: 4,
  restAddress: "http://loalhost:1317",
  wsAddress: "ws://localhost:26657/websocket",
  evmRpcAddress: "http://localhost:8545",
  pointerApi: "https://pointer.basementnode.ca", // *This is a custom endpoint
  timeout: 5000
};
```

## Database Schema

The following tables are used in the SQLite database:

- `indexer_progress`: Tracks progress for each indexing step.
- `code_ids`: Stores code ID metadata.
- `contracts`: Stores contract addresses and types.
- `contract_tokens`: Stores token data for each contract.
- `nft_owners`: Stores ownership details for each token.
- `pointer_data`: Tracks data related to pointer contracts.
- `wallet_associations`: Maps wallet addresses to their EVM counterparts.

## Running the Indexer

Operating is extremely simple. 
 - FComplete `config.js`
 - Install dependencies
 - Ru

   ```sh
   yarn install && yarn start
   ```

## Progress Tracking

The `indexer_progress` table tracks the last processed contract and token during each stage, allowing resuming after interruption without losing progress.

## Error Handling

- Failed operations are retried up to 3 times.
- Errors not warranting a retry (400 for example) will not be retried.
- Errors during batch processing are logged, and the indexing process continues.

## Contributing

Please submit issues a pull request for any bug fixes or enhancements.

##### * Run a local instance of the `pointer-api` using your own node and [this repo](https://github.com/cordt-sei/pointer-api).