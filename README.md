# CosmWasm NFT Indexer

Simple indexer that queries cw contracts, specifically focusing on CW721, CW404, and other NFT contracts. Gathers contract information, token details, ownership, and related data to store in a local SQLite database.

## Features

- Fetches code IDs and associated contract addresses.
- Identifies contract types for filtering and categorization.
- Fetches tokens from NFT contracts (CW721, CW404, etc.) with paginated querying.
- Tracks progress and resumes indexing from the last checkpoint if interrupted.
- Supports batch insertion for database efficiency.
- Provides optional real-time updates using WebSocket connections.

## Prerequisites

- Node.js (version >= 14.x)
- SQLite3
- Configuration of CosmWasm endpoints.

## Configuration

The indexer can be configured using `config.js`:

```javascript
export const config = {
  blockHeight: 94496767,
  paginationLimit: 100,
  numWorkers: 4,
  restAddress: "https://your-endpoint.com",
  wsAddress: "ws://localhost:26657/websocket",
  evmRpcAddress: "http://your-evm-rpc.com",
  pointerApi: "https://pointer.api",
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

1. Install dependencies:

   ```sh
   npm install
   ```

2. Start the indexer:

   ```sh
   npm start
   ```

3. Optional: Enable real-time monitoring:

   ```sh
   npm run realtime
   ```

## Progress Tracking

The indexer uses the `indexer_progress` table to track the last processed contract and token within a step. This allows the process to resume from where it left off after an interruption.

## Error Handling

- The indexer retries failed operations up to 3 times.
- Errors during batch processing are logged, and the indexing process continues.

## Contributing

Please submit issues and pull requests for any bug fixes or enhancements.
