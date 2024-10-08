# CWDB: CosmWasm Database Indexer

This project is a comprehensive indexer for CosmWasm smart contracts on the Sei blockchain. It collects and stores contract information, token data, and real-time updates in a SQLite database.

## Features

- Indexes CosmWasm smart contracts on the Sei blockchain
- Detects and categorizes contract types (CW721, CW20, CW1155)
- Queries and stores token information for CW721 contracts
- Real-time updates through WebSocket connection
- Efficient SQLite database storage
- Modular structure for easy maintenance and extension

## Prerequisites

- Node.js (v14.x or later)
- SQLite3

## Installation

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Dependencies**
   ```bash
   yarn install
   ```

## Project Structure

```
cwdb/
├── data/
│   └── smart_contracts.db
├── logs/
│   ├── data_collection.log
│   └── real_time_indexer.log
├── sample_queries/
│   ├── query_db.js
│   └── query_top5.js
├── cw721Helper.js
├── index.js
├── realtime_index.js
├── package.json
└── README.md
```

## Usage

1. **Initialize and Run the Indexer**

   Start the indexer to begin collecting data from the Sei blockchain:

   ```bash
   node index.js
   ```

   This script will:
   - Create necessary tables in the SQLite database (`data/smart_contracts.db`).
   - Fetch contract information from the Sei blockchain.
   - Store the contract and related information in the database.

2. **Run the Real-Time Indexer**

   To keep the database updated with real-time data, run:

   ```bash
   node realtime_index.js
   ```

   This script:
   - Connects to the Sei blockchain WebSocket.
   - Subscribes to relevant events (`instantiate`).
   - Updates the database with new data as events occur.
   - Determines contract types for newly instantiated contracts.
   - Queries and stores token information for CW721 contracts.

3. **Query the Database**

   To execute predefined queries and explore the collected data:

   - Run `sample_queries/query_db.js` for general queries:

     ```bash
     node sample_queries/query_db.js
     ```

   - Run `sample_queries/query_top5.js` to get the top 5 most deployed contracts:

     ```bash
     node sample_queries/query_top5.js
     ```

## Configuration

- **Database**: The SQLite database (`smart_contracts.db`) is located in the `data/` directory.
- **WebSocket URL**: The WebSocket URL for Sei blockchain is set in `realtime_index.js` (`RPC_WEBSOCKET_URL`).
- **Logging**: Logs are stored in the `logs/` directory:
  - `real_time_indexer.log`: Logs for the real-time indexer
  - `data_collection.log`: Logs for the initial data collection process

## Troubleshooting

- **WebSocket Connection Issues**: Ensure a stable internet connection and verify the WebSocket URL.
- **Database Access Errors**: Check if the database file is correctly created in the `data/` directory and is accessible. Ensure permissions are set correctly.
- **Missing Dependencies**: Run `yarn install` to ensure all required packages are installed.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the ISC License.