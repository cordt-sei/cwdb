#### Prerequisites

- Node.js (v14.x or later)
- SQLite3

#### Installation

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Dependencies**
   ```bash
   yarn install
   ```

#### Usage

1. **Initialize and Run the Indexer**

   Start the indexer to begin collecting data from the Sei blockchain:

   ```bash
   node index.js
   ```

   This script will:
   - Create necessary tables in the SQLite database (`smart_contracts.db`).
   - Fetch contract information from the Sei blockchain.
   - Store the contract and related information in the database.

2. **Run the Real-Time Indexer**

   To keep the database updated with real-time data, run:

   ```bash
   node realtime_index.js
   ```

   This script:
   - Connects to the Sei blockchain WebSocket.
   - Subscribes to relevant events (`store_code`, `instantiate`).
   - Updates the database with new data as events occur.

3. **Query the Database**

   To execute predefined queries and explore the collected data:

   - Run `query_db.js` for general queries:

     ```bash
     node query_db.js
     ```

   - Run `query_top5.js` to get the top 5 most deployed contracts:

     ```bash
     node query_top5.js
     ```

#### Configuration

- **Database**: The SQLite database (`smart_contracts.db`) is created in the project root directory.
- **WebSocket URL**: The WebSocket URL for Sei blockchain is set in `realtime_index.js` (`RPC_WEBSOCKET_URL`).
- **Logging**: Logs are stored in `real_time_indexer.log` and `data_collection.log`.

#### Troubleshooting

- **WebSocket Connection Issues**: Ensure a stable internet connection and verify the WebSocket URL.
- **Database Access Errors**: Check if the database file is correctly created and accessible. Ensure permissions are set correctly.
- **Missing Dependencies**: Run `yarn install` to ensure all required packages are installed.
