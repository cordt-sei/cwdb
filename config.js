// config.js
module.exports = {
  RPC_WEBSOCKET_URL: 'wss://rpc.sei-apis.com/websocket',
  restAddress: 'http://tasty.seipex.fi:1317', 
  API_KEY: '',
  DB_PATH: './data/smart_contracts.db',
  LOG_FILE: './logs/real_time_indexer.log',
  BATCH_SIZE: 100,
  MAX_RETRIES: 3,
  RETRY_DELAY: 1000,
  NUM_WORKERS: 3,
  BLOCK_HEIGHT: 107000671, // Set to null for latest block, or specify a number for a specific block height
};