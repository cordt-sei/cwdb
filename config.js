// config.js

export const config = {
  blockHeight: process.env.BLOCK_HEIGHT || 94496767,
  paginationLimit: parseInt(process.env.PAGINATION_LIMIT, 10) || 100,
  numWorkers: parseInt(process.env.NUM_WORKERS, 10) || 4,
  restAddress: process.env.REST_ADDRESS || "https://snowy-solitary-patron.sei-pacific.quiknode.pro/b85f33628bfb46d8a184419284f47270a24b4488",
  wsAddress: process.env.WS_ADDRESS || "ws://tasty.seipex.fi:26657/websocket",
  evmRpcAddress: process.env.EVM_RPC_ADDRESS || "http://tasty.seipex.fi:8545",
  pointerApi: process.env.POINTER_API || "https://pointer.basementnodes.ca",
  timeout: parseInt(process.env.TIMEOUT, 10) || 5000,
  logLevel: process.env.LOG_LEVEL || 'DEBUG', // Logging level: ERROR, INFO, DEBUG
  logToFile: process.env.LOG_TO_FILE !== 'true', // Default to true unless explicitly set to false
  retryConfig: {
    retries: parseInt(process.env.RETRY_COUNT, 10) || 3,
    delay: parseInt(process.env.RETRY_DELAY, 10) || 1000,
    backoffFactor: parseFloat(process.env.BACKOFF_FACTOR) || 2
  }
};


