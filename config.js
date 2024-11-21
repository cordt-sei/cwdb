// config.js

export const config = {
  blockHeight: null,
  paginationLimit: 100,
  concurrencyLimit: 5,
  numWorkers: 4,
  restAddress: "http://localhost:1317",
  wsAddress: "ws://localhost:26657/websocket",
  evmRpcAddress: "http://localhost:8545",
  pointerApi: "https://pointer.basementnodes.ca",
  timeout: 5000,
  logLevel: 'DEBUG',
  logToFile: true,
  retryConfig: {
    retries: 3,
    delay: 1000,
    backoffFactor: 2
  }
};
