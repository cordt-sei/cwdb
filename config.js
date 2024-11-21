// config.js

export const config = {
  blockHeight: null,
  paginationLimit: 100,
  concurrencyLimit: 5,
  numWorkers: 4,
  restAddress: "http://3.26.171.139:1317",
  wsAddress: "ws://tasty.seipex.fi:26657/websocket",
  evmRpcAddress: "http://tasty.seipex.fi:8545",
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
