// config.js

export const config = {
  blockHeight: null,
  paginationLimit: 100,
  concurrencyLimit: 8,
  numWorkers: 6,
  restAddress: "https://api.sei.basementnodes.ca",
  wsAddress: "wss://evm-rpc-ws.sei.basementnodes.ca",
  evmRpcAddress: "https://evm-rpc.sei.basementnodes.ca",
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
