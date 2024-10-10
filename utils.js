// utils.js
import axios from 'axios';
import WebSocket from 'ws';
import fs from 'fs';

// Logging function to write to both console and log file
export function log(message) {
  console.log(message);
  fs.appendFileSync('./logs/indexer.log', message + '\n');
}

// Retry logic for API calls with exponential backoff
export async function retryOperation(operation, maxRetries = 3) {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      return await operation();
    } catch (error) {
      attempt++;
      log(`Attempt ${attempt} failed. Retrying...`);
      if (attempt === maxRetries) throw error;
      await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt)));  // Exponential backoff
    }
  }
}

export async function fetchPaginatedData(url, contractAddress, payload, key, batchSize = 100) {
  let allData = [];
  let nextKey = null;

  do {
    const params = new URLSearchParams(payload);
    if (nextKey) {
      params.set('pagination.key', nextKey);
    }
    params.set('pagination.limit', batchSize.toString());

    const fullUrl = `${url}?${params.toString()}`;
    const response = await retryOperation(() => axios.get(fullUrl));

    if (response.status === 200 && response.data) {
      const dataBatch = response.data[key] || [];
      allData = allData.concat(dataBatch);

      nextKey = response.data.pagination?.next_key || null;
    } else {
      throw new Error(`Unexpected response: ${JSON.stringify(response.data)}`);
    }
  } while (nextKey);

  return allData;
}

// Helper function to send a smart contract query
export async function sendContractQuery(restAddress, contractAddress, payload, headers = {}) {
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${payloadBase64}`;

  try {
    const response = await retryOperation(() => axios.get(url, { headers }));
    return { data: response.data, status: response.status };
  } catch (error) {
    log(`Error querying contract ${contractAddress}: ${error.message}`);
    if (error.response) {
      log(`Status Code: ${error.response.status}, Data: ${JSON.stringify(error.response.data)}`);
    }
    return { error: error.response?.data || error.message, status: error.response?.status || 500 };
  }
}

// WebSocket setup function
export function setupWebSocket(url, messageHandler, log) {
  const ws = new WebSocket(url);

  ws.on('open', () => {
    log('Connected to WebSocket');
  });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      messageHandler(message);  // Delegate message handling to the passed handler
    } catch (error) {
      log(`Error processing WebSocket message: ${error.message}`);
    }
  });

  ws.on('close', () => {
    log('WebSocket connection closed. Reconnecting...');
    setTimeout(() => setupWebSocket(url, messageHandler, log), 5000);  // Reconnect after 5 seconds
  });

  ws.on('error', (error) => {
    log(`WebSocket error: ${error.message}`);
  });

  return ws;  // Return the WebSocket instance for external control if needed
}
