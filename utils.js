// utils.js
import axios from 'axios';
import WebSocket from 'ws';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';

// Logging function to write to both console and log file
export function log(message) {
  console.log(message);
  
  const logDir = './logs';
  const logFile = path.join(logDir, 'data_collection.log');

  // Ensure the logs directory exists
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }

  // Append to the log file
  fs.appendFileSync(logFile, message + '\n');
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
  let nextKey = payload['pagination.key'] || null;
  let firstIteration = true;

  do {
    const params = new URLSearchParams(payload);
    
    // Add the pagination.key only if nextKey exists (for subsequent queries)
    if (nextKey) {
      params.set('pagination.key', nextKey);
    }
    params.set('pagination.limit', batchSize.toString());

    const fullUrl = `${url}?${params.toString()}`;
    log(`Fetching data from: ${fullUrl}`); // Log the constructed URL
    const response = await retryOperation(() => axios.get(fullUrl));

    if (response.status === 200 && response.data) {
      log(`Full API response: ${JSON.stringify(response.data)}`); // Log the entire API response for debugging

      // Fetch the requested data using the key
      const dataBatch = response.data[key] || [];
      allData = allData.concat(dataBatch);
      log(`Fetched ${dataBatch.length} items in this batch.`);

      // Handle pagination only if it exists
      if (response.data.pagination && response.data.pagination.next_key) {
        log(`Pagination detected. Next key: ${response.data.pagination.next_key}`);
        nextKey = response.data.pagination.next_key;
      } else {
        log('No pagination or no next key detected. Exiting loop.');
        nextKey = null; // Ensure loop exits if no pagination exists
      }
    } else {
      throw new Error(`Unexpected response: ${JSON.stringify(response.data)}`);
    }

    firstIteration = false;
  } while (nextKey);

  log(`Total data fetched: ${allData.length}`); // Log the total fetched data
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

export async function checkProgress(db, step) {
  const dbGet = promisify(db.get).bind(db);
  const sql = `SELECT completed, last_processed FROM indexer_progress WHERE step = ?`;
  try {
    const result = await dbGet(sql, [step]);
    return result || { completed: 0, last_processed: null };
  } catch (error) {
    console.error(`Error checking progress for step ${step}:`, error);
    return { completed: 0, last_processed: null };
  }
}

export async function updateProgress(db, step, completed = 1, lastProcessed = null) {
  const dbRun = promisify(db.run).bind(db);
  const sql = `INSERT OR REPLACE INTO indexer_progress (step, completed, last_processed) VALUES (?, ?, ?)`;
  try {
    await dbRun(sql, [step, completed, lastProcessed]);
  } catch (error) {
    console.error(`Error updating progress for step ${step}:`, error);
    throw error;
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

export { promisify } from 'util';