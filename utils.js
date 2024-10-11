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

// Simple operation function without retry logic, throwing actual error response
export async function retryOperation(operation) {
  try {
    return await operation();
  } catch (error) {
    // Log the full error details, including the response if available
    if (error.response) {
      log(`Operation failed with status ${error.response.status}: ${JSON.stringify(error.response.data)}`);
    } else {
      log(`Operation failed: ${error.message}`);
    }
    throw error;  // Throw the actual error to ensure the response is passed along
  }
}

// Helper function to fetch paginated data
export async function fetchPaginatedData(url, payload, key, batchSize = 100) {
  let allData = [];
  let nextKey = payload?.['pagination.key'] || null;

  do {
    const params = new URLSearchParams();
    if (nextKey) {
      params.set('pagination.key', nextKey);
    }
    params.set('pagination.limit', batchSize.toString());

    const fullUrl = `${url}?${params.toString()}`;
    log(`Fetching data from: ${fullUrl}`);
    const response = await retryOperation(() => axios.get(fullUrl));

    if (response.status === 200 && response.data) {
      log(`Full API response: ${JSON.stringify(response.data)}`);

      // Correctly handle the tokens data extraction
      let dataBatch = [];
      if (response.data?.data?.[key]) {
        dataBatch = response.data.data[key];  // Access 'tokens' under 'data'
      }

      if (dataBatch.length > 0) {
        allData = allData.concat(dataBatch);
        log(`Fetched ${dataBatch.length} items in this batch.`);
      } else {
        log('No items found in this batch.');
      }

      // Handle pagination if applicable
      nextKey = response.data?.pagination?.next_key || null;
    } else {
      throw new Error(`Unexpected response: ${JSON.stringify(response.data)}`);
    }

  } while (nextKey);

  log(`Total data fetched: ${allData.length}`);
  return allData;
}

// Helper function to send a smart contract query
export async function sendContractQuery(restAddress, contractAddress, payload, headers = {}) {
  // Ensure payload is valid
  if (!payload || typeof payload !== 'object') {
    log(`Invalid payload provided for contract ${contractAddress}`);
    return null;
  }

  // Convert payload to base64
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${payloadBase64}`;

  try {
    const response = await retryOperation(() => axios.get(url, { headers }));

    // Log the full response
    log(`Full API response for ${contractAddress}, token ${payload.owner_of.token_id}: ${JSON.stringify(response.data)}`);
    log(`Response status: ${response.status}, Type of status: ${typeof response.status}`);

    // Return both data and status
    return { data: response.data, status: response.status };
  } catch (error) {
    log(`Error querying contract ${contractAddress}: ${error.message}`);
    return null;
  }
}

// batchInsert utility function
export async function batchInsert(dbRun, tableName, columns, data) {
  if (data.length === 0) return;

  const placeholders = data.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
  const insertSQL = `INSERT OR REPLACE INTO ${tableName} (${columns.join(', ')}) VALUES ${placeholders}`;
  const flatData = data.flat();

  try {
    await dbRun(insertSQL, flatData);
  } catch (error) {
    log(`Error performing batch insert into ${tableName}: ${error.message}`);
    throw error;
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