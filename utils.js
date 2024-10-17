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

// Retry function that avoids retrying on certain status codes like 400
export async function retryOperation(operation, retries = 3, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await operation();
    } catch (error) {
      // Avoid retrying if the error status is 400 (bad request)
      if (error.response && error.response.status === 400) {
        log(`Operation failed due to a 400 error: ${error.response.data?.message || error.message}`);
        throw error;
      }

      if (i < retries - 1) {
        log(`Retrying operation (${i + 1}/${retries}) after failure: ${error.message}`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        log(`Operation failed after ${retries} retries: ${error.message}`);
        throw error;
      }
    }
  }
}

// Helper function to fetch paginated data
export async function fetchPaginatedData(url, key, limit = 100, paginationType = 'offset', paginationPayload = null) {
  let allData = [];
  let startAfter = null;

  while (true) {
    let requestUrl = url;
    let payload;

    if (paginationType === 'offset') {
      // For URL-based pagination (offset)
      const params = new URLSearchParams();
      params.set('pagination.limit', limit.toString());
      params.set('pagination.offset', allData.length.toString());
      requestUrl = `${url}?${params.toString()}`;
    } else if (paginationType === 'query') {
      // For query-based pagination (custom payload)
      payload = {
        ...paginationPayload,
        limit,
        ...(startAfter && { start_after: startAfter })
      };
    }

    log(`Fetching data from: ${requestUrl}`);

    try {
      // Perform the request based on the pagination type
      let response;
      if (paginationType === 'offset') {
        response = await retryOperation(() => axios.get(requestUrl));
      } else if (paginationType === 'query') {
        response = await retryOperation(() => axios.post(requestUrl, payload));
      }

      // Validate the response
      if (response && response.status === 200 && response.data) {
        const dataBatch = response.data[key] || [];
        allData = allData.concat(dataBatch);
        log(`Fetched ${dataBatch.length} items in this batch.`);

        // If the number of fetched items is less than the limit, it indicates the last page
        if (dataBatch.length < limit) break;

        // Update the startAfter for the next query batch
        if (paginationType === 'query') {
          startAfter = dataBatch[dataBatch.length - 1];
        }
      } else {
        log(`Unexpected response structure: ${JSON.stringify(response?.data || {})}`);
        break;
      }
    } catch (error) {
      log(`Error fetching paginated data: ${error.message}`);
      throw error;
    }
  }

  log(`Total data fetched: ${allData.length}`);
  return allData;
}

// contract query function
export async function sendContractQuery(restAddress, contractAddress, payload, headers = {}) {
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${payloadBase64}`;
  
  log(`Querying contract at URL: ${url} with payload: ${JSON.stringify(payload)}`);

  try {
    const response = await retryOperation(() => axios.get(url, { headers }));
    
    // Check for valid response
    if (response && response.status === 200 && response.data) {
      log(`Query successful for contract ${contractAddress}: ${JSON.stringify(response.data)}`);
      return { data: response.data, status: response.status };
    } else {
      log(`Unexpected response structure or status for contract ${contractAddress}: ${response.status} - ${JSON.stringify(response.data)}`);
      return { error: 'Unexpected response format', status: response.status };
    }
  } catch (error) {
    // Improved logging for error scenarios
    if (error.response) {
      log(`Query failed for contract ${contractAddress} - HTTP ${error.response.status}: ${error.response.data?.message || error.message}`);
      return { error: error.response.data?.message || 'Request failed', status: error.response.status };
    } else {
      log(`Error querying contract ${contractAddress}: ${error.message}`);
      return { error: error.message, status: 500 };
    }
  }
}


// batchInsert with enhanced logging
export async function batchInsert(dbRun, tableName, columns, data) {
  if (data.length === 0) {
    log(`No data to insert into ${tableName}. Skipping batch insert.`);
    return;
  }

  const placeholders = data.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
  const insertSQL = `INSERT OR REPLACE INTO ${tableName} (${columns.join(', ')}) VALUES ${placeholders}`;
  const flatData = data.flat();

  try {
    await dbRun(insertSQL, flatData);
    log(`Successfully inserted ${data.length} rows into ${tableName}.`);
  } catch (error) {
    log(`Error performing batch insert into ${tableName}: ${error.message}`);
    throw error;
  }
}


// Updated checkProgress to include last_fetched_token
export async function checkProgress(db, step) {
  const dbGet = promisify(db.get).bind(db);
  const sql = `SELECT completed, last_processed, last_fetched_token FROM indexer_progress WHERE step = ?`;
  try {
    const result = await dbGet(sql, [step]);
    return result || { completed: 0, last_processed: null, last_fetched_token: null };
  } catch (error) {
    console.error(`Error checking progress for step ${step}:`, error);
    return { completed: 0, last_processed: null, last_fetched_token: null };
  }
}

// Updated updateProgress to handle last_fetched_token
export async function updateProgress(db, step, completed = 1, lastProcessed = null, lastFetchedToken = null) {
  const dbRun = promisify(db.run).bind(db);
  const sql = `INSERT OR REPLACE INTO indexer_progress (step, completed, last_processed, last_fetched_token) VALUES (?, ?, ?, ?)`;
  try {
    await dbRun(sql, [step, completed, lastProcessed, lastFetchedToken]);
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