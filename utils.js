// utils.js

import fs from 'fs';
import path from 'path';
import axios from 'axios';
import WebSocket from 'ws';
import { config } from './config.js';
import Database from 'better-sqlite3';

// Initialize the SQLite database
const db = new Database(path.join(__dirname, '../data/indexer.db'));
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

export function log(message, level = 'INFO') {
  const logLevels = { 'ERROR': 0, 'INFO': 1, 'DEBUG': 2 };
  const currentLogLevel = config.logLevel || 'INFO';

  // Skip logging if the message level is below the current log level
  if (logLevels[level] > logLevels[currentLogLevel]) {
    return;
  }

  // Format the log message with the current timestamp and level
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level}] ${message}`;

  // Always log to the console if the level is ERROR, otherwise based on level
  if (level === 'ERROR' || currentLogLevel === 'DEBUG') {
    console.log(formattedMessage);
  } else if (level === 'INFO' && currentLogLevel !== 'ERROR') {
    console.log(formattedMessage);
  }

  // Log to file if config.logToFile is true and the level is not DEBUG
  if (config.logToFile && level !== 'DEBUG') {
    const logDir = './logs';
    const logFile = path.join(logDir, 'data_collection.log');

    // Ensure the logs directory exists
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }

    // Append to the log file
    fs.appendFileSync(logFile, formattedMessage + '\n');
  }
}

// Retry function with exponential backoff and jitter
export async function retryOperation(operation, retries = 3, delay = 1000, backoffFactor = 2) {
  for (let i = 0; i < retries; i++) {
    try {
      return await operation();
    } catch (error) {
      log(`Retrying operation (${i + 1}/${retries}) after failure: ${error.message}`, 'INFO');
      const jitter = Math.random() * delay;
      await new Promise(resolve => setTimeout(resolve, delay + jitter));
      delay *= backoffFactor;
    }
  }
  log(`Operation failed after ${retries} retries.`, 'ERROR');
  return null; // Avoid throwing an error, return null to let main functions handle it
}

/**
 * Sends a smart contract query to the REST endpoint.
 * Handles encoding the payload in base64 format and accepts an optional POST request.
 * 
 * @param {string} restAddress - The REST API address.
 * @param {string} contractAddress - The contract address to query.
 * @param {Object} payload - The query payload that will be base64 encoded.
 * @param {boolean} usePost - If true, sends the query using a POST request; otherwise, uses GET. Defaults to false.
 * @returns {Object} - The parsed response from the contract query, or an error object if the request failed.
 */
export async function sendContractQuery(restAddress, contractAddress, payload, usePost = false) {
  // Encode the payload as a base64 string
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString('base64');
  const requestUrl = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`;

  log(`Sending contract query to: ${requestUrl}`, 'DEBUG');
  log(`Encoded Payload: ${encodedPayload}`, 'DEBUG');
  
  try {
    // Choose the request method based on usePost flag
    const response = usePost
      ? await axios.post(requestUrl, { base64_encoded_payload: encodedPayload })
      : await axios.get(`${requestUrl}/${encodedPayload}`);

    // Handle a successful response
    if (response.status === 200) {
      log(`Received response for contract ${contractAddress}`, 'DEBUG');
      return response.data; // Return the parsed JSON response
    } else if ([404, 403, 501, 503].includes(response.status)) {
      // Log complete failures for these status codes
      log(`Error querying contract: ${response.status} - ${response.statusText}`, 'ERROR');
      return { error: `Error: ${response.status} - ${response.statusText}` };
    } else {
      // For other statuses, just return the raw response data
      return response.data;
    }
  } catch (error) {
    // Log error if the request completely failed
    log(`Error querying contract: ${error.message}`, 'ERROR');
    if (error.response && error.response.data) {
      log(`Response data: ${JSON.stringify(error.response.data)}`, 'DEBUG');
    }
    return { error: error.message };
  }
}

// Fetch paginated data with conventional pagination strategy
export async function fetchPaginatedData(url, key, options = {}) {
  const {
    limit = 100,
    retries = 3,
    delay = 1000,
    backoffFactor = 2
  } = options;

  let allData = [];
  let nextKey = null;

  log(`Fetching data from: ${url}`, 'INFO');

  while (true) {
    const requestUrl = `${url}?pagination.limit=${limit}${nextKey ? `&pagination.key=${nextKey}` : ''}`;

    log(`Making request to URL: ${requestUrl}`, 'DEBUG');

    const operation = async () => axios.get(requestUrl);

    try {
      const response = await retryOperation(operation, retries, delay, backoffFactor);
      if (!response || response.status !== 200 || !response.data) {
        log(`Unexpected response structure for ${url}`, 'ERROR');
        return allData; // Return what was collected so far
      }

      log(`Received response data: ${JSON.stringify(response.data)}`, 'DEBUG');
      const dataKey = response.data[key];
      const dataBatch = Array.isArray(dataKey) ? dataKey : [];
      allData = allData.concat(dataBatch);

      if (response.data.pagination?.next_key) {
        nextKey = response.data.pagination.next_key;
      } else {
        break;
      }

      if (dataBatch.length < limit) break;
    } catch (error) {
      log(`Error fetching paginated data from ${url}: ${error.message}`, 'ERROR');
      return allData; // Return what was collected so far
    }
  }

  log(`Finished fetching ${allData.length} items for query ${url}`, 'INFO');
  return allData;
}

// Helper function to batch database operations with intelligent handling for updates
export function batchInsertOrUpdate(tableName, columns, values, uniqueColumn) {
  if (!Array.isArray(values) || values.length === 0) {
    log('No values provided for batch insertion or update.', 'DEBUG');
    return;
  }

  const placeholders = values[0].map(() => '?').join(', ');
  const sqlInsert = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
  const sqlUpdate = `UPDATE ${tableName} SET ${columns.map(col => `${col} = ?`).join(', ')} WHERE ${uniqueColumn} = ?`;
  const insert = db.prepare(sqlInsert);
  const update = db.prepare(sqlUpdate);

  const transaction = db.transaction((rows) => {
    for (const row of rows) {
      try {
        const uniqueValue = row[columns.indexOf(uniqueColumn)];
        const existingRow = db.prepare(`SELECT * FROM ${tableName} WHERE ${uniqueColumn} = ?`).get(uniqueValue);

        if (existingRow) {
          // Update the existing row if necessary
          update.run([...row, uniqueValue]);
          log(`Updated row in ${tableName} where ${uniqueColumn} = ${uniqueValue}`, 'DEBUG');
        } else {
          // Insert new row
          insert.run(row);
          log(`Inserted new row into ${tableName}`, 'DEBUG');
        }
      } catch (error) {
        log(`Failed to insert or update row in ${tableName}: ${error.message}`, 'ERROR');
      }
    }
  });

  try {
    transaction(values);
    log(`Successfully processed ${values.length} rows in ${tableName}`, 'DEBUG');
  } catch (error) {
    log(`Failed to process rows in ${tableName}: ${error.message}`, 'ERROR');
  }
}

// General-purpose GET request with retry logic
export async function fetchData(url, retries = 3) {
  const response = await retryOperation(async () => {
    try {
      const res = await axios.get(url, { timeout: config.timeout });
      log(`Received response for GET request: ${JSON.stringify(res.data)}`, 'DEBUG');
      if (res.status !== 200) {
        log(`Failed to fetch data: ${res.status} - ${res.statusText}`, 'ERROR');
        return null;
      }
      return res.data;
    } catch (error) {
      log(`Error fetching data from ${url}: ${error.message}`, 'ERROR');
      return null;
    }
  }, retries);
  return response || {}; // Avoid throwing an error, return an empty object to let main functions handle it
}

// WebSocket connection utility
export function createWebSocketConnection(url, onMessageCallback, onErrorCallback) {
  const ws = new WebSocket(url);

  ws.on('open', () => {
    log(`WebSocket connection established to ${url}`, 'INFO');
  });

  ws.on('message', (data) => {
    try {
      const parsedData = JSON.parse(data);
      log(`Received WebSocket message: ${JSON.stringify(parsedData)}`, 'DEBUG');
      onMessageCallback(parsedData);
    } catch (error) {
      log(`Failed to parse WebSocket message: ${error.message}`, 'ERROR');
    }
  });

  ws.on('error', (error) => {
    log(`WebSocket error on ${url}: ${error.message}`, 'ERROR');
    if (onErrorCallback) onErrorCallback(error);
  });

  ws.on('close', () => {
    log(`WebSocket connection closed for ${url}`, 'INFO');
  });

  return ws;
}

// Helper function to check progress in the database
export function checkProgress(step) {
  try {
    const row = db.prepare('SELECT completed, last_processed, last_fetched_token FROM indexer_progress WHERE step = ?').get(step);
    if (row) {
      return row;
    }
    return { completed: 0, last_processed: null, last_fetched_token: null };
  } catch (error) {
    log(`Error checking progress for step ${step}: ${error.message}`, 'ERROR');
    return { completed: 0, last_processed: null, last_fetched_token: null }; // Return default progress
  }
}

// Helper function to update progress in the database
export function updateProgress(step, completed = 1, lastProcessed = null, lastFetchedToken = null) {
  try {
    db.prepare(
      `INSERT OR REPLACE INTO indexer_progress (step, completed, last_processed, last_fetched_token) VALUES (?, ?, ?, ?)`
    ).run(step, completed, lastProcessed, lastFetchedToken);
    log(`Progress updated for step ${step}`, 'DEBUG');
  } catch (error) {
    log(`Error updating progress for step ${step}: ${error.message}`, 'ERROR');
  }
}
