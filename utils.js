// utils.js

import fs from 'fs';
import path from 'path';
import axios from 'axios';
import WebSocket from 'ws';
import { promisify } from 'util';
import { config } from './config.js';

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
  }

  // Log to file if config.logToFile is true and level is not DEBUG
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
  let lastError;
  for (let i = 0; i < retries; i++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (i < retries - 1) {
        log(`Retrying operation (${i + 1}/${retries}) after failure: ${error.message}`, 'INFO');
        const jitter = Math.random() * delay;
        await new Promise(resolve => setTimeout(resolve, delay + jitter));
        delay *= backoffFactor;
      }
    }
  }
  log(`Operation failed after ${retries} retries: ${lastError.message}`, 'ERROR');
  throw lastError;
}

// Contract query function that logs details and handles responses
export async function sendContractQuery(restAddress, contractAddress, payload, usePost = false) {
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString('base64');
  const requestUrl = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`;

  log(`Attempting to query contract: ${contractAddress}`, 'DEBUG');
  log(`Request URL: ${requestUrl}`, 'DEBUG');

  try {
    const response = usePost
      ? await axios.post(requestUrl, { base64_encoded_payload: encodedPayload })
      : await axios.get(`${requestUrl}/${encodedPayload}`);

    if (response.status === 200) {
      log(`Successfully queried contract: ${contractAddress}`, 'INFO');
      return response.data;
    } else {
      log(`Unexpected response status: ${response.status}`, 'ERROR');
      return null;
    }
  } catch (error) {
    log(`Error querying contract: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch paginated data with various pagination strategies
export async function fetchPaginatedData(url, key, options = {}) {
  const {
    limit = 100,
    paginationType = 'offset',
    paginationPayload = null,
    useNextKey = false,
    logLevel = 'FINAL_ONLY', // 'DETAILED' or 'FINAL_ONLY'
    retries = 3,
    delay = 1000,
    backoffFactor = 2,
    handleError = null,
    skipPagination = false
  } = options;

  if (skipPagination) {
    // If pagination is skipped, just make a single request
    const operation = async () => axios.get(url, { params: paginationPayload });
    const response = await retryOperation(operation, retries, delay, backoffFactor);
    if (!response || response.status !== 200 || !response.data) {
      log(`Unexpected response structure for ${url}`, 'ERROR');
      throw new Error(`Unexpected response status: ${response?.status}`);
    }
    return response.data[key] || [];
  }

  let allData = [];
  let nextKey = null;
  let startAfter = null;

  if (logLevel === 'DETAILED') {
    log(`Fetching data from: ${url}`, 'INFO');
  }

  while (true) {
    let requestUrl = url;
    let payload;

    switch (paginationType) {
      case 'offset':
        const offsetParams = new URLSearchParams();
        offsetParams.set('pagination.limit', limit.toString());
        offsetParams.set('pagination.offset', allData.length.toString());
        requestUrl = `${url}?${offsetParams.toString()}`;
        break;
      case 'query':
        payload = {
          ...paginationPayload,
          'pagination.limit': limit,
          ...(useNextKey && nextKey ? { 'pagination.key': nextKey } : {}),
          ...(!useNextKey && startAfter ? { start_after: startAfter } : {})
        };
        break;
      default:
        throw new Error(`Unsupported pagination type: ${paginationType}`);
    }

    const operation = async () => {
      if (paginationType === 'offset') {
        return await axios.get(requestUrl);
      } else {
        return await axios.get(requestUrl, { params: payload });
      }
    };

    try {
      const response = await retryOperation(operation, retries, delay, backoffFactor);
      if (!response || response.status !== 200 || !response.data) {
        log(`Unexpected response structure for ${url}`, 'ERROR');
        throw new Error(`Unexpected response status: ${response?.status}`);
      }

      const dataKey = response.data[key];
      const dataBatch = Array.isArray(dataKey) ? dataKey : [];
      allData = allData.concat(dataBatch);

      if (useNextKey && response.data.pagination?.next_key) {
        nextKey = response.data.pagination.next_key;
      } else if (!useNextKey && dataBatch.length > 0) {
        startAfter = dataBatch[dataBatch.length - 1]?.id;
      } else {
        break;
      }

      if (dataBatch.length < limit) break;
    } catch (error) {
      log(`Error fetching paginated data from ${url}: ${error.message}`, 'ERROR');
      throw error;
    }
  }

  log(`Finished fetching ${allData.length} items for query ${url}`, 'INFO');
  return allData;
}

// Batch insert function to handle data insertion with conflict resolution
export async function batchInsert(dbRun, tableName, columns, data) {
  if (data.length === 0) {
    log(`No data to insert into ${tableName}. Skipping batch insert.`, 'INFO');
    return;
  }

  const placeholders = columns.map(() => '?').join(', ');
  let updateSet = '';

  // Define conflict resolution for the 'contracts' table
  if (tableName === 'contracts') {
    updateSet = columns
      .filter(column => column !== 'address') // Avoid updating the unique key column
      .map(column => `${column} = excluded.${column}`)
      .join(', ');
  } else {
    // Other tables can have generic conflict resolution or be handled differently
    log(`No conflict resolution defined for table: ${tableName}`, 'WARN');
  }

  // SQL for inserting or updating on conflict
  const insertSQL = `
    INSERT INTO ${tableName} (${columns.join(', ')})
    VALUES ${data.map(() => `(${placeholders})`).join(', ')}
    ${updateSet ? `ON CONFLICT(address) DO UPDATE SET ${updateSet}` : ''}
  `;

  // Flatten the data for use in the SQL query
  const flatData = data.flat();

  try {
    await dbRun(insertSQL, flatData);
    log(`Successfully inserted or updated ${data.length} rows into ${tableName}.`, 'INFO');
  } catch (error) {
    log(`Error performing batch insert into ${tableName}: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Check progress function to track indexer state
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

// Update progress function to record the current state of the indexer
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

// WebSocket setup function to handle real-time communication
export function setupWebSocket(url, messageHandler, log) {
  const ws = new WebSocket(url);

  ws.on('open', () => {
    log('Connected to WebSocket');
  });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      messageHandler(message);
    } catch (error) {
      log(`Error processing WebSocket message: ${error.message}`);
    }
  });

  ws.on('close', () => {
    log('WebSocket connection closed. Reconnecting...');
    setTimeout(() => setupWebSocket(url, messageHandler, log), 5000);
  });

  ws.on('error', (error) => {
    log(`WebSocket error: ${error.message}`);
  });

  return ws;
}

export { promisify } from 'util';
