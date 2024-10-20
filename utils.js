// utils.js

import axios from 'axios';
import WebSocket from 'ws';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';

// Updated logging function with levels and filtering
export function log(message, level = 'INFO', logToFile = true) {
  const logLevels = { 'ERROR': 0, 'INFO': 1, 'DEBUG': 2 };
  const currentLogLevel = 'INFO'; // Set the desired log level here
  
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

  // Log to file if logToFile is true and level is not DEBUG
  if (logToFile && level !== 'DEBUG') {
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

// Retry function that handles 400 status gracefully
export async function retryOperation(operation, retries = 3, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await operation();
    } catch (error) {
      // Skip retrying for 400 errors, as they are expected for some cases
      if (error.response && error.response.status === 400) {
        log(`Skipping retry for 400 error: ${error.response.data?.message || error.message}`, 'INFO');
        return error.response; // Return the 400 response for further handling
      }

      // Retry for other types of errors
      if (i < retries - 1) {
        log(`Retrying operation (${i + 1}/${retries}) after failure: ${error.message}`, 'INFO');
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        log(`Operation failed after ${retries} retries: ${error.message}`, 'ERROR');
        throw error;
      }
    }
  }
}

/// Helper function to fetch paginated data with versatile error handling
export async function fetchPaginatedData(url, key, options = {}) {
  const {
    limit = 100,
    paginationType = 'offset',
    paginationPayload = null,
    useNextKey = false,
    onError = null,
    fallbackKey = null
  } = options;

  let allData = [];
  let nextKey = null;
  let startAfter = null;

  log(`Starting paginated data fetch from: ${url}`, 'INFO');

  while (true) {
    let requestUrl = url;
    let payload;

    if (paginationType === 'offset') {
      const params = new URLSearchParams();
      params.set('pagination.limit', limit.toString());
      params.set('pagination.offset', allData.length.toString());
      requestUrl = `${url}?${params.toString()}`;
    } else if (paginationType === 'query') {
      payload = {
        ...paginationPayload,
        'pagination.limit': limit,
        ...(useNextKey && nextKey ? { 'pagination.key': nextKey } : {}),
        ...(!useNextKey && startAfter ? { start_after: startAfter } : {})
      };
    }

    try {
      const response = paginationType === 'offset'
        ? await retryOperation(() => axios.get(requestUrl))
        : await retryOperation(() => axios.get(requestUrl, { params: payload }));

      if (response.status === 400) {
        log(`Skipping processing due to expected 400 error.`, 'DEBUG');
        if (onError) onError(response); // Trigger the custom error handler, if provided
        return allData; // Return what has been collected so far
      }

      // Handle unexpected response
      if (!response || response.status !== 200 || !response.data) {
        log(`Unexpected response structure: ${JSON.stringify(response?.data || {})}`, 'ERROR');
        if (onError) onError(response);
        break;
      }

      const dataKey = response.data[key] || (fallbackKey && response.data[fallbackKey]);
      const dataBatch = Array.isArray(dataKey) ? dataKey : [];
      allData = allData.concat(dataBatch);

      log(`Fetched ${dataBatch.length} items in this batch. Total fetched: ${allData.length}`, 'INFO');

      // Pagination continuation
      if (useNextKey && response.data.pagination?.next_key) {
        nextKey = response.data.pagination.next_key;
      } else if (!useNextKey && dataBatch.length > 0) {
        startAfter = dataBatch[dataBatch.length - 1]?.code_id || dataBatch[dataBatch.length - 1]?.id;
      } else {
        break; // End if no more data
      }

      if (dataBatch.length < limit) break;
    } catch (error) {
      log(`Error fetching paginated data: ${error.message}`, 'ERROR');
      if (onError) onError(error);
      throw error;
    }
  }

  log(`Finished fetching data. Total fetched: ${allData.length}`, 'INFO');
  return allData;
}

// Contract query function with versatile error handling
export async function sendContractQuery(restAddress, contractAddress, payload, headers = {}, options = {}) {
  const {
    fallbackKey = null,
    onError = null,
    retryCount = 3
  } = options;

  if (!payload || typeof payload !== 'object') {
    log(`Invalid payload for contract ${contractAddress}: ${JSON.stringify(payload)}`, 'ERROR');
    if (onError) onError(new Error('Invalid payload'));
    return { error: 'Invalid payload', status: 400 };
  }

  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${payloadBase64}`;

  log(`Querying contract ${contractAddress} with payload: ${JSON.stringify(payload)}`, 'INFO');

  try {
    const response = await retryOperation(() => axios.get(url, { headers }), retryCount);

    if (response.status === 400) {
      const match = response.data?.message?.match(/Error parsing into type (\S+)::msg::QueryMsg/);
      if (match) {
        const contractType = match[1];
        log(`Identified contract type from 400 error: ${contractType} for contract ${contractAddress}`, 'INFO');
        return { data: { contractType }, status: 400 };
      }
      log(`400 error for contract ${contractAddress} did not contain recognizable type info`, 'DEBUG');
      return { error: 'Type not found in 400 response', status: 400 };
    }

    // Handle successful response
    const data = response?.data || {};
    const result = fallbackKey && data[fallbackKey] ? data[fallbackKey] : data;

    if (response.status === 200 && typeof result === 'object') {
      log(`Query successful for contract ${contractAddress}. Data length: ${JSON.stringify(result).length}`, 'INFO');
      return { data: result, status: response.status };
    } else {
      log(`Unexpected response or status for contract ${contractAddress}: ${response.status}`, 'ERROR');
      if (onError) onError(new Error('Unexpected response format'));
      return { error: 'Unexpected response format', status: response.status };
    }
  } catch (error) {
    log(`Error querying contract ${contractAddress}: ${error.message}`, 'ERROR');
    if (onError) onError(error);
    return { error: error.message, status: 500 };
  }
}

// batchInsert with enhanced logging and conflict handling
export async function batchInsert(dbRun, tableName, columns, data) {
  if (data.length === 0) {
    log(`No data to insert into ${tableName}. Skipping batch insert.`, 'INFO');
    return;
  }

  const placeholders = columns.map(() => '?').join(', '); // for each row
  const updateSet = columns
    .map(column => `${column} = excluded.${column}`)
    .join(', '); // setting update for each column

  // Construct the SQL for inserting or updating on conflict
  const insertSQL = `
    INSERT INTO ${tableName} (${columns.join(', ')})
    VALUES ${data.map(() => `(${placeholders})`).join(', ')}
    ON CONFLICT(address) DO UPDATE SET ${updateSet}
  `;

  const flatData = data.flat();

  try {
    await dbRun(insertSQL, flatData);
    log(`Successfully inserted or updated ${data.length} rows into ${tableName}.`, 'INFO');
  } catch (error) {
    log(`Error performing batch insert into ${tableName}: ${error.message}`, 'ERROR');
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