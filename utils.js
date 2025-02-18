// utils.js
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';
import { config } from './config.js';
import { WebSocket } from 'ws';

// ES module-compatible __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Initialize the SQLite database
const db = new Database(path.join(__dirname, './data/indexer.db'));
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// Export the db instance for use in other modules
export { db };

export function log(message, level = 'INFO') {
  const logLevels = { 'ERROR': 0, 'INFO': 1, 'DEBUG': 2 };
  const currentLogLevel = config.logLevel || 'INFO';

  // Skip logging if the message level is below the current log level
  if (logLevels[level] > logLevels[currentLogLevel]) return;

  // Format the log message
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level}] ${message}`;

  // Log to console based on level
  if (level === 'ERROR' || currentLogLevel === 'DEBUG' || (level === 'INFO' && currentLogLevel !== 'ERROR')) {
    console.log(formattedMessage);
  }

  // Write to file if enabled and not DEBUG level to avoid duplicate log file entries
  if (config.logToFile && level !== 'DEBUG') {
    const logDir = './logs';
    const logFile = path.join(logDir, 'data_collection.log');

    if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });
    
    // Log directly to file without additional console confirmation
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
 * @param {boolean} skip400ErrorLog - If true, completely ignores 400 status errors, used only in identifyContractTypes.
 * @param {Object} [headers={}] - Optional headers to include in the request.
 * @returns {Object} - The parsed response from the contract query, or an error object if the request failed.
 */
export async function sendContractQuery(restAddress, contractAddress, payload, usePost = false, skip400ErrorLog = false) {
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString('base64');
  const requestUrl = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`;

  log(`Sending contract query to: ${requestUrl}`, 'DEBUG');
  log(`Encoded Payload: ${encodedPayload}`, 'DEBUG');

  // Dynamically handle the block height header based on config
  const headers = {};
  if (config.blockHeight !== null) {
    headers['x-cosmos-block-height'] = config.blockHeight.toString(); // Only add header if blockHeight is not null
  }

  try {
    const configOptions = { headers }; // Attach dynamically constructed headers
    const response = usePost
      ? await axios.post(requestUrl, { base64_encoded_payload: encodedPayload }, configOptions)
      : await axios.get(`${requestUrl}/${encodedPayload}`, configOptions);

    if (response.status === 200) {
      log(`Received response for contract ${contractAddress}`, 'DEBUG');
      return { data: response.data, error: null, message: response.data.message || null };
    }

    if (response.status === 400 && skip400ErrorLog) {
      return { data: null, error: null, message: response.data?.message || 'Expected 400 response' };
    }

    if ([404, 403, 501, 503].includes(response.status)) {
      const errorMsg = `Error querying contract: ${response.status} - ${response.statusText}`;
      log(errorMsg, 'ERROR');
      return { data: null, error: errorMsg, message: response.data?.message || null };
    }

    return { data: response.data, error: `Unexpected status ${response.status}`, message: response.data?.message || null };

  } catch (error) {
    const errorMsg = error.response?.data?.message || error.message;
    if (!(skip400ErrorLog && error.response?.status === 400)) {
      log(`Error querying contract ${contractAddress}: ${errorMsg}`, 'ERROR');
    }
    return { data: null, error: errorMsg, message: error.response?.data?.message || null };
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
  let pageCount = 0; // Track page count to determine if pagination logs are necessary

  log(`Fetching data for ${url.split('/').pop()}`, 'INFO'); // Simplified to show only the endpoint ID or type

  while (true) {
    const encodedNextKey = nextKey ? encodeURIComponent(nextKey) : '';
    const requestUrl = `${url}?pagination.limit=${limit}${encodedNextKey ? `&pagination.key=${encodedNextKey}` : ''}`;

    const operation = async () => axios.get(requestUrl);

    try {
      const response = await retryOperation(operation, retries, delay, backoffFactor);
      if (!response || response.status !== 200 || !response.data) {
        log(`Unexpected response structure for ${url}`, 'ERROR');
        return allData;
      }

      const dataBatch = Array.isArray(response.data[key]) ? response.data[key] : [];
      allData = allData.concat(dataBatch);

      pageCount += 1;
      nextKey = response.data.pagination?.next_key || null;

      if (!nextKey) break; // Stop if no next_key is present

      // Only log pagination if multiple pages are encountered
      if (pageCount > 1) {
        log(`Fetching additional page (${pageCount}) for ${url.split('/').pop()}`, 'INFO');
      }

      if (dataBatch.length < limit) break;
    } catch (error) {
      log(`Error fetching paginated data from ${url}: ${error.message}`, 'ERROR');
      return allData;
    }
  }

  log(`Fetched ${allData.length} items for ${url.split('/').pop()}`, 'INFO'); // Final count log
  return allData;
}

// Helper function to batch database operations
export function batchInsertOrUpdate(tableName, columns, values, uniqueColumns) {
  if (!Array.isArray(values) || values.length === 0) {
    log('No values provided for batch insertion or update.', 'DEBUG');
    return;
  }

  const placeholders = values[0].map(() => '?').join(', ');
  const updateConditions = Array.isArray(uniqueColumns)
    ? uniqueColumns.map((col) => `${col} = ?`).join(' AND ')
    : `${uniqueColumns} = ?`;

  const sqlInsert = `
    INSERT INTO ${tableName} (${columns.join(', ')})
    VALUES (${placeholders})
  `;
  const sqlUpdate = `
    UPDATE ${tableName}
    SET ${columns.map((col) => `${col} = ?`).join(', ')}
    WHERE ${updateConditions}
  `;

  const insert = db.prepare(sqlInsert);
  const update = db.prepare(sqlUpdate);

  const transaction = db.transaction((rows) => {
    for (const row of rows) {
      try {
        // Determine the unique values based on uniqueColumns
        const uniqueValues = Array.isArray(uniqueColumns)
          ? uniqueColumns.map((col) => row[columns.indexOf(col)])
          : [row[columns.indexOf(uniqueColumns)]];

        // Safely handle JSON strings, NULL values, and strings
        const sanitizedRow = row.map((val, idx) => {
          if (typeof val === 'object' && val !== null) {
            return JSON.stringify(val); // Escape JSON strings
          }
          return val !== null ? val : null;
        });

        log(`Processing row: ${sanitizedRow}`, 'DEBUG');

        // Check if an existing row matches the unique column(s)
        const existingRow = db
          .prepare(`SELECT * FROM ${tableName} WHERE ${updateConditions}`)
          .get(...uniqueValues);

        if (existingRow) {
          update.run([...sanitizedRow, ...uniqueValues]);
          log(
            `Updated row in ${tableName} where ${updateConditions} with values ${JSON.stringify(uniqueValues)}`,
            'DEBUG'
          );
        } else {
          insert.run(sanitizedRow);
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
    throw error; // Re-throw the error to allow higher-level handling
  }
}

// General-purpose HTTP request with retry logic
export async function fetchData(url, options = {}) {
  const { method = 'GET', data = null, headers = {}, retries = 3 } = options;

  const response = await retryOperation(async () => {
    try {
      const requestConfig = {
        method,
        url,
        timeout: config.timeout,
        headers,
        ...(data && { data })
      };

      const res = await axios(requestConfig);
      log(`Received response for ${method} request: ${JSON.stringify(res.data)}`, 'DEBUG');
      
      if (res.status !== 200) {
        log(`Failed to fetch data: ${res.status} - ${res.statusText}`, 'ERROR');
        return null;
      }
      return res.data;
    } catch (error) {
      log(`Error ${method} data to/from ${url}: ${error.message}`, 'ERROR');
      return null;
    }
  }, retries);
  
  return response || {};
}

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
