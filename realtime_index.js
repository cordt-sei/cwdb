import fetch from 'node-fetch';
import WebSocket from 'ws';
import sqlite3 from 'sqlite3';
import { promisify } from 'util';
import fs from 'fs';
import path, { dirname } from 'path';
import { fileURLToPath } from 'url';
import { 
  RPC_WEBSOCKET_URL, 
  restAddress, 
  DB_PATH, 
  LOG_FILE, 
  BLOCK_HEIGHT,
  MAX_RETRIES,
  RETRY_DELAY
} from './config.js';

import {
  handleContract,
  determineContractType,
  fetchAllTokensForContracts,
  fetchTokenOwners,
  checkAndFixMissingContractTypes,
  checkAndFixMissingTokens,
  checkAndFixMissingOwners,
  updatePointerAddresses,
  updateEVMAddresses
} from './cw721Helper.js';

const __filename = fileURLToPath(import.meta.url);

let db;
let logStream;

// Define the log function at the top level
function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${message}\n`;
  console.log(logMessage.trim());
  if (logStream && !logStream.destroyed) {
    logStream.write(logMessage);
  }
}

// Set up logging
function setupLogging() {
  const logDir = path.dirname(LOG_FILE);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
}

// Set up database connection
async function setupDatabase() {
  return new Promise((resolve, reject) => {
    const dbDir = path.dirname(DB_PATH);
    if (!fs.existsSync(dbDir)) {
      fs.mkdirSync(dbDir, { recursive: true });
    }
    db = new sqlite3.Database(DB_PATH, (err) => {
      if (err) {
        log(`Error opening database: ${err.message}`);
        reject(err);
      } else {
        log('Connected to the SQLite database.');
        resolve();
      }
    });
  });
}

// Create necessary tables for contracts, tokens, and owners
async function createTables() {
  const tables = [
    `CREATE TABLE IF NOT EXISTS indexer_progress (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_processed_code_id INTEGER,
      last_processed_block_height INTEGER
    )`,
    `CREATE TABLE IF NOT EXISTS contracts (
      address TEXT PRIMARY KEY,
      admin TEXT,
      creator TEXT,
      type TEXT DEFAULT 'UNKNOWN'
    )`,
    `CREATE TABLE IF NOT EXISTS contract_info (
      address TEXT PRIMARY KEY,
      type TEXT DEFAULT 'UNKNOWN'
    )`,
    `CREATE TABLE IF NOT EXISTS nft_owners (
      collection_address TEXT,
      token_id TEXT,
      owner TEXT,
      PRIMARY KEY (collection_address, token_id)
    )`,
    `CREATE TABLE IF NOT EXISTS contract_tokens (
      contract_address TEXT PRIMARY KEY,
      admin TEXT,
      creator TEXT,
      contract_type TEXT,
      extra_data TEXT
    )`
  ];

  // Execute each SQL statement to create the necessary tables
  for (const table of tables) {
    try {
      await promisify(db.run).bind(db)(table);
    } catch (error) {
      log(`Error creating/updating table: ${error.message}`);
    }
  }

  log('Tables created/updated successfully.');
}

// Fetch all contracts without a known type
async function fetchContractsWithoutType() {
  const sql = "SELECT address FROM contracts WHERE type = 'UNKNOWN'";
  const contracts = await promisify(db.all).bind(db)(sql);
  return contracts.map(contract => contract.address);
}

// Update contract type in the database
async function updateContractType(contractAddress, contractType) {
  const sql = "UPDATE contracts SET type = ? WHERE address = ?";
  await promisify(db.run).bind(db)(sql, [contractType, contractAddress]);

  const sqlInfo = "UPDATE contract_info SET type = ? WHERE address = ?";
  await promisify(db.run).bind(db)(sqlInfo, [contractType, contractAddress]);

  log(`Updated contract ${contractAddress} with type ${contractType}`);
}

// Step 1: Retroactively determine contract types using the test query
async function determineContractTypes() {
  log('Determining contract types...');
  const contracts = await fetchContractsWithoutType();

  for (const contractAddress of contracts) {
    log(`Checking contract type for ${contractAddress}...`);
    try {
      const contractType = await determineContractType(restAddress, contractAddress);

      if (contractType) {
        await updateContractType(contractAddress, contractType);
        log(`Contract ${contractAddress} is labeled as ${contractType}`);
      } else {
        log(`Could not determine contract type for ${contractAddress}.`);
      }
    } catch (error) {
      log(`Error determining contract type for ${contractAddress}: ${error.message}`);
    }
  }

  log('Finished determining contract types.');
}

// Step 2: Iterate over all CW721 contracts to query token IDs and owners
async function processCW721Contracts() {
  const sql = "SELECT address FROM contracts WHERE type = 'CW721'";
  const cw721Contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of cw721Contracts) {
    const contractAddress = contract.address;
    log(`Querying tokens for CW721 contract ${contractAddress}...`);

    try {
      await handleContract(restAddress, contractAddress, db);
    } catch (error) {
      log(`Error processing CW721 contract ${contractAddress}: ${error.message}`);
    }
  }
}

// Function to get the last processed state from the database
async function getLastProcessedState() {
  try {
    const result = await promisify(db.get).bind(db)('SELECT * FROM indexer_progress WHERE id = 1');
    return result || { last_processed_code_id: 0, last_processed_block_height: 0 };
  } catch (error) {
    log(`Error retrieving last processed state: ${error.message}`);
    return { last_processed_code_id: 0, last_processed_block_height: 0 }; // Default values
  }
}

// Retry operation with exponential backoff
async function retryOperation(operation, maxRetries = MAX_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      // Attempt the operation
      return await operation();
    } catch (error) {
      log(`Attempt ${i + 1} failed: ${error.message}`);
      // If it's the last retry, throw the error
      if (i === maxRetries - 1) {
        log('Max retries reached. Operation failed.');
        throw error;
      }
      // Otherwise, wait before retrying
      log(`Retrying operation in ${RETRY_DELAY * Math.pow(2, i)} ms...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, i)));
    }
  }
}

// WebSocket connection and subscription
function setupWebSocket() {
  const ws = new WebSocket(RPC_WEBSOCKET_URL);

  ws.on('open', () => {
    log('Connected to WebSocket');
    
    // Subscribe to instantiate contract events
    ws.send(JSON.stringify({
      jsonrpc: '2.0',
      method: 'subscribe',
      id: 2,
      params: {
        query: "tm.event='Tx' AND instantiate._contract_address EXISTS"
      }
    }));
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      if (message.result && message.result.events) {
        const events = message.result.events;

        if (events['instantiate._contract_address']) {
          const instantiateEvent = {
            _contract_address: events['instantiate._contract_address'][0],
            code_id: events['instantiate.code_id'][0],
            sender: events['message.sender'][0],
            admin: events['instantiate.admin'] ? events['instantiate.admin'][0] : '',
            label: events['instantiate.label'] ? events['instantiate.label'][0] : ''
          };
          log(`Detected new contract: ${instantiateEvent._contract_address}`);
          await determineContractTypes();
        }
      }
    } catch (error) {
      log(`Error processing WebSocket message: ${error.message}`);
    }
  });

  ws.on('close', () => {
    log('WebSocket connection closed. Reconnecting...');
    setTimeout(setupWebSocket, 5000);
  });

  ws.on('error', (error) => {
    log(`WebSocket error: ${error.message}`);
  });
}

async function main() {
  try {
    setupLogging();
    await setupDatabase();
    await createTables();

    log(`Using block height: ${BLOCK_HEIGHT === null ? 'latest' : BLOCK_HEIGHT}`);

    const { last_processed_code_id, last_processed_block_height } = await getLastProcessedState();
    log(`Resuming from Code ID: ${last_processed_code_id}, Block Height: ${last_processed_block_height}`);
    
    // Check and fix missing data
    await checkAndFixMissingContractTypes(db, restAddress);
    await checkAndFixMissingTokens(db, restAddress);
    await checkAndFixMissingOwners(db, restAddress);

    // Fetch all tokens for contracts (CW721 and CW1155)
    await fetchAllTokensForContracts(restAddress, db);

    // Fetch and store token owners for the fetched tokens
    await fetchTokenOwners(restAddress, db);

    // Get the latest block height
    const latestBlockResponse = await retryOperation(() => fetch(`${restAddress}/cosmos/base/tendermint/v1beta1/blocks/latest`));
    const latestBlockData = await latestBlockResponse.json();
    const currentBlockHeight = parseInt(latestBlockData.block.header.height);

    // Fetch contract code info
    const codeInfos = [];
    let nextKey = null;
    do {
      const response = await retryOperation(() => fetch(`${restAddress}/cosmwasm/wasm/v1/code?pagination.key=${nextKey}&pagination.reverse=false&pagination.limit=100`));
      const data = await response.json();
      const newCodeInfos = data.code_infos
        .filter(info => parseInt(info.code_id) > last_processed_code_id)
        .map(info => ({
          code_id: info.code_id,
          creator: info.creator,
          data_hash: info.data_hash,
          instantiate_permission: JSON.stringify(info.instantiate_permission)
        }));
      codeInfos.push(...newCodeInfos);
      nextKey = data.pagination.next_key;
      log(`Fetched ${codeInfos.length} new code infos so far. Next key: ${nextKey || 'None'}`);
    } while (nextKey);

    log(`Fetched a total of ${codeInfos.length} new code infos. Starting processing with workers.`);

    // Process contracts using workers
    await processContractsWithWorkers(codeInfos, currentBlockHeight);

    // Update pointer and EVM addresses after contract processing
    await updatePointerAddresses(db);
    await updateEVMAddresses(db);

    // Update the last processed state if new code infos were fetched
    if (codeInfos.length > 0) {
      const lastCodeId = codeInfos[codeInfos.length - 1].code_id;
      await updateLastProcessedState(lastCodeId, currentBlockHeight);
    }

    log('Data collection completed successfully.');

    // Set up WebSocket if no specific block height is provided
    if (BLOCK_HEIGHT === null) {
      setupWebSocket();
    } else {
      log('Specific block height provided. Skipping WebSocket setup.');
    }
  } catch (error) {
    log(`An unexpected error occurred: ${error.message}`);
    log(error.stack);
  } finally {
    if (db) {
      try {
        await promisify(db.run).bind(db)('PRAGMA optimize');
        await new Promise((resolve, reject) => {
          db.close((err) => {
            if (err) reject(err);
            else resolve();
          });
        });
        log('Database connection closed.');
      } catch (closeError) {
        log(`Error closing database: ${closeError.message}`);
      }
    }
    if (logStream) {
      logStream.end();
    }
  }
}

main();
