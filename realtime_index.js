import fetch from 'node-fetch';
import sqlite3 from 'sqlite3';
import WebSocket from 'ws';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import {
  RPC_WEBSOCKET_URL,
  restAddress,
  DB_PATH,
  LOG_FILE,
  BATCH_SIZE,
  MAX_RETRIES,
  RETRY_DELAY,
  POINTER_API_URL,
  SEIEVM_API_URL,
} from './config.js';
import {
  handleContract,
  determineContractType,
  fetchAllTokensForContracts,
  fetchTokenOwners,
  updatePointerAddresses,
  updateEVMAddresses
} from './cw721Helper.js';

let db;
let logStream;

// Log function
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

// Set up the database connection
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

// Create necessary tables
async function createTables() {
  const tables = [
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
    )`,
    `CREATE TABLE IF NOT EXISTS pointer_addresses (
      contract_address TEXT PRIMARY KEY,
      pointer_address TEXT,
      pointee_address TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS evm_addresses (
      wallet TEXT PRIMARY KEY,
      evm_address TEXT
    )`
  ];

  for (const table of tables) {
    try {
      await promisify(db.run).bind(db)(table);
    } catch (error) {
      log(`Error creating/updating table: ${error.message}`);
    }
  }

  log('Tables created/updated successfully.');
}

// Fetch all code IDs
async function fetchCodeIDs() {
  const codeInfos = [];
  let nextKey = null;
  do {
    const response = await retryOperation(() => fetch(`${restAddress}/cosmwasm/wasm/v1/code?pagination.key=${nextKey}&pagination.limit=100`));
    const data = await response.json();
    const newCodeInfos = data.code_infos.map(info => ({
      code_id: info.code_id,
      creator: info.creator
    }));
    codeInfos.push(...newCodeInfos);
    nextKey = data.pagination.next_key;
    log(`Fetched ${codeInfos.length} code IDs so far. Next key: ${nextKey || 'None'}`);
  } while (nextKey);

  log(`Fetched a total of ${codeInfos.length} code IDs.`);
  return codeInfos;
}

// Fetch contract addresses from each code ID
async function fetchContractAddressesFromCode(code_id) {
  const response = await retryOperation(() => fetch(`${restAddress}/cosmwasm/wasm/v1/contract?code_id=${code_id}`));
  const data = await response.json();
  return data.contracts || [];
}

// Insert contracts into the database
async function insertContracts(code_id, contracts) {
  for (const contractAddress of contracts) {
    const insertSQL = `INSERT OR REPLACE INTO contracts (address, creator) VALUES (?, ?)`;
    await promisify(db.run).bind(db)(insertSQL, [contractAddress, code_id]);
  }
}

// Step 1: Bootstrap the contracts by fetching code IDs and associated contract addresses
async function bootstrapContracts() {
  const codeIDs = await fetchCodeIDs();
  for (const { code_id, creator } of codeIDs) {
    const contracts = await fetchContractAddressesFromCode(code_id);
    if (contracts.length > 0) {
      log(`Inserting ${contracts.length} contracts for code_id: ${code_id}`);
      await insertContracts(creator, contracts);
    }
  }
}

// Step 2: Label contracts by determining their type
async function labelContracts() {
  const contracts = await promisify(db.all).bind(db)(`SELECT address FROM contracts WHERE type = 'UNKNOWN'`);
  for (const { address } of contracts) {
    const contractType = await determineContractType(restAddress, address);
    if (contractType) {
      await promisify(db.run).bind(db)(`UPDATE contracts SET type = ? WHERE address = ?`, [contractType, address]);
      log(`Labeled contract ${address} as ${contractType}`);
    }
  }
}

// Step 3: Fetch token IDs from CW721 contracts
async function fetchTokenIDsFromContracts() {
  await fetchAllTokensForContracts(restAddress, db);
}

// Step 4: Fetch owners for each token ID
async function fetchOwnersForTokenIDs() {
  await fetchTokenOwners(restAddress, db);
}

// Step 5: Fetch pointer addresses for each contract
async function fetchPointerAddresses() {
  await updatePointerAddresses(db);
}

// Step 6: Fetch EVM addresses for each wallet
async function fetchEVMAddresses() {
  await updateEVMAddresses(db);
}

// Retry operation with exponential backoff
async function retryOperation(operation, maxRetries = MAX_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      log(`Attempt ${i + 1} failed: ${error.message}`);
      if (i === maxRetries - 1) {
        throw error;
      }
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, i)));
    }
  }
}

// Main function
async function main() {
  try {
    setupLogging();
    await setupDatabase();
    await createTables();

    // Step 1: Bootstrap contracts
    await bootstrapContracts();

    // Step 2: Label contracts by type
    await labelContracts();

    // Step 3: Fetch token IDs
    await fetchTokenIDsFromContracts();

    // Step 4: Fetch owners of each token
    await fetchOwnersForTokenIDs();

    // Step 5: Fetch pointer addresses
    await fetchPointerAddresses();

    // Step 6: Fetch EVM addresses
    await fetchEVMAddresses();

    log('Data collection completed successfully.');
  } catch (error) {
    log(`An unexpected error occurred: ${error.message}`);
  } finally {
    if (db) {
      db.close();
      log('Database connection closed.');
    }
    if (logStream) {
      logStream.end();
    }
  }
}

main();
