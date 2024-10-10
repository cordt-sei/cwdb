// index.js
import { 
  fetchAndStoreCodeIds, 
  fetchAndStoreContractsByCode, 
  fetchAndStoreContractHistory, 
  identifyContractTypes, 
  fetchAndStoreTokensForContracts, 
  fetchAndStoreTokenOwners, 
  fetchAndStorePointerData, 
  fetchAndStoreAssociatedWallets 
} from './cw721Helper.js';
import { setupWebSocket, log } from './utils.js';
import { Worker } from 'worker_threads';
import { config } from './config.js';
import sqlite3 from 'sqlite3';
import fs from 'fs';

// Ensure data and logs directories exist
if (!fs.existsSync('./data')) fs.mkdirSync('./data');
if (!fs.existsSync('./logs')) fs.mkdirSync('./logs');

// Initialize SQLite database
const db = new sqlite3.Database('./data/indexer.db');
initializeDatabase(db);

// Create necessary tables if they don't exist
function initializeDatabase(db) {
  db.run(`
    CREATE TABLE IF NOT EXISTS code_ids (
      code_id TEXT PRIMARY KEY,
      creator TEXT,
      data_hash TEXT,
      instantiate_permission TEXT
    )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS contracts (
      code_id TEXT,
      address TEXT PRIMARY KEY
    )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS contract_history (
      contract_address TEXT,
      operation TEXT,
      code_id TEXT,
      updated TEXT,
      msg TEXT
    )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS contract_tokens (
      contract_address TEXT PRIMARY KEY,
      extra_data TEXT
    )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS nft_owners (
      collection_address TEXT,
      token_id TEXT,
      owner TEXT
    )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS pointer_data (
      contract_address TEXT PRIMARY KEY,
      pointer_address TEXT,
      pointee_address TEXT
    )`);

  db.run(`
    CREATE TABLE IF NOT EXISTS wallet_associations (
      wallet_address TEXT PRIMARY KEY,
      evm_address TEXT
    )`);

  console.log('Database tables initialized.');
}

// Helper function to get contract tokens (token IDs) from the database
async function getContractTokens(db) {
  const sql = "SELECT contract_address, extra_data FROM contract_tokens"; // Fetch stored token data
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      resolve(rows.map(row => ({
        contract_address: row.contract_address,
        token_ids: row.extra_data.split(',') // Token IDs stored as a comma-separated string
      })));
    });
  });
}

// Helper function to get all contracts to process
async function getContractsToProcess(db, type = '') {
  const sql = type ? `SELECT address FROM contracts WHERE type = '${type}'` : `SELECT address FROM contracts`;
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      resolve(rows.map(row => row.address));
    });
  });
}

// Helper function to get all unique owners
async function getAllOwners(db) {
  const sql = "SELECT DISTINCT owner FROM nft_owners";
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      resolve(rows.map(row => row.owner));
    });
  });
}

// Multi-threading logic for speeding up the queries
async function runMultiThreadedQueries(taskType, dataToProcess) {
  const chunkSize = Math.ceil(dataToProcess.length / config.numWorkers);
  const workerPromises = [];

  for (let i = 0; i < config.numWorkers; i++) {
    const workerData = dataToProcess.slice(i * chunkSize, (i + 1) * chunkSize);

    const worker = new Worker('./workerTask.js', {
      workerData: {
        restAddress: config.restAddress,
        dbFilePath: './data/indexer.db', // Pass the SQLite DB path to worker
        contractsToProcess: workerData,
        taskType
      }
    });

    workerPromises.push(new Promise((resolve, reject) => {
      worker.on('message', resolve);
      worker.on('error', reject);
    }));
  }

  return Promise.all(workerPromises);
}

// index.js
async function runIndexer() {
  try {
    // Step 1: Fetch and store all code IDs
    await fetchAndStoreCodeIds(config.restAddress, db);

    // Step 2: Fetch and store contracts for all code IDs
    await fetchAndStoreContractsByCode(config.restAddress, db);

    // Step 3: Fetch contract history
    await fetchAndStoreContractHistory(config.restAddress, db);

    // Step 4: Identify contract types
    await identifyContractTypes(config.restAddress, db);

    // Step 5: Fetch and store token IDs for CW721 contracts
    await fetchAndStoreTokensForContracts(config.restAddress, db);

    // Step 6: Fetch and store token ownership details
    await fetchAndStoreTokenOwners(config.restAddress, db);

    // Step 7: Fetch and store pointer data
    await fetchAndStorePointerData(config.pointerApi, db);

    // Step 8: Fetch and store associated wallet addresses
    await fetchAndStoreAssociatedWallets(config.evmRpcAddress, db);

    // Step 9: Enter WebSocket monitoring mode
    setupWebSocket(config.wsAddress, handleMessage, log);
  } catch (error) {
    log(`Error running indexer: ${error.message}`);
  }
}

// WebSocket message handler for real-time updates
function handleMessage(message) {
  log('Received message:', message);
  // Handle new data entries from WebSocket
}

// Start the indexer
runIndexer();
