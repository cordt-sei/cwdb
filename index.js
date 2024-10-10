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
import { 
  setupWebSocket, 
  log, 
  checkProgress, 
  updateProgress 
} from './utils.js';
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
    CREATE TABLE IF NOT EXISTS indexer_progress (
      step TEXT PRIMARY KEY,
      completed INTEGER DEFAULT 0,
      last_processed TEXT
    )`);

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

// index.js
async function runIndexer() {
  try {
    let progress;

    // Fetch and store all code IDs
    progress = await checkProgress(db, 'fetchCodeIds');
    if (!progress.completed) {
      await fetchAndStoreCodeIds(config.restAddress, db);
      await updateProgress(db, 'fetchCodeIds');
    }

    // Fetch and store contracts for all code IDs
    progress = await checkProgress(db, 'fetchContracts');
    if (!progress.completed) {
      await fetchAndStoreContractsByCode(config.restAddress, db);
      await updateProgress(db, 'fetchContracts');
    }

    // Fetch contract history
    progress = await checkProgress(db, 'fetchContractHistory');
    if (!progress.completed) {
      await fetchAndStoreContractHistory(config.restAddress, db);
      await updateProgress(db, 'fetchContractHistory');
    }

    // Identify contract types
    progress = await checkProgress(db, 'identifyContractTypes');
    if (!progress.completed) {
      await identifyContractTypes(config.restAddress, db);
      await updateProgress(db, 'identifyContractTypes');
    }

    // Fetch and store token IDs for CW721, CW404, and CW1155 contracts
    progress = await checkProgress(db, 'fetchTokens');
    if (!progress.completed) {
      await fetchAndStoreTokensForContracts(config.restAddress, db);
      await updateProgress(db, 'fetchTokens');
    }

    // Fetch and store token ownership details
    progress = await checkProgress(db, 'fetchTokenOwners');
    if (!progress.completed) {
      await fetchAndStoreTokenOwners(config.restAddress, db);
      await updateProgress(db, 'fetchTokenOwners');
    }

    // Fetch and store pointer data
    progress = await checkProgress(db, 'fetchPointerData');
    if (!progress.completed) {
      await fetchAndStorePointerData(config.pointerApi, db);
      await updateProgress(db, 'fetchPointerData');
    }

    // Fetch and store associated wallet addresses
    progress = await checkProgress(db, 'fetchAssociatedWallets');
    if (!progress.completed) {
      await fetchAndStoreAssociatedWallets(config.evmRpcAddress, db);
      await updateProgress(db, 'fetchAssociatedWallets');
    }

    log('All indexing steps completed successfully.');

    // WebSocket monitoring mode (WIP)
    // Not included in the resume functionality as it's WIP
    setupWebSocket(config.wsAddress, handleMessage, log);
  } catch (error) {
    log(`Error running indexer: ${error.message}`);
  }
}

// WebSocket message handler for real-time updates (WIP)
function handleMessage(message) {
  log('Received message:', message);
  // Handle new data entries from WebSocket
}

// Start the indexer
runIndexer();
