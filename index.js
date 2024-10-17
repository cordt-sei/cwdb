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

// Create necessary tables if they don't exist, including adding missing columns if required
function initializeDatabase(db) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // Create tables
      const createTableStatements = [
        `CREATE TABLE IF NOT EXISTS indexer_progress (
          step TEXT PRIMARY KEY,
          completed INTEGER DEFAULT 0,
          last_processed TEXT,
          last_fetched_token TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS code_ids (
          code_id TEXT PRIMARY KEY,
          creator TEXT,
          data_hash TEXT,
          instantiate_permission TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS contracts (
          code_id TEXT,
          address TEXT PRIMARY KEY,
          type TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS contract_history (
          contract_address TEXT,
          operation TEXT,
          code_id TEXT,
          updated TEXT,
          msg TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS contract_tokens (
          contract_address TEXT PRIMARY KEY,
          extra_data TEXT,
          contract_type TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS nft_owners (
          collection_address TEXT,
          token_ids TEXT,
          owner TEXT,
          contract_type TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS pointer_data (
          contract_address TEXT PRIMARY KEY,
          pointer_address TEXT,
          pointee_address TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS wallet_associations (
          wallet_address TEXT PRIMARY KEY,
          evm_address TEXT
        )`
      ];

      createTableStatements.forEach(statement => {
        db.run(statement, (err) => {
          if (err) {
            console.error('Error creating table:', err);
            reject(err);
          }
        });
      });

      // Alter table to add the new column if it does not exist
      const alterTableStatements = [
        `ALTER TABLE indexer_progress ADD COLUMN last_fetched_token TEXT`
      ];

      alterTableStatements.forEach(statement => {
        db.run(statement, (err) => {
          if (err && !err.message.includes('duplicate column name')) {
            console.error('Error altering table:', err);
            reject(err);
          }
        });
      });

      // Check if the database is new by looking for any existing progress
      db.get("SELECT COUNT(*) as count FROM indexer_progress", (err, row) => {
        if (err) {
          console.error('Error checking indexer_progress:', err);
          reject(err);
          return;
        }

        if (row.count === 0) {
          console.log('New database initialized. Progress table is empty.');
        } else {
          console.log('Existing database detected. Progress table has entries.');
        }

        console.log('Database tables initialized successfully.');
        resolve();
      });
    });
  });
}

// main function
async function runIndexer() {
  try {
    await initializeDatabase(db);
    
    // Fetch and store all code IDs
    let progress = await checkProgress(db, 'fetchCodeIds');
    if (!progress.completed) {
      await fetchAndStoreCodeIds(config.restAddress, db);
      await updateProgress(db, 'fetchCodeIds', 1);
    } else {
      console.log('Skipping fetchCodeIds: Already completed');
    }

    // Fetch and store contracts for all code IDs
    progress = await checkProgress(db, 'fetchContracts');
    if (!progress.completed) {
      await fetchAndStoreContractsByCode(config.restAddress, db);
      await updateProgress(db, 'fetchContracts', 1);
    } else {
      console.log('Skipping fetchContracts: Already completed');
    }

    // Fetch contract history
    progress = await checkProgress(db, 'fetchContractHistory');
    if (!progress.completed) {
      await fetchAndStoreContractHistory(config.restAddress, db);
      await updateProgress(db, 'fetchContractHistory', 1);
    } else {
      console.log('Skipping fetchContractHistory: Already completed');
    }

    // Identify contract types
    progress = await checkProgress(db, 'identifyContractTypes');
    if (!progress.completed) {
      await identifyContractTypes(config.restAddress, db);
      await updateProgress(db, 'identifyContractTypes', 1);
    } else {
      console.log('Skipping identifyContractTypes: Already completed');
    }

    // Fetch and store token IDs for CW721, CW404, and CW1155 contracts
    progress = await checkProgress(db, 'fetchTokens');
    if (!progress.completed) {
      await fetchAndStoreTokensForContracts(config.restAddress, db);
      await updateProgress(db, 'fetchTokens', 1);
    } else {
      console.log('Skipping fetchTokens: Already completed');
    }

    // Fetch and store token ownership details
    progress = await checkProgress(db, 'fetchTokenOwners');
    if (!progress.completed) {
      await fetchAndStoreTokenOwners(config.restAddress, db);
      await updateProgress(db, 'fetchTokenOwners', 1);
    } else {
      console.log('Skipping fetchTokenOwners: Already completed');
    }

    // Fetch and store pointer data
    progress = await checkProgress(db, 'fetchPointerData');
    if (!progress.completed) {
      await fetchAndStorePointerData(config.pointerApi, db);
      await updateProgress(db, 'fetchPointerData', 1);
    } else {
      console.log('Skipping fetchPointerData: Already completed');
    }

    // Fetch and store associated wallet addresses
    progress = await checkProgress(db, 'fetchAssociatedWallets');
    if (!progress.completed) {
      await fetchAndStoreAssociatedWallets(config.evmRpcAddress, db);
      await updateProgress(db, 'fetchAssociatedWallets', 1);
    } else {
      console.log('Skipping fetchAssociatedWallets: Already completed');
    }

    console.log('All indexing steps completed successfully.');

    // WebSocket monitoring mode
    setupWebSocket(config.wsAddress, handleMessage, console.log);
  } catch (error) {
    console.error(`Error running indexer: ${error.message}`);
    if (error.stack) {
      console.error(`Stack trace: ${error.stack}`);
    }
  }
}

// WebSocket message handler for real-time updates (WIP)
function handleMessage(message) {
  log('Received message:', message);
  // Handle new data entries from WebSocket
}

// Start the indexer
runIndexer().catch(error => {
  console.error('Failed to run indexer:', error);
});