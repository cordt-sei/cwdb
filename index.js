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
} from './contractHelper.js';
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
await initializeDatabase(db);

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
            log(`Error creating table with statement "${statement}": ${err.message}`, 'ERROR');
            reject(new Error(`Table creation failed: ${statement}`));
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
            log(`Error altering table with statement "${statement}": ${err.message}`, 'ERROR');
            reject(new Error(`Table alteration failed: ${statement}`));
          }
        });
      });

      // Check if the database is new by looking for any existing progress
      db.get("SELECT COUNT(*) as count FROM indexer_progress", (err, row) => {
        if (err) {
          log(`Error checking indexer_progress: ${err.message}`, 'ERROR');
          reject(new Error('Failed to check indexer progress'));
          return;
        }

        if (row.count === 0) {
          log('New database initialized. Progress table is empty.', 'INFO');
        } else {
          log('Existing database detected. Progress table has entries.', 'INFO');
        }

        log('Database tables initialized successfully.', 'INFO');
        resolve();
      });
    });
  });
}

// Main function
async function runIndexer() {
  try {
    await initializeDatabase(db);
    log('Database initialized.', 'INFO');

    // Define the steps to execute
    const steps = [
      { name: 'fetchCodeIds', action: () => fetchAndStoreCodeIds(config.restAddress, db) },
      { name: 'fetchContracts', action: () => fetchAndStoreContractsByCode(config.restAddress, db) },
      { name: 'fetchContractHistory', action: () => fetchAndStoreContractHistory(config.restAddress, db) },
      { name: 'identifyContractTypes', action: () => identifyContractTypes(config.restAddress, db) },
      { name: 'fetchTokens', action: () => fetchAndStoreTokensForContracts(config.restAddress, db) },
      { name: 'fetchTokenOwners', action: () => fetchAndStoreTokenOwners(config.restAddress, db) },
      { name: 'fetchPointerData', action: () => fetchAndStorePointerData(config.pointerApi, db) },
      { name: 'fetchAssociatedWallets', action: () => fetchAndStoreAssociatedWallets(config.evmRpcAddress, db) }
    ];

    // Execute each step in sequence
    for (const step of steps) {
      const progress = await checkProgress(db, step.name);
      if (!progress.completed) {
        log(`Starting step: ${step.name}`, 'INFO');
        try {
          await step.action();
          await updateProgress(db, step.name, 1);
          log(`Completed step: ${step.name}`, 'INFO');
        } catch (error) {
          log(`Error during ${step.name}: ${error.message}`, 'ERROR');
          throw error; // Re-throw to be caught by the outer try-catch
        }
      } else {
        log(`Skipping ${step.name}: Already completed`, 'DEBUG');
      }
    }

    log('All indexing steps completed successfully.', 'INFO');

    // WebSocket monitoring mode
    setupWebSocket(config.wsAddress, handleMessage, log);
  } catch (error) {
    log(`Error running indexer: ${error.message}`, 'ERROR');
    if (error.stack) {
      log(`Stack trace: ${error.stack}`, 'DEBUG');
    }
  }
}

// WebSocket message handler for real-time updates (WIP)
function handleMessage(message) {
  log(`Received WebSocket message: ${JSON.stringify(message)}`, 'DEBUG');
  // Process the message
}

// Start the indexer
runIndexer().catch(error => {
  log(`Failed to run indexer: ${error.message}`, 'ERROR');
  if (error.stack) {
    log(`Stack trace: ${error.stack}`, 'DEBUG');
  }
});
