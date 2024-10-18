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

// Create necessary tables if they don't exist
async function initializeDatabase(db) {
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
      contract_address TEXT,
      token_id TEXT,
      contract_type TEXT,
      PRIMARY KEY (contract_address, token_id)
    )`,
    `CREATE TABLE IF NOT EXISTS nft_owners (
      collection_address TEXT,
      token_id TEXT,
      owner TEXT,
      contract_type TEXT,
      PRIMARY KEY (collection_address, token_id)
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

  try {
    for (const statement of createTableStatements) {
      await new Promise((resolve, reject) => {
        db.run(statement, (err) => {
          if (err) {
            log(`Error creating table: ${err.message}`, 'ERROR');
            reject(new Error(`Failed to create table: ${statement}`));
          } else {
            log(`Table created/verified: ${statement}`, 'INFO');
            resolve();
          }
        });
      });
    }
    log('All tables initialized successfully.', 'INFO');
  } catch (error) {
    log(`Failed during table initialization: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Main function to run the indexer
async function runIndexer() {
  try {
    await initializeDatabase(db);
    log('Database initialized successfully.', 'INFO');

    // Define the sequence of steps
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

    // Execute each step sequentially
    for (const step of steps) {
      const progress = await checkProgress(db, step.name);
      log(`Progress for ${step.name}: completed=${progress.completed}`, 'DEBUG');
    
      if (!progress.completed) {
        log(`Starting step: ${step.name}`, 'INFO');
        try {
          await step.action();
          await updateProgress(db, step.name, 1);
          log(`Completed step: ${step.name}`, 'INFO');
        } catch (error) {
          log(`Error during step "${step.name}": ${error.message}`, 'ERROR');
          throw error; // Stop execution if any step fails
        }
      } else {
        log(`Skipping step: ${step.name} (already completed)`, 'DEBUG');
      }
    }

    log('All indexing steps completed successfully.', 'INFO');

    // WebSocket monitoring (if applicable)
    setupWebSocket(config.wsAddress, handleMessage, log);
  } catch (error) {
    log(`Failed to run indexer: ${error.message}`, 'ERROR');
    if (error.stack) {
      log(`Stack trace: ${error.stack}`, 'DEBUG');
    }
  }
}

// WebSocket handler
function handleMessage(message) {
  log(`WebSocket message received: ${JSON.stringify(message)}`, 'DEBUG');
}

// Start the indexer
runIndexer().catch((error) => {
  log(`Failed to initialize and run the indexer: ${error.message}`, 'ERROR');
  if (error.stack) {
    log(`Stack trace: ${error.stack}`, 'DEBUG');
  }
});
