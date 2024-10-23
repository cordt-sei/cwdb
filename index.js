// index.js

import { 
  fetchCodeIds, 
  fetchContractAddressesByCodeId,
  fetchContractMetadata,
  fetchContractHistory, 
  identifyContractTypes, 
  processTokensAndOwners, 
  fetchPointerData, 
  fetchAssociatedWallets 
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
      type TEXT,
      creator TEXT,
      admin TEXT,
      label TEXT,
      token_ids TEXT
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
      pointee_address TEXT,
      is_base_asset INTEGER,
      is_pointer INTEGER,
      pointer_type TEXT
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

    // Define the sequence of steps with retries
    const steps = [
      { name: 'fetchCodeIds', action: () => fetchCodeIds(config.restAddress, db) },
      { name: 'fetchContractAddressesByCode', action: () => fetchContractAddressesByCodeId(config.restAddress, db) },
      { name: 'fetchContractMetadata', action: () => fetchContractMetadata(config.restAddress, db) },
      { name: 'fetchContractHistory', action: () => fetchContractHistory(config.restAddress, db) },
      { name: 'identifyContractTypes', action: () => identifyContractTypes(config.restAddress, db) },
      { name: 'processTokensAndOwners', action: () => processTokensAndOwners(config.restAddress, db) },
      { name: 'fetchPointerData', action: () => fetchPointerData(config.pointerApi, db) },
      { name: 'fetchAssociatedWallets', action: () => fetchAssociatedWallets(config.evmRpcAddress, db) }
    ];

    let allStepsCompleted = true;

    // Execute each step sequentially
    for (const step of steps) {
      let retries = 3; // Allow for 3 retry attempts
      let completed = false;

      while (retries > 0 && !completed) {
        const progress = await checkProgress(db, step.name);
        log(`Progress for ${step.name}: completed=${progress.completed}`, 'DEBUG');

        if (!progress.completed) {
          log(`Starting step: ${step.name}`, 'INFO');
          try {
            await step.action();
            await updateProgress(db, step.name, 1);
            log(`Completed step: ${step.name}`, 'INFO');
            completed = true; // Mark as completed to exit the retry loop
          } catch (error) {
            retries--;
            log(`Error during step "${step.name}": ${error.message}. Retries left: ${retries}`, 'ERROR');
            if (retries === 0) {
              allStepsCompleted = false;
              log(`Failed step "${step.name}" after multiple retries.`, 'ERROR');
              break; // Exit retry loop after all attempts fail
            }
          }
        } else {
          completed = true;
          log(`Skipping step: ${step.name} (already completed)`, 'DEBUG');
        }
      }

      // If a step ultimately fails, break out of the entire indexing process
      if (!completed) {
        log(`Aborting indexing due to failure in step: ${step.name}`, 'ERROR');
        allStepsCompleted = false;
        break;
      }
    }

    if (allStepsCompleted) {
      log('All indexing steps completed successfully.', 'INFO');
      // WebSocket monitoring (if applicable)
      setupWebSocket(config.wsAddress, handleMessage, log);
    } else {
      log('Not all steps were completed successfully. Skipping WebSocket setup.', 'ERROR');
    }
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
