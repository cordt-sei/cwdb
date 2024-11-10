// Import necessary functions and modules
import { 
  fetchCodeIds, 
  fetchContractAddressesByCodeId, 
  fetchContractMetadata, 
  fetchContractHistory,  // Import the new fetchContractHistory function
  identifyContractTypes, 
  fetchTokensAndOwners, 
  fetchPointerData, 
  fetchAssociatedWallets 
} from './contractHelper.js';
import { 
  log, 
  updateProgress,
  db
} from './utils.js';

import { config } from './config.js';
import fs from 'fs';

// Ensure data and logs directories exist
if (!fs.existsSync('./data')) fs.mkdirSync('./data');
if (!fs.existsSync('./logs')) fs.mkdirSync('./logs');

// Create necessary tables if they don't exist
function initializeDatabase() {
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
      msg TEXT,
      PRIMARY KEY (contract_address, operation, code_id)
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
    const dbInstance = db; // Using better-sqlite3 instance directly
    dbInstance.transaction(() => {
      for (const statement of createTableStatements) {
        dbInstance.prepare(statement).run();
        log(`Table created/verified: ${statement}`, 'INFO');
      }
    })();
    log('All tables initialized successfully.', 'INFO');
  } catch (error) {
    log(`Failed during table initialization: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Main function to run the test indexer
async function runTest() {
  try {
    log('Test Indexer started with log level: DEBUG', 'INFO');
    initializeDatabase();
    log('Test database initialized successfully.', 'INFO');

    // Define the steps for the test
    const steps = [
      { name: 'fetchCodeIds', action: () => fetchCodeIds(config.restAddress) },
      { name: 'fetchContractAddressesByCode', action: () => fetchContractAddressesByCodeId(config.restAddress) },
      { name: 'fetchContractMetadata', action: () => fetchContractMetadata(config.restAddress) },
      { name: 'fetchContractHistory', action: () => fetchContractHistory(config.restAddress) }, // Added the fetchContractHistory step
      { name: 'identifyContractTypes', action: () => identifyContractTypes(config.restAddress) },
      { name: 'fetchTokensAndOwners', action: () => fetchTokensAndOwners(config.restAddress) },
      { name: 'fetchPointerData', action: () => fetchPointerData(config.pointerApi) },
      { name: 'fetchAssociatedWallets', action: () => fetchAssociatedWallets(config.evmRpcAddress) }
    ];

    // Execute each step sequentially
    for (const step of steps) {
      let retries = 3;
      let completed = false;

      while (retries > 0 && !completed) {
        try {
          log(`Running test step: ${step.name}`, 'INFO');
          await step.action();
          updateProgress(step.name, 1);
          log(`Test step ${step.name} completed successfully.`, 'INFO');
          completed = true;
        } catch (error) {
          retries--;
          log(`Test step ${step.name} failed with error: ${error.message}. Retries left: ${retries}`, 'ERROR');
          if (error.stack) {
            log(`Stack trace: ${error.stack}`, 'DEBUG');
          }
          if (retries === 0) {
            throw error; // Stop the test if retries are exhausted
          }
        }
      }
    }

    log('All test steps completed successfully.', 'INFO');
  } catch (error) {
    log(`Test failed with error: ${error.message}`, 'ERROR');
    if (error.stack) {
      log(`Stack trace: ${error.stack}`, 'DEBUG');
    }
  }
}

// Start the test
runTest().catch((error) => {
  log(`Test run failed: ${error.message}`, 'ERROR');
  if (error.stack) {
    log(`Stack trace: ${error.stack}`, 'DEBUG');
  }
});
