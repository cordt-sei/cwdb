// index.js

import { 
  fetchCodeIds, 
  fetchContractAddressesByCodeId,
  fetchContractMetadata,
  fetchContractHistory, 
  identifyContractTypes, 
  fetchTokensAndOwners, 
  fetchPointerData, 
  fetchAssociatedWallets 
} from './contractHelper.js';
import { 
  createWebSocketConnection, 
  log, 
  checkProgress, 
  updateProgress,
  db
} from './utils.js';
import { config } from './config.js';
import fs from 'fs';

// Ensure data and logs directories exist
if (!fs.existsSync('./data')) fs.mkdirSync('./data');
if (!fs.existsSync('./logs')) fs.mkdirSync('./logs');

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

// Main function to run the indexer
async function runIndexer() {
  try {
    log(`Indexer started with log level: ${config.logLevel}`, 'INFO');
    initializeDatabase();
    log('Database initialized successfully.', 'INFO');

    const steps = [
      { name: 'fetchCodeIds', action: () => fetchCodeIds(config.restAddress) },
      { name: 'fetchContractsByCode', action: () => fetchContractAddressesByCodeId(config.restAddress) },
      { name: 'fetchContractMetadata', action: () => fetchContractMetadata(config.restAddress) },
      { name: 'fetchContractHistory', action: () => fetchContractHistory(config.restAddress) },
      { name: 'identifyContractTypes', action: () => identifyContractTypes(config.restAddress) },
      { name: 'fetchTokensAndOwners', action: () => fetchTokensAndOwners(config.restAddress) },
      { name: 'fetchPointerData', action: () => fetchPointerData(config.pointerApi) },
      { name: 'fetchAssociatedWallets', action: () => fetchAssociatedWallets(config.evmRpcAddress) }
    ];

    let allStepsCompleted = true;

    for (const step of steps) {
      let retries = 3;
      let completed = false;

      while (retries > 0 && !completed) {
        const progress = checkProgress(step.name);
        log(`Progress for ${step.name}: completed=${progress.completed}`, 'DEBUG');

        if (!progress.completed) {
          log(`Starting step: ${step.name}`, 'INFO');
          try {
            log(`Executing action for step: ${step.name}`, 'DEBUG');
            await step.action();
            updateProgress(step.name, 1);
            log(`Completed step: ${step.name}`, 'INFO');
            completed = true;
          } catch (error) {
            retries--;
            log(`Error during step "${step.name}": ${error.message}. Retries left: ${retries}`, 'ERROR');
            log(`Stack trace for error in step "${step.name}": ${error.stack}`, 'DEBUG');
            if (retries === 0) {
              allStepsCompleted = false;
              log(`Failed step "${step.name}" after multiple retries.`, 'ERROR');
              break;
            }
          }
        } else {
          completed = true;
          log(`Skipping step: ${step.name} (already completed)`, 'INFO');
        }
      }

      if (!completed) {
        log(`Aborting indexing due to failure in step: ${step.name}`, 'ERROR');
        allStepsCompleted = false;
        break;
      }
    }

    if (allStepsCompleted) {
      log('All indexing steps completed successfully.', 'INFO');
      createWebSocketConnection(config.wsAddress, handleMessage, log);
      log('WebSocket setup initiated after successful indexing.', 'INFO');
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
