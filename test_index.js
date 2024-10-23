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
  log, 
  updateProgress,
  promisify
} from './utils.js';
import { config } from './config.js';
import sqlite3 from 'sqlite3';

async function initializeDatabase(db) {
  const dbRun = promisify(db.run).bind(db);

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
      await dbRun(statement);
    }
    log('All tables initialized successfully.', 'INFO');
  } catch (error) {
    log(`Failed during table initialization: ${error.message}`, 'ERROR');
    throw error;
  }
}

async function runTest() {
  const db = new sqlite3.Database('./data/test_indexer.db');

  try {
    // Initialize the database for testing
    await initializeDatabase(db);
    log('Test database initialized successfully.', 'INFO');

    // Temporary test configuration for code_id "100" only
    const testCodeId = "100";
    
    // Update steps to filter by code_id "100"
    const steps = [
      { name: 'fetchCodeIds', action: () => fetchCodeIds(config.restAddress, db, [testCodeId]) },
      { name: 'fetchContractAddressesByCode', action: () => fetchContractAddressesByCodeId(config.restAddress, db, [testCodeId]) },
      { name: 'fetchContractMetadata', action: () => fetchContractMetadata(config.restAddress, db) },
      { name: 'fetchContractHistory', action: () => fetchContractHistory(config.restAddress, db) },
      { name: 'identifyContractTypes', action: () => identifyContractTypes(config.restAddress, db) },
      { name: 'fetchTokensAndOwners', action: () => fetchTokensAndOwners(config.restAddress, db) },
      { name: 'fetchPointerData', action: () => fetchPointerData(config.pointerApi, db) },
      { name: 'fetchAssociatedWallets', action: () => fetchAssociatedWallets(config.evmRpcAddress, db) }
    ];

    // Execute each step sequentially
    for (const step of steps) {
      try {
        log(`Running test step: ${step.name}`, 'INFO');
        await step.action();
        await updateProgress(db, step.name, 1);
        log(`Test step ${step.name} completed successfully.`, 'INFO');
      } catch (error) {
        log(`Test step ${step.name} failed with error: ${error.message}`, 'ERROR');
        throw error; // Stop the test if any step fails
      }
    }

    log('All test steps completed successfully.', 'INFO');
  } catch (error) {
    log(`Test failed with error: ${error.message}`, 'ERROR');
    if (error.stack) {
      log(`Stack trace: ${error.stack}`, 'DEBUG');
    }
  } finally {
    db.close();
    log('Test database closed.', 'INFO');
  }
}

runTest().catch((error) => {
  log(`Test run failed: ${error.message}`, 'ERROR');
});
