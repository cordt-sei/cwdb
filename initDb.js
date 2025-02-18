// databaseInit.js
import { log, db } from './utils.js';

export function initializeDatabase(isTest = false) {
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
      tokens_minted TEXT
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
      token_uri TEXT,
      metadata TEXT,
      PRIMARY KEY (contract_address, token_id)
    )`,
    `CREATE TABLE IF NOT EXISTS cw20_owners (
      contract_address TEXT,
      owner_address TEXT,
      balance TEXT,
      PRIMARY KEY (contract_address, owner_address)
    )`,
    `CREATE TABLE IF NOT EXISTS nft_owners (
      collection_address TEXT,
      token_id TEXT,
      owner TEXT,
      contract_type TEXT,
      PRIMARY KEY (collection_address, token_id, owner)
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

  const progressSteps = [
    'fetchCodeIds',
    'fetchContractsByCode',
    'fetchContractMetadata',
    'fetchContractHistory',
    'identifyContractTypes',
    'fetchTokensAndOwners',
    'fetchPointerData',
    'fetchAssociatedWallets'
  ];

  try {
    const dbInstance = db;
    dbInstance.transaction(() => {
      // Create all tables
      for (const statement of createTableStatements) {
        dbInstance.prepare(statement).run();
        log(`Table created/verified: ${statement}`, 'INFO');
      }

      // For test mode, initialize progress steps
      if (isTest) {
        progressSteps.forEach((step) => {
          dbInstance.prepare(`
            INSERT OR IGNORE INTO indexer_progress (step, completed, last_processed, last_fetched_token)
            VALUES (?, 0, NULL, NULL)
          `).run(step);
        });
        log('Progress steps initialized for test mode', 'INFO');
      }
    })();

    log('Database initialization completed successfully.', 'INFO');
  } catch (error) {
    log(`Failed during database initialization: ${error.message}`, 'ERROR');
    throw error;
  }
}