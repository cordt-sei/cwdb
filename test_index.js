// test_index.js

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
  checkProgress,
  updateProgress,
  db
} from './utils.js';
import { config } from './config.js';
import fs from 'fs';
import axios from 'axios';

// Ensure data and logs directories exist
if (!fs.existsSync('./data')) fs.mkdirSync('./data');
if (!fs.existsSync('./logs')) fs.mkdirSync('./logs');

// Helper function to fetch code_id for a specific contract
async function fetchCodeIdForContract(contractAddress) {
  const url = `${config.restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}`;
  try {
    const response = await axios.get(url);
    if (!response.data?.contract_info?.code_id) {
      throw new Error(`Failed to fetch code_id for contract ${contractAddress}`);
    }
    return response.data.contract_info.code_id;
  } catch (error) {
    log(`Error fetching code_id for ${contractAddress}: ${error.message}`, 'ERROR');
    throw error;
  }
}

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
      for (const statement of createTableStatements) {
        dbInstance.prepare(statement).run();
        log(`Table created/verified: ${statement}`, 'INFO');
      }

      // Insert default progress rows
      progressSteps.forEach((step) => {
        dbInstance.prepare(`
          INSERT OR IGNORE INTO indexer_progress (step, completed, last_processed, last_fetched_token)
          VALUES (?, 0, NULL, NULL)
        `).run(step);
      });
    })();
    log('All tables initialized successfully.', 'INFO');
  } catch (error) {
    log(`Failed during table initialization: ${error.message}`, 'ERROR');
    throw error;
  }
}

async function seedContracts() {
  const contracts = [
    "sei1pkteljh83a83gmazcvam474f7dwt9wzcyqcf5puxvqqs6jcx8nnq2y74lu",
    "sei1g2a0q3tddzs7vf7lk45c2tgufsaqerxmsdr2cprth3mjtuqxm60qdmravc",
    "sei13zrt6ts27fd7rl3hfha7t63udghm0d904ds68h5wsvkkx5md9jqqkd7z5j"
  ];

  try {
    log('Starting contract seeding process...', 'INFO');

    for (const contractAddress of contracts) {
      const codeId = await fetchCodeIdForContract(contractAddress);
      
      db.transaction(() => {
        // Insert into code_ids table
        db.prepare(`
          INSERT OR IGNORE INTO code_ids (code_id, creator, instantiate_permission)
          VALUES (?, ?, ?)
        `).run(codeId, 'manual_seed', '{"permission":"Everybody","address":""}');
        log(`Inserted code_id ${codeId} for contract ${contractAddress}`, 'INFO');

        // Insert into contracts table
        db.prepare(`
          INSERT OR IGNORE INTO contracts (code_id, address, type, creator, admin, label, tokens_minted)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(codeId, contractAddress, null, 'manual_creator', null, 'manually_seeded', null);
        log(`Inserted contract ${contractAddress} with code_id ${codeId}`, 'INFO');
      })();
    }

    // Mark initial steps as completed
    db.transaction(() => {
      updateProgress('fetchCodeIds', 1);
      updateProgress('fetchContractsByCode', 1);
    })();

    log('Contract seeding completed successfully.', 'INFO');
  } catch (error) {
    log(`Error during contract seeding: ${error.message}`, 'ERROR');
    throw error;
  }
}

async function runTestIndexer() {
  try {
    log('Test Indexer started with log level: DEBUG', 'INFO');
    
    const steps = [
      { name: 'fetchContractMetadata', action: () => fetchContractMetadata(config.restAddress) },
      { name: 'fetchContractHistory', action: () => fetchContractHistory(config.restAddress) },
      { name: 'identifyContractTypes', action: () => identifyContractTypes(config.restAddress) },
      { name: 'fetchTokensAndOwners', action: () => fetchTokensAndOwners(config.restAddress) },
      { name: 'fetchPointerData', action: () => fetchPointerData(config.pointerApi) },
      { name: 'fetchAssociatedWallets', action: () => fetchAssociatedWallets(config.evmRpcAddress) }
    ];

    let batchProgressUpdates = [];

    for (const step of steps) {
      const progress = checkProgress(step.name);
      
      if (!progress.completed) {
        try {
          log(`Starting test step: ${step.name}`, 'INFO');
          await step.action();
          batchProgressUpdates.push({ step: step.name, completed: 1 });
          log(`Test step ${step.name} completed successfully.`, 'INFO');
        } catch (error) {
          log(`Test step ${step.name} failed: ${error.message}`, 'ERROR');
          if (error.stack) {
            log(`Stack trace: ${error.stack}`, 'DEBUG');
          }
          throw error;
        }
      } else {
        log(`Skipping test step: ${step.name} (already completed)`, 'INFO');
      }
    }

    // Update progress for completed steps
    batchProgressUpdates.forEach(({ step, completed }) => {
      updateProgress(step, completed);
    });

    log('All test steps completed successfully.', 'INFO');
  } catch (error) {
    log(`Test failed: ${error.message}`, 'ERROR');
    if (error.stack) {
      log(`Stack trace: ${error.stack}`, 'DEBUG');
    }
    throw error;
  }
}

// Handle command line arguments
if (process.argv.includes('--init-db')) {
  initializeDatabase();
  console.log('Database initialization complete.');
  process.exit(0);
}

if (process.argv.includes('--seed-contracts')) {
  initializeDatabase();
  seedContracts()
    .then(() => {
      console.log('Contract seeding complete.');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Failed to seed contracts:', error);
      process.exit(1);
    });
} else {
  // Default: initialize DB, seed contracts, and run the indexer
  initializeDatabase();
  seedContracts()
    .then(() => runTestIndexer())
    .catch((error) => {
      log(`Test run failed: ${error.message}`, 'ERROR');
      process.exit(1);
    });
}