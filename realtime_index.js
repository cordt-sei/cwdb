const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');
const fs = require('fs');
const { handleContract, determineContractType } = require('./cw721Helper');

const RPC_WEBSOCKET_URL = 'wss://rpc.sei-apis.com/websocket';
const restAddress = 'http://tasty.seipex.fi:1317'; // REST endpoint for queries
const DB_PATH = './smart_contracts.db';
const LOG_FILE = 'real_time_indexer.log';

let db;
let logStream;

// Set up logging
function setupLogging() {
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
}

function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${message}\n`;
  console.log(logMessage.trim());
  if (logStream && !logStream.destroyed) {
    logStream.write(logMessage);
  }
}

// Set up database connection
async function setupDatabase() {
  return new Promise((resolve, reject) => {
    db = new sqlite3.Database(DB_PATH, (err) => {
      if (err) {
        log(`Error opening database: ${err.message}`);
        reject(err);
      } else {
        log('Connected to the SQLite database.');
        resolve();
      }
    });
  });
}

// Create necessary tables for contracts, tokens, and owners
async function createTables() {
  const tables = [
    `CREATE TABLE IF NOT EXISTS contracts (
      address TEXT PRIMARY KEY,
      admin TEXT,
      creator TEXT,
      type TEXT DEFAULT 'UNKNOWN'
    )`,
    `CREATE TABLE IF NOT EXISTS contract_info (
      address TEXT PRIMARY KEY,
      type TEXT DEFAULT 'UNKNOWN'
    )`,
    `CREATE TABLE IF NOT EXISTS nft_owners (
      collection_address TEXT,
      token_id TEXT,
      owner TEXT,
      PRIMARY KEY (collection_address, token_id)
    )`,
    `CREATE TABLE IF NOT EXISTS contract_tokens (
      contract_address TEXT PRIMARY KEY,
      admin TEXT,
      creator TEXT,
      contract_type TEXT,
      extra_data TEXT
    )`
  ];

  for (const table of tables) {
    try {
      await promisify(db.run).bind(db)(table);
    } catch (error) {
      log(`Error creating/updating table: ${error.message}`);
    }
  }

  log('Tables created/updated successfully.');
}

// Fetch all contracts without a known type
async function fetchContractsWithoutType() {
  const sql = "SELECT address FROM contracts WHERE type = 'UNKNOWN'";
  const contracts = await promisify(db.all).bind(db)(sql);
  return contracts.map(contract => contract.address);
}

// Update contract type in the database
async function updateContractType(contractAddress, contractType) {
  const sql = "UPDATE contracts SET type = ? WHERE address = ?";
  await promisify(db.run).bind(db)(sql, [contractType, contractAddress]);

  const sqlInfo = "UPDATE contract_info SET type = ? WHERE address = ?";
  await promisify(db.run).bind(db)(sqlInfo, [contractType, contractAddress]);

  log(`Updated contract ${contractAddress} with type ${contractType}`);
}

// Step 1: Retroactively determine contract types using the test query
async function determineContractTypes() {
  log('Determining contract types...');
  const contracts = await fetchContractsWithoutType();

  for (const contractAddress of contracts) {
    log(`Checking contract type for ${contractAddress}...`);
    try {
      const contractType = await determineContractType(restAddress, contractAddress);

      if (contractType) {
        await updateContractType(contractAddress, contractType);
        log(`Contract ${contractAddress} is labeled as ${contractType}`);
      } else {
        log(`Could not determine contract type for ${contractAddress}.`);
      }
    } catch (error) {
      log(`Error determining contract type for ${contractAddress}: ${error.message}`);
    }
  }

  log('Finished determining contract types.');
}

// Step 2: Iterate over all CW721 contracts to query token IDs
async function processCW721Contracts() {
  const sql = "SELECT address FROM contracts WHERE type = 'CW721'";
  const cw721Contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of cw721Contracts) {
    const contractAddress = contract.address;
    log(`Querying tokens for CW721 contract ${contractAddress}...`);

    try {
      const owners = await handleContract(restAddress, contractAddress, db);

      if (owners && owners.length > 0) {
        for (const { token_id, owner } of owners) {
          const insertSQL = `INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner)
            VALUES (?, ?, ?)`;
          await promisify(db.run).bind(db)(insertSQL, [contractAddress, token_id, owner]);
          log(`Recorded ownership: Token ${token_id} owned by ${owner}`);
        }
      }
    } catch (error) {
      log(`Error processing CW721 contract ${contractAddress}: ${error.message}`);
    }
  }
}

// WebSocket connection and subscription
function setupWebSocket() {
  const ws = new WebSocket(RPC_WEBSOCKET_URL);

  ws.on('open', () => {
    log('Connected to WebSocket');
    
    // Subscribe to instantiate contract events
    ws.send(JSON.stringify({
      jsonrpc: '2.0',
      method: 'subscribe',
      id: 2,
      params: {
        query: "tm.event='Tx' AND instantiate._contract_address EXISTS"
      }
    }));
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      if (message.result && message.result.events) {
        const events = message.result.events;

        if (events['instantiate._contract_address']) {
          const instantiateEvent = {
            _contract_address: events['instantiate._contract_address'][0],
            code_id: events['instantiate.code_id'][0],
            sender: events['message.sender'][0],
            admin: events['instantiate.admin'] ? events['instantiate.admin'][0] : '',
            label: events['instantiate.label'] ? events['instantiate.label'][0] : ''
          };
          log(`Detected new contract: ${instantiateEvent._contract_address}`);
          await determineContractTypes(); // Run the contract type detection for the new contract
        }
      }
    } catch (error) {
      log(`Error processing WebSocket message: ${error.message}`);
    }
  });

  ws.on('close', () => {
    log('WebSocket connection closed. Reconnecting...');
    setTimeout(setupWebSocket, 5000);
  });

  ws.on('error', (error) => {
    log(`WebSocket error: ${error.message}`);
  });
}

async function main() {
  try {
    setupLogging();
    await setupDatabase();
    await createTables();

    await determineContractTypes();

    await processCW721Contracts();

    setupWebSocket();
  } catch (error) {
    log(`Error in main: ${error.message}`);
    process.exit(1);
  }
}

main();
