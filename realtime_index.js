const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');
const fs = require('fs');

const RPC_WEBSOCKET_URL = 'wss://rpc.sei-apis.com/websocket';
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

// Create tables if they don't exist
async function createTables() {
  const tables = [
    `CREATE TABLE IF NOT EXISTS code_infos (
      code_id INTEGER PRIMARY KEY,
      creator TEXT,
      data_hash TEXT,
      instantiate_permission TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS contracts (
      address TEXT PRIMARY KEY,
      code_id INTEGER,
      FOREIGN KEY (code_id) REFERENCES code_infos (code_id)
    )`,
    `CREATE TABLE IF NOT EXISTS contract_info (
      address TEXT PRIMARY KEY,
      code_id INTEGER,
      creator TEXT,
      admin TEXT,
      label TEXT,
      ibc_port_id TEXT,
      FOREIGN KEY (address) REFERENCES contracts (address)
    )`
  ];

  for (const table of tables) {
    await promisify(db.run).bind(db)(table);
  }
  log('Tables created successfully.');
}

// Insert or update code info
async function upsertCodeInfo(codeInfo) {
  const sql = `
    INSERT OR REPLACE INTO code_infos (code_id, creator, data_hash, instantiate_permission)
    VALUES (?, ?, ?, ?)
  `;
  await promisify(db.run).bind(db)(sql, [
    codeInfo.code_id,
    codeInfo.creator,
    codeInfo.data_hash,
    JSON.stringify(codeInfo.instantiate_permission)
  ]);
  log(`Upserted code info for code_id: ${codeInfo.code_id}`);
}

// Insert or update contract
async function upsertContract(contract) {
  const sql = `
    INSERT OR REPLACE INTO contracts (address, code_id)
    VALUES (?, ?)
  `;
  await promisify(db.run).bind(db)(sql, [contract.address, contract.code_id]);
  log(`Upserted contract: ${contract.address}`);
}

// Insert or update contract info
async function upsertContractInfo(contractInfo) {
  const sql = `
    INSERT OR REPLACE INTO contract_info (address, code_id, creator, admin, label, ibc_port_id)
    VALUES (?, ?, ?, ?, ?, ?)
  `;
  await promisify(db.run).bind(db)(sql, [
    contractInfo.address,
    contractInfo.code_id,
    contractInfo.creator,
    contractInfo.admin,
    contractInfo.label,
    contractInfo.ibc_port_id
  ]);
  log(`Upserted contract info for address: ${contractInfo.address}`);
}

// Process store code event
async function processStoreCodeEvent(event) {
  const codeInfo = {
    code_id: parseInt(event.code_id),
    creator: event.sender,
    data_hash: event.code_hash,
    instantiate_permission: { permission: 'Everybody', address: '' } // Default permission, adjust as needed
  };
  await upsertCodeInfo(codeInfo);
}

// Process instantiate contract event
async function processInstantiateEvent(event) {
  const contract = {
    address: event._contract_address,
    code_id: parseInt(event.code_id)
  };
  await upsertContract(contract);

  const contractInfo = {
    address: event._contract_address,
    code_id: parseInt(event.code_id),
    creator: event.sender,
    admin: event.admin || '',
    label: event.label || '',
    ibc_port_id: '' // Add this if available in the event
  };
  await upsertContractInfo(contractInfo);
}

// WebSocket connection and subscription
function setupWebSocket() {
  const ws = new WebSocket(RPC_WEBSOCKET_URL);

  ws.on('open', () => {
    log('Connected to WebSocket');
    
    // Subscribe to store code events
    ws.send(JSON.stringify({
      jsonrpc: '2.0',
      method: 'subscribe',
      id: 1,
      params: {
        query: "tm.event='Tx' AND store_code.code_id EXISTS"
      }
    }));

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
        
        if (events['store_code.code_id']) {
          const storeCodeEvent = {
            code_id: events['store_code.code_id'][0],
            sender: events['message.sender'][0],
            code_hash: events['store_code.code_hash'][0]
          };
          await processStoreCodeEvent(storeCodeEvent);
        }

        if (events['instantiate._contract_address']) {
          const instantiateEvent = {
            _contract_address: events['instantiate._contract_address'][0],
            code_id: events['instantiate.code_id'][0],
            sender: events['message.sender'][0],
            admin: events['instantiate.admin'] ? events['instantiate.admin'][0] : '',
            label: events['instantiate.label'] ? events['instantiate.label'][0] : ''
          };
          await processInstantiateEvent(instantiateEvent);
        }
      }
    } catch (error) {
      log(`Error processing message: ${error.message}`);
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

// Main function
async function main() {
  try {
    setupLogging();
    await setupDatabase();
    await createTables();
    setupWebSocket();
  } catch (error) {
    log(`Error in main: ${error.message}`);
    process.exit(1);
  }
}

// Run the main function
main();