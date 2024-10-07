const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');
const fs = require('fs');
const { isCW721Contract, queryNFTOwners } = require('./cw721Helper'); // Import the helper module

const RPC_WEBSOCKET_URL = 'wss://rpc.sei-apis.com/websocket';
const RPC_REST_URL = 'http://tasty.seipex.fi:1317'; // Add your RPC REST URL for querying contract info
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

// Process instantiate contract event
async function processInstantiateEvent(event) {
  const contract = {
    address: event._contract_address,
    code_id: parseInt(event.code_id)
  };
  
  log(`Processing new contract: ${contract.address}`);
  
  // Check if the contract is CW721
  const isCW721 = await isCW721Contract(RPC_REST_URL, contract.address);
  if (isCW721) {
    log(`Contract ${contract.address} is a CW721 contract. Querying NFT owners...`);
    await queryNFTOwners(RPC_REST_URL, contract.address, db);
  } else {
    log(`Contract ${contract.address} is not a CW721 contract.`);
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
    setupWebSocket();
  } catch (error) {
    log(`Error in main: ${error.message}`);
    process.exit(1);
  }
}

// Run the main function
main();
