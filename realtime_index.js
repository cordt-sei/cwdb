const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const axios = require('axios');
const { 
  RPC_WEBSOCKET_URL, 
  restAddress, 
  API_KEY, 
  DB_PATH, 
  LOG_FILE, 
  BATCH_SIZE, 
  MAX_RETRIES, 
  RETRY_DELAY,
  NUM_WORKERS,
  BLOCK_HEIGHT
} = require('./config');
const {
  handleContract,
  determineContractType,
  fetchAllTokensForContracts,
  fetchTokenOwners,
  batchInsert,
  updatePointerAddresses,
  updateEVMAddresses
} = require('./cw721Helper');

let db;
let logStream;

// Logging setup
function setupLogging() {
  const logDir = path.dirname(LOG_FILE);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
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

function logDetailedProgress(workerId, message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - Worker ${workerId}: ${message}`;
  console.log(logMessage);
  if (logStream && !logStream.destroyed) {
    logStream.write(logMessage + '\n');
  }
}

// Database setup
async function setupDatabase() {
  return new Promise((resolve, reject) => {
    const dbDir = path.dirname(DB_PATH);
    if (!fs.existsSync(dbDir)) {
      fs.mkdirSync(dbDir, { recursive: true });
    }
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

// Create necessary tables
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
      admin TEXT,
      creator TEXT,
      type TEXT DEFAULT 'UNKNOWN',
      pointer_address TEXT,
      pointee_address TEXT,
      FOREIGN KEY (code_id) REFERENCES code_infos (code_id)
    )`,
    `CREATE TABLE IF NOT EXISTS contract_history (
      address TEXT,
      operation TEXT,
      code_id INTEGER,
      msg TEXT,
      FOREIGN KEY (address) REFERENCES contracts (address)
    )`,
    `CREATE TABLE IF NOT EXISTS contract_info (
      address TEXT PRIMARY KEY,
      code_id INTEGER,
      creator TEXT,
      admin TEXT,
      label TEXT,
      ibc_port_id TEXT,
      type TEXT DEFAULT 'UNKNOWN',
      FOREIGN KEY (address) REFERENCES contracts (address)
    )`,
    `CREATE TABLE IF NOT EXISTS nft_owners (
      collection_address TEXT,
      token_id TEXT,
      owner TEXT,
      pointer_address TEXT,
      pointee_address TEXT,
      evm_address TEXT,
      PRIMARY KEY (collection_address, token_id)
    )`,
    `CREATE TABLE IF NOT EXISTS contract_tokens (
      contract_address TEXT PRIMARY KEY,
      admin TEXT,
      creator TEXT,
      contract_type TEXT,
      extra_data TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS indexer_progress (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_processed_code_id INTEGER,
      last_processed_block_height INTEGER
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

// Function to get the last processed state
async function getLastProcessedState() {
  const result = await promisify(db.get).bind(db)('SELECT * FROM indexer_progress WHERE id = 1');
  return result || { last_processed_code_id: 0, last_processed_block_height: 0 };
}

// Function to update the last processed state
async function updateLastProcessedState(codeId, blockHeight) {
  await promisify(db.run).bind(db)(
    'INSERT OR REPLACE INTO indexer_progress (id, last_processed_code_id, last_processed_block_height) VALUES (1, ?, ?)',
    [codeId, blockHeight]
  );
}

// Create an axios instance with the API key and block height header
function createApiInstance() {
  const headers = { 'x-api-key': API_KEY };
  if (BLOCK_HEIGHT !== null) {
    headers['x-cosmos-block-height'] = BLOCK_HEIGHT.toString();
  }
  return axios.create({
    baseURL: restAddress,
    headers: headers
  });
}

// Retry operation with exponential backoff
async function retryOperation(operation, maxRetries = MAX_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      log(`Attempt ${i + 1} failed: ${error.message}`);
      if (i === maxRetries - 1) {
        log('Max retries reached. Operation failed.');
        throw error;
      }
      log(`Retrying operation in ${RETRY_DELAY * Math.pow(2, i)} ms...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, i)));
    }
  }
}

// Worker function to process code infos and contracts
async function workerFunction(workerId, codeInfos, startBlockHeight) {
  const api = createApiInstance();
  const contractsBatch = [];
  const contractHistoryBatch = [];
  const contractInfoBatch = [];

  for (const [codeInfoIndex, codeInfo] of codeInfos.entries()) {
    try {
      log(`Worker ${workerId}: Processing code info ${codeInfoIndex + 1}/${codeInfos.length} (ID: ${codeInfo.code_id})`);

      let nextKey = null;
      let totalContracts = 0;
      do {
        const response = await retryOperation(() => api.get(`/cosmwasm/wasm/v1/code/${codeInfo.code_id}/contracts`, {
          params: { 'pagination.key': nextKey }
        }));
        const data = response.data;
        log(`Worker ${workerId}: Received ${data.contracts.length} contracts for code ID ${codeInfo.code_id}. Total so far: ${totalContracts + data.contracts.length}`);

        for (const [contractIndex, contractAddress] of data.contracts.entries()) {
          totalContracts++;
          log(`Worker ${workerId}: Processing contract ${contractIndex + 1}/${data.contracts.length} (Total: ${totalContracts}) for code ID ${codeInfo.code_id}`);

          const existingContract = await promisify(db.get).bind(db)('SELECT * FROM contracts WHERE address = ?', [contractAddress]);
          if (existingContract) {
            log(`Worker ${workerId}: Contract ${contractAddress} already exists. Skipping.`);
            continue;
          }

          const contractType = await determineContractType(restAddress, contractAddress, BLOCK_HEIGHT);
          contractsBatch.push({ address: contractAddress, code_id: codeInfo.code_id, type: contractType });

          const historyResponse = await retryOperation(() => api.get(`/cosmwasm/wasm/v1/contract/${contractAddress}/history`));
          for (const entry of historyResponse.data.entries) {
            contractHistoryBatch.push({
              address: contractAddress,
              operation: entry.operation,
              code_id: entry.code_id,
              msg: entry.msg
            });
          }

          const infoResponse = await retryOperation(() => api.get(`/cosmwasm/wasm/v1/contract/${contractAddress}`));
          const contractInfo = infoResponse.data.contract_info;
          contractInfoBatch.push({
            address: contractAddress,
            code_id: contractInfo.code_id,
            creator: contractInfo.creator,
            admin: contractInfo.admin,
            label: contractInfo.label,
            ibc_port_id: contractInfo.ibc_port_id,
            type: contractType
          });

          if (['CW721', 'CW1155', 'CW404'].includes(contractType)) {
            await handleContract(restAddress, contractAddress, db, BLOCK_HEIGHT);
          }

          if (contractsBatch.length >= BATCH_SIZE) {
            await batchInsert(db, 'contracts', contractsBatch);
            contractsBatch.length = 0;
          }
          if (contractHistoryBatch.length >= BATCH_SIZE) {
            await batchInsert(db, 'contract_history', contractHistoryBatch);
            contractHistoryBatch.length = 0;
          }
          if (contractInfoBatch.length >= BATCH_SIZE) {
            await batchInsert(db, 'contract_info', contractInfoBatch);
            contractInfoBatch.length = 0;
          }

          await updateLastProcessedState(codeInfo.code_id, startBlockHeight);
        }

        nextKey = data.pagination.next_key;
        log(`Worker ${workerId}: Processed ${totalContracts} contracts for code ID ${codeInfo.code_id} so far. Next key: ${nextKey || 'None'}`);
      } while (nextKey);

      log(`Worker ${workerId}: Finished processing all contracts for code ID ${codeInfo.code_id}. Total processed: ${totalContracts}`);
    } catch (error) {
      log(`Worker ${workerId}: Error processing code info ${codeInfo.code_id}: ${error.message}`);
      if (error.response) {
        log(`Worker ${workerId}: Response status: ${error.response.status}`);
        log(`Worker ${workerId}: Response data: ${JSON.stringify(error.response.data)}`);
      }
    }
  }

  if (contractsBatch.length > 0) await batchInsert(db, 'contracts', contractsBatch);
  if (contractHistoryBatch.length > 0) await batchInsert(db, 'contract_history', contractHistoryBatch);
  if (contractInfoBatch.length > 0) await batchInsert(db, 'contract_info', contractInfoBatch);

  parentPort.postMessage('done');
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
          await processNewContract(instantiateEvent);
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

// Process a newly detected contract
async function processNewContract(instantiateEvent) {
  const contractAddress = instantiateEvent._contract_address;
  const contractType = await determineContractType(restAddress, contractAddress, BLOCK_HEIGHT);

  await batchInsert(db, 'contracts', [{
    address: contractAddress,
    code_id: instantiateEvent.code_id,
    admin: instantiateEvent.admin,
    creator: instantiateEvent.sender,
    type: contractType
  }]);

  await batchInsert(db, 'contract_info', [{
    address: contractAddress,
    code_id: instantiateEvent.code_id,
    creator: instantiateEvent.sender,
    admin: instantiateEvent.admin,
    label: instantiateEvent.label,
    type: contractType
  }]);

  if (['CW721', 'CW1155', 'CW404'].includes(contractType)) {
    await handleContract(restAddress, contractAddress, db, BLOCK_HEIGHT);
  }

  await updatePointerAddresses(db, [contractAddress]);
}

async function main() {
  try {
    setupLogging();
    await setupDatabase();
    await createTables();

    log(`Using block height: ${BLOCK_HEIGHT === null ? 'latest' : BLOCK_HEIGHT}`);

    const api = createApiInstance();

    const { last_processed_code_id, last_processed_block_height } = await getLastProcessedState();
    log(`Resuming from Code ID: ${last_processed_code_id}, Block Height: ${last_processed_block_height}`);

    const latestBlockResponse = await retryOperation(() => api.get('/cosmos/base/tendermint/v1beta1/blocks/latest'));
    const currentBlockHeight = parseInt(latestBlockResponse.data.block.header.height);

    // Fetch all code IDs
    const codeInfos = [];
    let nextKey = null;
    do {
      const response = await retryOperation(() => api.get('/cosmwasm/wasm/v1/code', {
        params: {
          'pagination.key': nextKey,
          'pagination.reverse': false,
          'pagination.limit': 100
        }
      }));
      const data = response.data;
      const newCodeInfos = data.code_infos
        .filter(info => parseInt(info.code_id) > last_processed_code_id)
        .map(info => ({
          code_id: info.code_id,
          creator: info.creator,
          data_hash: info.data_hash,
          instantiate_permission: JSON.stringify(info.instantiate_permission)
        }));
      codeInfos.push(...newCodeInfos);
      nextKey = data.pagination.next_key;
      log(`Fetched ${codeInfos.length} new code infos so far. Next key: ${nextKey || 'None'}`);
    } while (nextKey);

    log(`Fetched a total of ${codeInfos.length} new code infos. Starting processing.`);

    // Insert code infos into the database
    for (let i = 0; i < codeInfos.length; i += BATCH_SIZE) {
      const batch = codeInfos.slice(i, i + BATCH_SIZE);
      await batchInsert(db, 'code_infos', batch);
    }

    // Process contracts using workers
    const chunkSize = Math.ceil(codeInfos.length / NUM_WORKERS);
    const workers = [];

    for (let i = 0; i < NUM_WORKERS; i++) {
      const start = i * chunkSize;
      const end = start + chunkSize;
      const worker = new Worker(__filename, {
        workerData: {
          workerId: i,
          codeInfos: codeInfos.slice(start, end),
          startBlockHeight: currentBlockHeight
        }
      });

      worker.on('error', (err) => {
        log(`Worker ${i} encountered an error: ${err.message}`);
        worker.terminate();
        log(`Worker ${i} terminated. Restarting...`);
        
        const retryWorker = new Worker(__filename, {
          workerData: {
            workerId: i,
            codeInfos: codeInfos.slice(start, end),
            startBlockHeight: currentBlockHeight
          }
        });

        retryWorker.on('message', resolve);
      });

      workers.push(worker);
    }

    await Promise.all(workers.map(worker => new Promise((resolve) => {
      worker.on('message', resolve);
    })));

    // Update pointer addresses and EVM addresses
    await updatePointerAddresses(db);
    await updateEVMAddresses(db);

    if (codeInfos.length > 0) {
      const lastCodeId = codeInfos[codeInfos.length - 1].code_id;
      await updateLastProcessedState(lastCodeId, currentBlockHeight);
    }

    log('Data collection completed successfully.');

    if (BLOCK_HEIGHT === null) {
      setupWebSocket();
    } else {
      log('Specific block height provided. Skipping WebSocket setup.');
    }
  } catch (error) {
    log(`An unexpected error occurred: ${error.message}`);
    log(error.stack);
  } finally {
    if (db) {
      try {
        await promisify(db.run).bind(db)('PRAGMA optimize');
        await new Promise((resolve, reject) => {
          db.close((err) => {
            if (err) reject(err);
            else resolve();
          });
        });
        log('Database connection closed.');
      } catch (closeError) {
        log(`Error closing database: ${closeError.message}`);
      }
    }
    if (logStream) {
      logStream.end();
    }
  }
}

if (isMainThread) {
  main().catch(error => {
    console.error(`Unhandled error in main: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  });
} else {
  workerFunction(workerData.workerId, workerData.codeInfos, workerData.startBlockHeight)
    .catch(error => {
      console.error(`Unhandled error in worker: ${error.message}`);
      console.error(error.stack);
      process.exit(1);
    });
}