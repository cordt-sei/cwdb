const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const { promisify } = require('util');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

const restAddress = "http://tasty.seipex.fi:1317"
const NUM_WORKERS = 3;
const API_KEY = 'a48f0d74';
const BATCH_SIZE = 100;
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

let logStream;
let db;

// Set up logging
function setupLogging() {
  logStream = fs.createWriteStream('data_collection.log', { flags: 'a' });
}

function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${message}\n`;
  console.log(logMessage.trim());
  if (logStream && !logStream.destroyed) {
    logStream.write(logMessage);
  }
}

// Set up database
async function setupDatabase() {
  return new Promise((resolve, reject) => {
    db = new sqlite3.Database('./smart_contracts.db', (err) => {
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

// Create tables
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
      FOREIGN KEY (address) REFERENCES contracts (address)
    )`,
    `CREATE TABLE IF NOT EXISTS indexer_progress (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_processed_code_id INTEGER,
      last_processed_block_height INTEGER
    )`
  ];

  for (const table of tables) {
    await promisify(db.run).bind(db)(table);
    log(`Table created successfully.`);
  }
}

// Create an axios instance with the API key
function createApiInstance() {
  return axios.create({
    baseURL: restAddress,
    headers: {
      'x-api-key': API_KEY
    }
  });
}

// Helper function to insert data into the database in batches
async function batchInsert(table, dataArray) {
  if (dataArray.length === 0) return;

  const keys = Object.keys(dataArray[0]);
  const placeholders = dataArray.map(() => `(${keys.map(() => '?').join(',')})`).join(',');
  const sql = `INSERT OR REPLACE INTO ${table} (${keys.join(',')}) VALUES ${placeholders}`;
  
  const values = dataArray.flatMap(obj => keys.map(key => obj[key]));
  
  await promisify(db.run).bind(db)(sql, values);
  log(`Inserted ${dataArray.length} rows into ${table}`);
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

// Modified worker function to handle incremental updates
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

          // Check if contract already exists and skip if it does
          const existingContract = await promisify(db.get).bind(db)('SELECT * FROM contracts WHERE address = ?', [contractAddress]);
          if (existingContract) {
            log(`Worker ${workerId}: Contract ${contractAddress} already exists. Skipping.`);
            continue;
          }

          contractsBatch.push({ address: contractAddress, code_id: codeInfo.code_id });

          // Fetch contract history
          const historyResponse = await retryOperation(() => api.get(`/cosmwasm/wasm/v1/contract/${contractAddress}/history`));
          for (const entry of historyResponse.data.entries) {
            contractHistoryBatch.push({
              address: contractAddress,
              operation: entry.operation,
              code_id: entry.code_id,
              msg: entry.msg
            });
          }

          // Fetch contract info
          const infoResponse = await retryOperation(() => api.get(`/cosmwasm/wasm/v1/contract/${contractAddress}`));
          const contractInfo = infoResponse.data.contract_info;
          contractInfoBatch.push({
            address: contractAddress,
            code_id: contractInfo.code_id,
            creator: contractInfo.creator,
            admin: contractInfo.admin,
            label: contractInfo.label,
            ibc_port_id: contractInfo.ibc_port_id
          });

          // Perform batch inserts when batch size reaches BATCH_SIZE
          if (contractsBatch.length >= BATCH_SIZE) {
            await batchInsert('contracts', contractsBatch);
            contractsBatch.length = 0;
          }
          if (contractHistoryBatch.length >= BATCH_SIZE) {
            await batchInsert('contract_history', contractHistoryBatch);
            contractHistoryBatch.length = 0;
          }
          if (contractInfoBatch.length >= BATCH_SIZE) {
            await batchInsert('contract_info', contractInfoBatch);
            contractInfoBatch.length = 0;
          }

          // Update the last processed state after each contract
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

  // Insert any remaining items in batches
  if (contractsBatch.length > 0) await batchInsert('contracts', contractsBatch);
  if (contractHistoryBatch.length > 0) await batchInsert('contract_history', contractHistoryBatch);
  if (contractInfoBatch.length > 0) await batchInsert('contract_info', contractInfoBatch);

  parentPort.postMessage('done');
}

// Retry operation with exponential backoff
async function retryOperation(operation, maxRetries = MAX_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, i)));
    }
  }
}

// Modified main function to handle incremental updates
async function main() {
  if (isMainThread) {
    setupLogging();
    log('Starting data collection process...');
    try {
      await setupDatabase();
      await createTables();

      const api = createApiInstance();

      // Get the last processed state
      const { last_processed_code_id, last_processed_block_height } = await getLastProcessedState();
      log(`Resuming from Code ID: ${last_processed_code_id}, Block Height: ${last_processed_block_height}`);

      // Fetch the current block height
      const latestBlockResponse = await retryOperation(() => api.get('/cosmos/base/tendermint/v1beta1/blocks/latest'));
      const currentBlockHeight = parseInt(latestBlockResponse.data.block.header.height);

      // Fetch all code infos after the last processed code_id
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

      log(`Fetched a total of ${codeInfos.length} new code infos. Starting processing with workers.`);

      // Insert new code infos in batches
      for (let i = 0; i < codeInfos.length; i += BATCH_SIZE) {
        const batch = codeInfos.slice(i, i + BATCH_SIZE);
        await batchInsert('code_infos', batch);
      }

      // Divide work among workers
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
        workers.push(worker);
      }

      // Wait for all workers to complete
      await Promise.all(workers.map(worker => new Promise((resolve) => {
        worker.on('message', resolve);
      })));

      // Update the last processed state after all workers are done
      if (codeInfos.length > 0) {
        const lastCodeId = codeInfos[codeInfos.length - 1].code_id;
        await updateLastProcessedState(lastCodeId, currentBlockHeight);
      }

      log('Data collection completed successfully.');
    } catch (error) {
      log(`An unexpected error occurred: ${error.message}`);
      log(error.stack);
    } finally {
      if (db) {
        await promisify(db.close).bind(db)();
        log('Database connection closed.');
      }
      if (logStream) {
        logStream.end();
      }
    }
  } else {
    // Worker thread code
    await setupDatabase();
    await workerFunction(workerData.workerId, workerData.codeInfos, workerData.startBlockHeight);
  }
}

main().catch(error => {
  console.error(`Unhandled error in main: ${error.message}`);
  console.error(error.stack);
  process.exit(1);
});
