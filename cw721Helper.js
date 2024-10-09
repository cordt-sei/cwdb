import fetch from 'node-fetch';
import { Buffer } from 'buffer';
import { promisify } from 'util';
import { 
  POINTER_API_URL, 
  SEIEVM_API_URL, 
  restAddress, 
  BLOCK_HEIGHT 
} from './config.js';


// Helper function to log messages
function log(message) {
  const timestamp = new Date().toISOString();
  console.log(`${timestamp} - ${message}`);
}

// Helper function to send a smart contract query
async function sendContractQuery(rpcEndpoint, contractAddress, payload, blockHeight = null) {
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const url = `${rpcEndpoint}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${payloadBase64}`;

  const headers = {};
  if (blockHeight !== null) {
    headers['x-cosmos-block-height'] = blockHeight.toString();
  }

  try {
    const response = await fetch(url, { headers });
    const data = await response.json();
    return { data, status: response.status };
  } catch (error) {
    log(`Error querying contract ${contractAddress}: ${error.message}`);
    return { error: error.message, status: 500 };
  }
}

// Function to determine the contract type
async function determineContractType(rpcEndpoint, contractAddress, blockHeight = null) {
  log(`Determining contract type for ${contractAddress}...`);
  const testPayload = { a: "b" }; // Example invalid payload
  const { error, status } = await sendContractQuery(rpcEndpoint, contractAddress, testPayload, blockHeight);

  if (status === 400 && error && error.message) {
    const errorMessage = error.message.toLowerCase();

    if (errorMessage.includes('cw721')) {
      log(`Contract ${contractAddress} determined to be CW721`);
      return 'CW721';
    } else if (errorMessage.includes('cw20')) {
      log(`Contract ${contractAddress} determined to be CW20`);
      return 'CW20';
    } else if (errorMessage.includes('cw1155')) {
      log(`Contract ${contractAddress} determined to be CW1155`);
      return 'CW1155';
    } else if (errorMessage.includes('cw404')) {
      log(`Contract ${contractAddress} determined to be CW404`);
      return 'CW404';
    }
  }

  log(`Could not determine the contract type for ${contractAddress}.`);
  return 'UNKNOWN';
}

// Function to query all tokens for a CW721 contract
async function queryAllTokens(rpcEndpoint, contractAddress, blockHeight = null) {
  log(`Querying tokens for CW721 contract ${contractAddress}...`);

  const tokenQueryPayload = { all_tokens: {} };
  const { data, error, status } = await sendContractQuery(rpcEndpoint, contractAddress, tokenQueryPayload, blockHeight);

  if (status === 200 && data && data.data && Array.isArray(data.data.tokens)) {
    log(`Found tokens for ${contractAddress}: ${data.data.tokens}`);
    return data.data.tokens;
  } else {
    log(`No tokens found for contract ${contractAddress}. Status: ${status}, Error: ${error || 'No data returned'}`);
    return [];
  }
}

// Function to query the owner of each token
async function queryTokenOwner(rpcEndpoint, contractAddress, token_id, blockHeight = null) {
  log(`Querying owner for token ID: ${token_id} in contract: ${contractAddress}`);

  const ownerQueryPayload = { owner_of: { token_id: token_id.toString() } };
  
  const { data, error, status } = await sendContractQuery(rpcEndpoint, contractAddress, ownerQueryPayload, blockHeight);

  if (status === 200 && data && data.data && data.data.owner) {
    log(`Token ID: ${token_id}, Owner: ${data.data.owner}`);
    return data.data.owner;
  } else {
    log(`No owner found for token ID: ${token_id}. Status: ${status}, Error: ${error || 'No data returned'}`);
    return null;
  }
}

// Stage 1: Fetch all token IDs for all CW721 contracts and record them in batches
async function fetchAllTokensForContracts(rpcEndpoint, db, batchSize = 50, blockHeight = null) {
  const sql = "SELECT address FROM contracts WHERE type = 'CW721'";
  const cw721Contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of cw721Contracts) {
    const contractAddress = contract.address;
    const tokens = await queryAllTokens(rpcEndpoint, contractAddress, blockHeight);

    for (let i = 0; i < tokens.length; i += batchSize) {
      const tokenBatch = tokens.slice(i, i + batchSize);
      const tokenIdsStr = tokenBatch.join(',');

      const insertContractSQL = `INSERT OR REPLACE INTO contract_tokens (contract_address, extra_data) VALUES (?, ?)`;
      await retryDatabaseOperation(() => promisify(db.run).bind(db)(insertContractSQL, [contractAddress, tokenIdsStr]));

      log(`Stored tokens for contract ${contractAddress}. Batch size: ${tokenBatch.length}`);
    }
  }
}

// Stage 2: Fetch and store owner information for each token in batches
async function fetchTokenOwners(rpcEndpoint, db, batchSize = 50, blockHeight = null) {
  const sql = "SELECT contract_address, extra_data FROM contract_tokens";
  const contractTokens = await promisify(db.all).bind(db)(sql);

  for (const contractToken of contractTokens) {
    const contractAddress = contractToken.contract_address;
    const tokenIds = contractToken.extra_data.split(',');

    for (let i = 0; i < tokenIds.length; i += batchSize) {
      const tokenBatch = tokenIds.slice(i, i + batchSize);

      for (const token_id of tokenBatch) {
        const owner = await queryTokenOwner(rpcEndpoint, contractAddress, token_id, blockHeight);

        if (owner) {
          const insertOwnerSQL = `INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner) VALUES (?, ?, ?)`;
          await retryDatabaseOperation(() => promisify(db.run).bind(db)(insertOwnerSQL, [contractAddress, token_id, owner]));

          log(`Recorded ownership: Token ${token_id} owned by ${owner}`);
        }
      }

      log(`Processed owner info for contract ${contractAddress}. Batch size: ${tokenBatch.length}`);
    }
  }
}

// Main function to handle CW404, CW721, and CW1155 contracts
async function handleContract(rpcEndpoint, contractAddress, db, blockHeight = null) {
  log(`Handling contract ${contractAddress}...`);

  // First, determine the contract type (CW721, CW1155, or CW404)
  const contractType = await determineContractType(rpcEndpoint, contractAddress, blockHeight);

  if (contractType === 'CW404') {
    log(`Contract ${contractAddress} is a CW404 type. No tokens or ownership records to process.`);
    return;
  }

  // Fetch all tokens based on contract type
  const tokens = await queryAllTokens(rpcEndpoint, contractAddress, blockHeight);

  if (Array.isArray(tokens) && tokens.length > 0) {
    const tokenIdsStr = tokens.join(',');

    // Insert contract and tokens into the database
    const insertContractSQL = `INSERT OR REPLACE INTO contract_tokens (contract_address, contract_type, extra_data) VALUES (?, ?, ?)`;
    await promisify(db.run).bind(db)(insertContractSQL, [contractAddress, contractType, tokenIdsStr]);

    log(`Inserted contract tokens for ${contractAddress} of type ${contractType}.`);

    // For each token, query the owner and insert it into the database
    for (const token_id of tokens) {
      const owner = await queryTokenOwner(rpcEndpoint, contractAddress, token_id, blockHeight);

      if (owner) {
        const insertOwnerSQL = `INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner) VALUES (?, ?, ?)`;
        await promisify(db.run).bind(db)(insertOwnerSQL, [contractAddress, token_id, owner]);

        log(`Recorded ownership: Token ${token_id} owned by ${owner}`);
      } else {
        log(`No owner found for token ${token_id} in contract ${contractAddress}.`);
      }
    }
  } else {
    log(`No tokens found or an error occurred for contract ${contractAddress}.`);
  }
}

// Helper function to retry SQLite operations in case of database locking errors
async function retryDatabaseOperation(operation, maxRetries = 3, retryDelay = 500) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (err) {
      if (err.message.includes('SQLITE_BUSY') && i < maxRetries - 1) {
        log(`Database locked, retrying... (${i + 1}/${maxRetries})`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      } else {
        throw err;
      }
    }
  }
}

// Updated batchInsert with retry logic and prepared statements
async function batchInsert(db, table, dataArray) {
  if (dataArray.length === 0) return;

  const keys = Object.keys(dataArray[0]);
  const placeholders = `(${keys.map(() => '?').join(',')})`;
  const sql = `INSERT OR REPLACE INTO ${table} (${keys.join(',')}) VALUES ${placeholders}`;

  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');

      const stmt = db.prepare(sql);

      for (const item of dataArray) {
        const values = keys.map(key => item[key]);
        stmt.run(values, (err) => {
          if (err) {
            console.error(`Error inserting row: ${err.message}`);
          }
        });
      }

      stmt.finalize();

      db.run('COMMIT', (err) => {
        if (err) {
          console.error(`Error committing transaction: ${err.message}`);
          db.run('ROLLBACK');
          reject(err);
        } else {
          console.log(`Inserted ${dataArray.length} rows into ${table}`);
          resolve();
        }
      });
    });
  });
}

// Helper function to query pointer addresses
async function queryPointerAddresses(addresses) {
  try {
    const response = await fetch(POINTER_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ addresses })
    });
    const data = await response.json();
    return data;
  } catch (error) {
    log(`Error querying pointer addresses: ${error.message}`);
    return [];
  }
}

// Helper function to query EVM address
async function queryEVMAddress(bech32Address) {
  try {
    const response = await fetch(SEIEVM_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'sei_getEVMAddress',
        params: [bech32Address],
        id: 1
      })
    });
    const data = await response.json();
    return data.result;
  } catch (error) {
    log(`Error querying EVM address for ${bech32Address}: ${error.message}`);
    return null;
  }
}

// New function to update pointer and pointee addresses
async function updatePointerAddresses(db, batchSize = 100) {
  const contractAddresses = await promisify(db.all).bind(db)('SELECT address FROM contracts');
  const addressChunks = chunkArray(contractAddresses.map(c => c.address), batchSize);

  for (const chunk of addressChunks) {
    const pointerData = await queryPointerAddresses(chunk);
    for (const data of pointerData) {
      await promisify(db.run).bind(db)(
        'UPDATE contracts SET pointer_address = ?, pointee_address = ? WHERE address = ?',
        [data.pointerAddress || null, data.pointeeAddress || null, data.address]
      );
      await promisify(db.run).bind(db)(
        'UPDATE nft_owners SET pointer_address = ?, pointee_address = ? WHERE collection_address = ?',
        [data.pointerAddress || null, data.pointeeAddress || null, data.address]
      );
    }
  }
  log('Pointer addresses updated successfully.');
}

// New function to update EVM addresses for NFT owners
async function updateEVMAddresses(db) {
  const owners = await promisify(db.all).bind(db)('SELECT DISTINCT owner FROM nft_owners');
  for (const ownerObj of owners) {
    const evmAddress = await queryEVMAddress(ownerObj.owner);
    if (evmAddress) {
      await promisify(db.run).bind(db)(
        'UPDATE nft_owners SET evm_address = ? WHERE owner = ?',
        [evmAddress, ownerObj.owner]
      );
    }
  }
  log('EVM addresses updated successfully.');
}

// Helper function to chunk array
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// Function to check and fix missing contract types
async function checkAndFixMissingContractTypes(db, rpcEndpoint) {
  log('Checking for missing contract types...');
  const sql = 'SELECT address FROM contracts WHERE type IS NULL OR type = "UNKNOWN"';
  const contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of contracts) {
    const contractType = await determineContractType(rpcEndpoint, contract.address);
    if (contractType) {
      const updateSql = 'UPDATE contracts SET type = ? WHERE address = ?';
      await promisify(db.run).bind(db)(updateSql, [contractType, contract.address]);
      log(`Updated contract ${contract.address} to type ${contractType}`);
    }
  }
}

// Function to check and fix missing tokens
async function checkAndFixMissingTokens(db, rpcEndpoint) {
  log('Checking for missing tokens...');
  const sql = 'SELECT address FROM contracts WHERE type = "CW721"';
  const cw721Contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of cw721Contracts) {
    await handleContract(rpcEndpoint, contract.address, db);
  }
}

// Function to check and fix missing owners
async function checkAndFixMissingOwners(db, rpcEndpoint) {
  log('Checking for missing owners...');
  const sql = 'SELECT collection_address, token_id FROM nft_owners WHERE owner IS NULL';
  const tokens = await promisify(db.all).bind(db)(sql);

  for (const token of tokens) {
    const owner = await queryTokenOwner(rpcEndpoint, token.collection_address, token.token_id);
    if (owner) {
      const updateSql = 'UPDATE nft_owners SET owner = ? WHERE collection_address = ? AND token_id = ?';
      await promisify(db.run).bind(db)(updateSql, [owner, token.collection_address, token.token_id]);
      log(`Updated owner for token ${token.token_id} in contract ${token.collection_address} to ${owner}`);
    }
  }
}

// Export all functions
export {
  handleContract,
  determineContractType,
  fetchAllTokensForContracts,
  fetchTokenOwners,
  checkAndFixMissingContractTypes,
  checkAndFixMissingTokens,
  checkAndFixMissingOwners,
  batchInsert,
  updatePointerAddresses,
  updateEVMAddresses,
  log
};
