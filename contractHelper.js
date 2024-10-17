// contractHelper.js

import { 
  fetchPaginatedData, 
  sendContractQuery, 
  retryOperation, 
  log,
  batchInsert,
  checkProgress,
  updateProgress,
  promisify
} from './utils.js';
import { Buffer } from 'buffer';
import axios from 'axios';
import { config } from './config.js';

// Fetch all code IDs and store them in the database
export async function fetchAndStoreCodeIds(restAddress, db) {
  const dbRun = promisify(db.run).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchCodeIds');
    if (progress.completed) {
      log('Skipping fetchAndStoreCodeIds: Already completed', 'INFO');
      return;
    }

    let startAfter = progress.last_processed ? parseInt(progress.last_processed) : null;
    let allCodeInfos = [];
    let hasMore = true;

    while (hasMore) {
      const payload = {
        'pagination.reverse': false,
        'pagination.limit': config.paginationLimit
      };

      if (startAfter !== null) {
        payload['pagination.key'] = Buffer.from(startAfter.toString()).toString('base64');
      }

      const codeInfos = await fetchPaginatedData(
        `${restAddress}/cosmwasm/wasm/v1/code`,
        null,
        payload,
        'code_infos'
      );

      if (codeInfos.length === 0) {
        log('No more code IDs to fetch.', 'INFO');
        break;
      }

      // Prepare batch data for inserting code IDs
      const batchData = codeInfos.map(({ code_id, creator }) => [code_id, creator]);

      // Perform batch insert
      await batchInsert(dbRun, 'code_ids', ['code_id', 'creator'], batchData);
      log(`Inserted ${batchData.length} code IDs into the database.`, 'INFO');

      // Update progress
      allCodeInfos = allCodeInfos.concat(codeInfos);
      startAfter = parseInt(codeInfos[codeInfos.length - 1].code_id);
      hasMore = codeInfos.length === config.paginationLimit;

      await updateProgress(db, 'fetchCodeIds', 0, startAfter);
    }

    log(`Total code IDs fetched and stored: ${allCodeInfos.length}`, 'INFO');
    await updateProgress(db, 'fetchCodeIds', 1, null);
  } catch (error) {
    log(`Error in fetchAndStoreCodeIds: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch contracts by code and store them in the database using batch insert
export async function fetchAndStoreContractsByCode(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchContracts');
    if (progress.completed) {
      log('Skipping fetchAndStoreContractsByCode: Already completed', 'INFO');
      return;
    }

    const codeIdsResult = await dbAll('SELECT code_id FROM code_ids');
    const codeIds = codeIdsResult.map(row => row.code_id);

    const startIndex = progress.last_processed ? codeIds.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < codeIds.length; i++) {
      const code_id = codeIds[i];

      // Fetch contracts associated with the code_id using pagination
      const contracts = await fetchPaginatedData(
        `${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts`,
        null,
        { 'pagination.limit': config.paginationLimit }, // Use config for pagination limit
        'contracts'
      );

      if (contracts.length > 0) {
        log(`Fetched ${contracts.length} contracts for code_id: ${code_id}`, 'INFO');

        // Prepare batch data for insertion
        const batchData = contracts.map(contractAddress => [code_id, contractAddress]);

        // Perform batch insert
        await batchInsert(db.run.bind(db), 'contracts', ['code_id', 'address'], batchData);
        log(`Batch inserted ${batchData.length} contracts for code_id: ${code_id}`, 'INFO');
      } else {
        log(`No contracts found for code_id: ${code_id}`, 'INFO');
      }

      // Update progress after processing each code_id
      await updateProgress(db, 'fetchContracts', 0, code_id);
    }

    log(`Finished processing contracts for all ${codeIds.length} code IDs`, 'INFO');
    await updateProgress(db, 'fetchContracts', 1, null);
  } catch (error) {
    log(`Error in fetchAndStoreContractsByCode: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store contract history per contract
export async function fetchAndStoreContractHistory(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchContractHistory');
    if (progress.completed) {
      log('Skipping fetchAndStoreContractHistory: Already completed', 'INFO');
      return;
    }

    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contractAddresses = contractsResult.map(row => row.address);
    const startIndex = progress.last_processed ? contractAddresses.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contractAddresses.length; i++) {
      const address = contractAddresses[i];
      let batchData = [];

      // Fetch history entries using paginated requests
      const historyEntries = await fetchPaginatedData(
        `${restAddress}/cosmwasm/wasm/v1/contract/${address}/history`,
        null,
        { 'pagination.limit': config.paginationLimit },
        'entries'
      );

      if (historyEntries.length > 0) {
        log(`Fetched ${historyEntries.length} history entries for contract: ${address}`, 'INFO');

        // Prepare data for batch insertion
        batchData = historyEntries.map(({ operation, code_id, updated, msg }) => [
          address,
          operation,
          code_id,
          updated || null,
          JSON.stringify(msg)
        ]);

        // Perform batch insert
        await batchInsert(db.run.bind(db), 'contract_history', ['contract_address', 'operation', 'code_id', 'updated', 'msg'], batchData);
        log(`Inserted ${batchData.length} history records for contract: ${address}`, 'INFO');
      } else {
        log(`No history entries found for contract: ${address}`, 'INFO');
      }

      // Update progress after processing each contract
      await updateProgress(db, 'fetchContractHistory', 0, address);
    }

    log(`Finished processing history for all ${contractAddresses.length} contracts`, 'INFO');
    await updateProgress(db, 'fetchContractHistory', 1, null);
  } catch (error) {
    log(`Error in fetchAndStoreContractHistory: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Identify contract types and store them in the database using batch processing
export async function identifyContractTypes(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);

  const batchSize = 50; // Set a batch size for DB writes
  let batchData = []; // Hold the data to be written in a batch

  try {
    const progress = await checkProgress(db, 'identifyContractTypes');
    if (progress.completed) {
      log('Skipping identifyContractTypes: Already completed', 'INFO');
      return;
    }

    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contracts = contractsResult.map(row => row.address);
    const startIndex = progress.last_processed ? contracts.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contracts.length; i++) {
      const contractAddress = contracts[i];

      // Determine the contract type using a test payload
      const testPayload = { "a": "b" }; // Example invalid payload
      let contractType = 'other'; // Default type if no type is identified

      try {
        const response = await sendContractQuery(restAddress, contractAddress, testPayload);
        contractType = response?.contractType || 'other';
      } catch (error) {
        log(`Failed to determine type for contract ${contractAddress}: ${error.message}`, 'DEBUG');
      }

      log(`Identified contract ${contractAddress} as ${contractType}`, 'INFO');

      // Add contract type and address to the batch
      batchData.push([contractType, contractAddress]);

      // Once batch is full, write to the database
      if (batchData.length >= batchSize) {
        await batchInsert(db.run.bind(db), 'contracts', ['type', 'address'], batchData);
        log(`Batch inserted ${batchData.length} contract types into the database`, 'INFO');
        batchData = []; // Clear the batch after writing

        // Update progress
        await updateProgress(db, 'identifyContractTypes', 0, contractAddress);
      }
    }

    // Insert any remaining data
    if (batchData.length > 0) {
      await batchInsert(db.run.bind(db), 'contracts', ['type', 'address'], batchData);
      log(`Final batch inserted ${batchData.length} contract types into the database`, 'INFO');
    }

    log('Finished identifying types for all contracts', 'INFO');
    await updateProgress(db, 'identifyContractTypes', 1, null);
  } catch (error) {
    log(`Error in identifyContractTypes: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store tokens for contracts of specific types
export async function fetchAndStoreTokensForContracts(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchTokens');
    if (progress.completed) {
      log('Skipping fetchAndStoreTokensForContracts: Already completed', 'INFO');
      return;
    }

    // Get the list of contracts to process
    const contractsResult = await dbAll("SELECT address, type FROM contracts WHERE type IN ('cw404', 'cw721_base', 'cw1155')");
    const startIndex = progress.last_processed ? contractsResult.findIndex(c => c.address === progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contractsResult.length; i++) {
      const { address: contractAddress, type: contractType } = contractsResult[i];
      let allTokensFetched = [];

      try {
        // Fetch all tokens for the current contract
        allTokensFetched = await fetchAllTokensForContract(restAddress, contractAddress, progress.last_fetched_token);

        // Insert fetched tokens into the contract_tokens table
        if (allTokensFetched.length > 0) {
          const insertData = allTokensFetched.map(tokenId => [contractAddress, tokenId, contractType]);
          await batchInsert(db.run.bind(db), 'contract_tokens', ['contract_address', 'token_id', 'contract_type'], insertData);
          log(`Stored ${allTokensFetched.length} tokens for contract ${contractAddress}`, 'INFO');

          // Update the token_ids column in the contracts table
          const tokenIdsString = allTokensFetched.join(',');
          await db.run(`UPDATE contracts SET token_ids = ? WHERE address = ?`, [tokenIdsString, contractAddress]);
          log(`Updated token_ids for contract ${contractAddress}: ${tokenIdsString}`, 'INFO');
        } else {
          log(`No tokens found for contract ${contractAddress}`, 'INFO');
        }

      } catch (error) {
        log(`Error fetching tokens for contract ${contractAddress}: ${error.message}`, 'ERROR');
      }

      // Update progress after processing each contract
      await updateProgress(db, 'fetchTokens', 0, contractAddress, null);
    }

    log('Finished processing tokens for all relevant contracts', 'INFO');
    await updateProgress(db, 'fetchTokens', 1, null, null);
  } catch (error) {
    log(`Error in fetchAndStoreTokensForContracts: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to fetch all tokens for a specific contract
async function fetchAllTokensForContract(restAddress, contractAddress, lastFetchedToken) {
  let startAfter = lastFetchedToken || null;
  let allTokens = [];
  let tokensFetched = 0;

  while (true) {
    const tokenQueryPayload = {
      all_tokens: {
        limit: config.paginationLimit,
        ...(startAfter && { start_after: startAfter })
      }
    };

    try {
      // Query the contract for tokens
      const { data, status } = await sendContractQuery(restAddress, contractAddress, tokenQueryPayload);
      if (status === 200 && data?.data?.tokens?.length > 0) {
        const tokenIds = data.data.tokens;
        allTokens = allTokens.concat(tokenIds);
        tokensFetched += tokenIds.length;
        log(`Fetched ${tokensFetched} tokens for contract ${contractAddress}`, 'INFO');

        // Set the start_after parameter for the next batch
        startAfter = tokenIds[tokenIds.length - 1];

        // If fewer tokens than the limit were fetched, it indicates the last page
        if (tokenIds.length < config.paginationLimit) {
          break;
        }
      } else {
        log(`Finished fetching tokens for contract ${contractAddress}`, 'INFO');
        break;
      }
    } catch (error) {
      log(`Error during token fetch for contract ${contractAddress}: ${error.message}`, 'ERROR');
      break;
    }
  }

  return allTokens;
}

// Fetch owner wallet address for each token_id in each collection
export async function fetchAndStoreTokenOwners(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchTokenOwners');
    if (progress.completed) {
      log('Skipping fetchAndStoreTokenOwners: Already completed', 'INFO');
      return;
    }

    log('Querying contract_tokens table...');
    const contractTokensResult = await getContractTokensToProcess(db, progress);
    log(`Fetched ${contractTokensResult.length} tokens from contract_tokens.`);

    const batchSize = 50;
    const ownershipData = [];

    for (let i = 0; i < contractTokensResult.length; i += batchSize) {
      const batch = contractTokensResult.slice(i, i + batchSize);
      await processTokenOwnershipBatch(restAddress, batch, ownershipData);

      // Perform batch insert into nft_owners table if data exists
      if (ownershipData.length > 0) {
        await batchInsert(db.run.bind(db), 'nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData);
        ownershipData.length = 0; // Clear the batch after insert
      }

      // Update progress after processing the batch
      const lastToken = batch[batch.length - 1];
      await updateProgress(db, 'fetchTokenOwners', 0, lastToken.contract_address, lastToken.token_id);
      log(`Processed ${i + batchSize} tokens so far.`, 'INFO');
    }

    log('Finished processing token ownership for all contracts.', 'INFO');
    await updateProgress(db, 'fetchTokenOwners', 1, null, null);
  } catch (error) {
    log(`Error in fetchAndStoreTokenOwners: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to get contract tokens that need processing
async function getContractTokensToProcess(db, progress) {
  const dbAll = promisify(db.all).bind(db);
  let contractTokensQuery = 'SELECT contract_address, token_id, contract_type FROM contract_tokens';
  let params = [];

  if (progress.last_processed_contract_address && progress.last_processed_token_id) {
    contractTokensQuery += ' WHERE (contract_address > ? OR (contract_address = ? AND token_id > ?))';
    params = [progress.last_processed_contract_address, progress.last_processed_contract_address, progress.last_processed_token_id];
  }

  return dbAll(contractTokensQuery, params);
}

// Helper function to process a batch of token ownerships
async function processTokenOwnershipBatch(restAddress, batch, ownershipData) {
  const fetchPromises = batch.map(({ contract_address: contractAddress, token_id: tokenId, contract_type: contractType }) => 
    fetchTokenOwner(restAddress, contractAddress, tokenId, contractType, ownershipData)
  );

  await Promise.all(fetchPromises);
}

// Helper function to fetch the owner of a token
async function fetchTokenOwner(restAddress, contractAddress, tokenId, contractType, ownershipData) {
  log(`Processing token ${tokenId} for contract ${contractAddress}...`, 'DEBUG');
  const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
  const headers = { 'x-cosmos-block-height': config.blockHeight.toString() };

  try {
    const { data, status } = await sendContractQuery(restAddress, contractAddress, ownerQueryPayload, headers);

    if (status === 200 && data?.data?.owner) {
      const owner = data.data.owner;
      log(`Owner for token ${tokenId} found: ${owner}`, 'INFO');
      ownershipData.push([contractAddress, tokenId, owner, contractType]);
    } else {
      log(`No valid owner found for token ${tokenId} in contract ${contractAddress}.`, 'INFO');
    }
  } catch (err) {
    log(`Error fetching token ownership for contract ${contractAddress}, token ${tokenId}: ${err.message}`, 'ERROR');
  }
}

// Fetch and store CW404 specific details
export async function fetchCW404Details(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const cw404Contracts = await getCW404Contracts(dbAll);
    if (cw404Contracts.length === 0) {
      log('No CW404 contracts found to process.', 'INFO');
      return;
    }

    await processCW404Contracts(cw404Contracts, restAddress, dbRun);
    log('Finished processing CW404 contract details.', 'INFO');
  } catch (error) {
    log(`Error in fetchCW404Details: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to get CW404 contracts from the database
async function getCW404Contracts(dbAll) {
  log('Fetching CW404 contracts from the database...', 'DEBUG');
  return dbAll("SELECT address FROM contracts WHERE type = 'cw404'");
}

// Helper function to process CW404 contracts and store details
async function processCW404Contracts(cw404Contracts, restAddress, dbRun) {
  const fetchPromises = cw404Contracts.map(async ({ address }) => {
    try {
      await fetchAndStoreCW404Details(restAddress, address, dbRun);
    } catch (error) {
      log(`Error processing CW404 contract ${address}: ${error.message}`, 'ERROR');
    }
  });

  // Execute all promises in parallel
  await Promise.all(fetchPromises);
}

// Helper function to fetch CW404 details and store them in the database
async function fetchAndStoreCW404Details(restAddress, contractAddress, dbRun) {
  const detailsPayload = { cw404_info: {} };
  const { data, status } = await sendContractQuery(restAddress, contractAddress, detailsPayload);

  if (status === 200 && data) {
    const insertSQL = `
      INSERT OR REPLACE INTO contract_details 
      (contract_address, base_uri, max_supply, royalty_percentage) 
      VALUES (?, ?, ?, ?)
    `;
    await dbRun(insertSQL, [contractAddress, data.base_uri, data.max_edition, data.royalty_percentage]);

    log(`Stored CW404 details for contract ${contractAddress}.`, 'INFO');
  } else {
    log(`No details found for CW404 contract ${contractAddress}.`, 'INFO');
  }
}

// Fetch and store pointer data in chunks to avoid request size limitations
export async function fetchAndStorePointerData(pointerApi, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const contractAddresses = await getContractAddresses(dbAll);
    if (contractAddresses.length === 0) {
      log('No contracts found to process for pointer data.', 'INFO');
      return;
    }

    await processPointerDataInChunks(contractAddresses, pointerApi, dbRun);
    log('Finished processing pointer data for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchAndStorePointerData: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to fetch all contract addresses from the database
async function getContractAddresses(dbAll) {
  log('Fetching contract addresses from the database...', 'DEBUG');
  const contractsResult = await dbAll('SELECT address FROM contracts');
  return contractsResult.map(row => row.address);
}

// Helper function to process pointer data in chunks
async function processPointerDataInChunks(contractAddresses, pointerApi, dbRun, chunkSize = 10) {
  for (let i = 0; i < contractAddresses.length; i += chunkSize) {
    const chunk = contractAddresses.slice(i, i + chunkSize);
    const payload = { addresses: chunk };

    try {
      await fetchAndStorePointerChunk(pointerApi, payload, dbRun);
    } catch (error) {
      log(`Error processing pointer data chunk: ${error.message}`, 'ERROR');
    }
  }
}

// Helper function to fetch and store pointer data for a chunk
async function fetchAndStorePointerChunk(pointerApi, payload, dbRun) {
  const response = await axios.post(pointerApi, payload);
  if (response.status === 200 && response.data) {
    for (const entry of response.data) {
      const { address, pointerAddress, pointeeAddress, isBaseAsset, isPointer } = entry;
      const insertSQL = `
        INSERT OR REPLACE INTO pointer_data (contract_address, pointer_address, pointee_address, is_base_asset, is_pointer)
        VALUES (?, ?, ?, ?, ?)
      `;
      await dbRun(insertSQL, [
        address,
        pointerAddress || null,
        pointeeAddress || null,
        isBaseAsset ? 1 : 0, // 1 for true, 0 for false
        isPointer ? 1 : 0    // 1 for true, 0 for false
      ]);
      log(`Stored pointer data for address ${address}`, 'DEBUG');
    }
  } else {
    log(`Failed to fetch pointer data. Status: ${response.status}`, 'ERROR');
  }
}

// Fetch and store associated wallet addresses based on EVM address lookup
export async function fetchAndStoreAssociatedWallets(evmRpcAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const owners = await getUniqueOwners(dbAll);
    if (owners.length === 0) {
      log('No unique owners found in nft_owners table.', 'INFO');
      return;
    }

    await processOwnersForEVMAddress(owners, evmRpcAddress, dbRun);
    log('Finished processing associated wallets.', 'INFO');
  } catch (error) {
    log(`Error in fetchAndStoreAssociatedWallets: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to get unique owners from the database
async function getUniqueOwners(dbAll) {
  log('Fetching unique owners from the nft_owners table...', 'DEBUG');
  const ownersResult = await dbAll('SELECT DISTINCT owner FROM nft_owners');
  return ownersResult.map(row => row.owner);
}

// Helper function to process owners and fetch their EVM addresses
async function processOwnersForEVMAddress(owners, evmRpcAddress, dbRun) {
  const fetchPromises = owners.map(async (owner) => {
    try {
      await fetchAndStoreEVMAddressForOwner(owner, evmRpcAddress, dbRun);
    } catch (error) {
      log(`Error processing owner ${owner}: ${error.message}`, 'ERROR');
    }
  });

  // Execute all fetch operations in parallel
  await Promise.all(fetchPromises);
}

// Helper function to fetch and store the EVM address for an owner
async function fetchAndStoreEVMAddressForOwner(owner, evmRpcAddress, dbRun) {
  const payload = {
    jsonrpc: "2.0",
    method: "sei_getEVMAddress",
    params: [owner],
    id: 1
  };

  const response = await retryOperation(() => axios.post(evmRpcAddress, payload));
  if (response.status === 200 && response.data && response.data.result) {
    const evmAddress = response.data.result;
    const insertSQL = `
      INSERT OR REPLACE INTO wallet_associations (wallet_address, evm_address)
      VALUES (?, ?)
    `;
    await dbRun(insertSQL, [owner, evmAddress]);
    log(`Stored EVM address ${evmAddress} for wallet ${owner}`, 'DEBUG');
  } else {
    log(`Failed to fetch EVM address for wallet ${owner}.`, 'ERROR');
  }
}
