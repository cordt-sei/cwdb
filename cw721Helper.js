// cw721Helper.js

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
      log('Skipping fetchAndStoreCodeIds: Already completed');
      return;
    }

    let startAfter = progress.last_processed ? parseInt(progress.last_processed) : null; // Set to null for the first request
    let allCodeInfos = [];
    let hasMore = true;

    while (hasMore) {
      const payload = {
        'pagination.reverse': false,
        'pagination.limit': config.paginationLimit
      };

      if (startAfter !== null) {
        // Only add the pagination.key if startAfter is not null (i.e., not the first query)
        payload['pagination.key'] = Buffer.from(startAfter.toString()).toString('base64');
      }

      const codeInfos = await fetchPaginatedData(
        `${restAddress}/cosmwasm/wasm/v1/code`,
        null,
        payload,
        'code_infos'
      );

      for (const codeInfo of codeInfos) {
        const { code_id, creator } = codeInfo;
        const insertSQL = `INSERT OR REPLACE INTO code_ids (code_id, creator) VALUES (?, ?)`;
        await dbRun(insertSQL, [code_id, creator]);
        log(`Stored code_id: ${code_id} from creator ${creator}`);
        await updateProgress(db, 'fetchCodeIds', 0, code_id);
        startAfter = parseInt(code_id);
      }

      allCodeInfos = allCodeInfos.concat(codeInfos);
      hasMore = codeInfos.length === config.paginationLimit;
    }

    log(`Total code_ids fetched: ${allCodeInfos.length}`);
    await updateProgress(db, 'fetchCodeIds', 1, null);
  } catch (error) {
    log(`Error in fetchAndStoreCodeIds: ${error.message}`);
    throw error;
  }
}

// Fetch contracts by code and store them in the database using batch insert
export async function fetchAndStoreContractsByCode(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchContracts');
    if (progress.completed) {
      log('Skipping fetchAndStoreContractsByCode: Already completed');
      return;
    }

    const codeIdsResult = await dbAll('SELECT code_id FROM code_ids');
    const codeIds = codeIdsResult.map(row => row.code_id);

    const startIndex = progress.last_processed ? codeIds.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < codeIds.length; i++) {
      const code_id = codeIds[i];
      let batchData = [];

      // Use fetchPaginatedData to fetch all contracts associated with the code_id
      const contracts = await fetchPaginatedData(
        `${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts`,
        null,
        { 'pagination.limit': 100 }, // Pagination payload
        'contracts'
      );

      if (contracts.length > 0) {
        log(`Fetched ${contracts.length} contracts for code_id: ${code_id}`);

        // Prepare batch data for insertion
        contracts.forEach(contractAddress => {
          batchData.push([code_id, contractAddress]);
        });

        // Perform batch insert
        const placeholders = batchData.map(() => '(?, ?)').join(',');
        const insertSQL = `INSERT OR REPLACE INTO contracts (code_id, address) VALUES ${placeholders}`;
        const flatBatch = batchData.flat();
        await dbRun(insertSQL, flatBatch);
        log(`Batch inserted ${batchData.length} contracts for code_id: ${code_id}`);

      } else {
        log(`No contracts found for code_id: ${code_id}`);
      }

      // Update progress after each code_id is processed
      await updateProgress(db, 'fetchContracts', 0, code_id);
    }

    log(`Finished processing contracts for all ${codeIds.length} code IDs`);
    await updateProgress(db, 'fetchContracts', 1, null);
  } catch (error) {
    log(`Error in fetchAndStoreContractsByCode: ${error.message}`);
    throw error;
  }
}

// Fetch and store contract history per contract
export async function fetchAndStoreContractHistory(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchContractHistory');
    if (progress.completed) {
      log('Skipping fetchAndStoreContractHistory: Already completed');
      return;
    }

    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contractAddresses = contractsResult.map(row => row.address);

    const startIndex = progress.last_processed ? contractAddresses.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contractAddresses.length; i++) {
      const address = contractAddresses[i];
      let hasMore = true;
      let nextKey = null;
      let batchData = [];

      while (hasMore) {
        const payload = {};
        if (nextKey) {
          payload['pagination.key'] = nextKey;
        }

        // Fetch contract history entries using pagination
        const historyEntries = await fetchPaginatedData(`${restAddress}/cosmwasm/wasm/v1/contract/${address}/history`, null, payload, 'entries');
        
        // Store fetched entries in the batch
        for (const entry of historyEntries) {
          const { operation, code_id, updated, msg } = entry;
          batchData.push([address, operation, code_id, updated || null, JSON.stringify(msg)]);
        }

        // Perform batch insert if data exists
        if (batchData.length > 0) {
          await batchInsert(dbRun, 'contract_history', ['contract_address', 'operation', 'code_id', 'updated', 'msg'], batchData);
          batchData = []; // Clear batch data after insertion
        }

        // Check pagination for next page
        nextKey = historyEntries.pagination?.next_key || null;
        hasMore = historyEntries.length === config.paginationLimit && nextKey;
      }

      // Update progress after each contract
      await updateProgress(db, 'fetchContractHistory', 0, address);
    }

    log(`Finished processing history for all ${contractAddresses.length} contracts`);
    await updateProgress(db, 'fetchContractHistory', 1, null);
  } catch (error) {
    log(`Error in fetchAndStoreContractHistory: ${error.message}`);
    throw error;
  }
}

// Function to determine the contract type based on error message
async function determineContractType(restAddress, contractAddress) {
  const testPayload = { "a": "b" }; // Example invalid payload
  try {
    const { contractType } = await sendContractQuery(restAddress, contractAddress, testPayload);

    // Return contract type without logging here
    return contractType;
  } catch (err) {
    // Handle unexpected errors gracefully and assume 'other' contract type
    return 'other';
  }
}

// Main function to process contracts with batch DB write using batchInsert
export async function identifyContractTypes(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  const batchSize = 50; // Set a batch size for DB write
  let batchData = []; // Hold the data to be written in a batch

  try {
    const progress = await checkProgress(db, 'identifyContractTypes');
    if (progress.completed) {
      log('Skipping identifyContractTypes: Already completed');
      return;
    }

    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contracts = contractsResult.map(row => row.address);
    const startIndex = progress.last_processed ? contracts.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contracts.length; i++) {
      const contractAddress = contracts[i];

      try {
        const contractType = await determineContractType(restAddress, contractAddress);

        // Log after contract type is identified
        log(`Identified contract ${contractAddress} as ${contractType}`);

        // Add contract type and address to the batch
        batchData.push([contractType, contractAddress]);

        // Once batch is full, write to DB using batchInsert
        if (batchData.length >= batchSize) {
          await batchInsert(dbRun, 'contracts', ['type', 'address'], batchData);
          log(`Batch written to the database for ${batchData.length} contracts`);
          batchData = []; // Clear the batch after writing to the DB

          // Update progress
          await updateProgress(db, 'identifyContractTypes', 0, contractAddress);
        }
      } catch (error) {
        log(`Error processing contract ${contractAddress}: ${error.message}`);
      }
    }

    // Write any remaining data that didn't fill up the batch size
    if (batchData.length > 0) {
      await batchInsert(dbRun, 'contracts', ['type', 'address'], batchData);
      log(`Final batch written to the database for ${batchData.length} contracts`);
      batchData = []; // Clear the batch after writing to the DB
    }

    log('Finished identifying types for all contracts.');
    await updateProgress(db, 'identifyContractTypes', 1, null);
  } catch (error) {
    log(`Error in identifyContractTypes: ${error.message}`);
    throw error;
  }
}

// Updated fetchAndStoreTokensForContracts to track last_fetched_token
export async function fetchAndStoreTokensForContracts(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchTokens');
    if (progress.completed) {
      log('Skipping fetchAndStoreTokensForContracts: Already completed');
      return;
    }

    // Get the list of contracts to process
    const contractsResult = await dbAll("SELECT address, type FROM contracts WHERE type IN ('cw404', 'cw721_base', 'galxe_nft')");
    const startIndex = progress.last_processed ? contractsResult.findIndex(c => c.address === progress.last_processed) + 1 : 0;

    const fetchPromises = contractsResult.slice(startIndex).map(async ({ address: contractAddress, type: contractType }) => {
      let startAfter = progress.last_fetched_token || null; // Pagination parameter for the contract
      let tokensFetched = 0;
      let allTokensFetched = [];

      try {
        // Fetch tokens in paginated batches
        while (true) {
          // Construct the query payload with pagination
          const tokenQueryPayload = {
            all_tokens: {
              limit: config.paginationLimit, // Use the configured pagination limit
              ...(startAfter && { start_after: startAfter }) // Include start_after only if set
            }
          };

          // Query the contract for tokens
          const { data, status } = await sendContractQuery(restAddress, contractAddress, tokenQueryPayload);
          if (status === 200 && data && data.data && data.data.tokens && data.data.tokens.length > 0) {
            const tokenIds = data.data.tokens;
            allTokensFetched = allTokensFetched.concat(tokenIds);

            tokensFetched += tokenIds.length;
            log(`Fetched ${tokensFetched} tokens for contract ${contractAddress}`);

            // Set the start_after parameter for the next batch
            startAfter = tokenIds[tokenIds.length - 1];
            
            // Update progress after processing the batch
            await updateProgress(db, 'fetchTokens', 0, contractAddress, startAfter);

            // If fewer tokens than the limit were fetched, it indicates the last page
            if (tokenIds.length < config.paginationLimit) {
              break;
            }
          } else {
            // If no tokens are returned, we have finished fetching all tokens for this contract
            log(`Finished fetching tokens for contract ${contractAddress}`);
            break;
          }
        }

        // Insert all fetched tokens into the database
        if (allTokensFetched.length > 0) {
          const insertData = allTokensFetched.map(tokenId => [contractAddress, tokenId, contractType]);
          await batchInsert(dbRun, 'contract_tokens', ['contract_address', 'token_id', 'contract_type'], insertData);
          log(`Stored ${allTokensFetched.length} tokens for contract ${contractAddress}.`);
        }

        // Update the tokens column in the contracts table with the total tokens fetched
        await dbRun(`UPDATE contracts SET token_count = ? WHERE address = ?`, [tokensFetched, contractAddress]);
        log(`Updated token count for contract ${contractAddress}: ${tokensFetched}`);

      } catch (error) {
        log(`Error fetching tokens for contract ${contractAddress}: ${error.message}`);
      }

      // Update progress after processing each contract
      await updateProgress(db, 'fetchTokens', 0, contractAddress, null);
    });

    // Execute all fetch promises in parallel
    await Promise.all(fetchPromises);

    log('Finished processing tokens for all relevant contracts.');
    await updateProgress(db, 'fetchTokens', 1, null, null);
  } catch (error) {
    log(`Error in fetchAndStoreTokensForContracts: ${error.message}`);
    throw error;
  }
}

// Fetch owner wallet address for each token_id in each collection
export async function fetchAndStoreTokenOwners(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchTokenOwners');
    log('Checking progress for fetchTokenOwners...');
    let lastProcessedContractAddress = progress.last_processed_contract_address || null;
    let lastProcessedTokenId = progress.last_processed_token_id || null;

    if (progress.completed) {
      log('Skipping fetchAndStoreTokenOwners: Already completed');
      return;
    }

// Query contract_tokens table to get the contract address and token_id, skipping already processed tokens
log('Querying contract_tokens table...');
let contractTokensQuery = 'SELECT contract_address, token_id, contract_type FROM contract_tokens';
let params = [];

if (lastProcessedContractAddress && lastProcessedTokenId) {
  contractTokensQuery += ` WHERE (contract_address > ? OR (contract_address = ? AND token_id > ?))`;
  params = [lastProcessedContractAddress, lastProcessedContractAddress, lastProcessedTokenId];
}

const contractTokensResult = await dbAll(contractTokensQuery, params);
log(`Fetched ${contractTokensResult.length} tokens from contract_tokens.`);


    const batchSize = 50; // Set a batch size for concurrent requests
    const progressUpdateInterval = 5; // Update progress every 5 batches
    let processed = 0;
    const ownershipData = [];

    // Batch processing of token ownership queries
    for (let i = 0; i < contractTokensResult.length; i += batchSize) {
      const batch = contractTokensResult.slice(i, i + batchSize);
      
      // Fetch ownership for the current batch
      const batchPromises = batch.map(async ({ contract_address: contractAddress, token_id: tokenId, contract_type: contractType }) => {
        log(`Processing token ${tokenId} for contract ${contractAddress}...`);

        // Prepare payload for querying the owner of the token
        const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
        const headers = { 'x-cosmos-block-height': config.blockHeight.toString() };

        try {
          // Query the contract for the token owner
          log(`Sending query for token ${tokenId} to contract ${contractAddress}...`);
          const { data, status } = await sendContractQuery(restAddress, contractAddress, ownerQueryPayload, headers);

          // Ensure response is valid and contains the 'owner' field
          if (status === 200 && data && data.data && data.data.owner) {
            const owner = data.data.owner;
            log(`Owner for token ${tokenId} found: ${owner}`);

            // Add owner details to the batch insert data
            ownershipData.push([contractAddress, tokenId, owner, contractType]);
          } else {
            log(`No valid owner found for contract ${contractAddress}, token ${tokenId}`);
          }
        } catch (err) {
          log(`Error fetching token ownership for contract ${contractAddress}, token ${tokenId}: ${err.message}`);
        }
      });

      // Wait for all promises in the batch to complete
      await Promise.all(batchPromises);

      // Perform batch insert into nft_owners table
      if (ownershipData.length > 0) {
        await batchInsert(dbRun, 'nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData);
        ownershipData.length = 0; // Clear the batch after insert
      }

      // Update the processed count
      processed += batch.length;
      log(`Processed ${processed} tokens so far.`);

      // Update progress after every batch
      const lastToken = batch[batch.length - 1];
      await updateProgress(db, 'fetchTokenOwners', 0, lastToken.contract_address, lastToken.token_id);

      // Update progress every `progressUpdateInterval` batches
      if ((i / batchSize) % progressUpdateInterval === 0) {
        log(`Updated progress after ${progressUpdateInterval} batches.`);
      }
    }

    log('Finished processing token ownership for all contracts');
    await updateProgress(db, 'fetchTokenOwners', 1, null, null);
  } catch (error) {
    log(`Error in fetchAndStoreTokenOwners: ${error.message}`);
    throw error;
  }
}

// Fetch and store CW404 specific details
export async function fetchCW404Details(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const cw404ContractsResult = await dbAll("SELECT address FROM contracts WHERE type = 'cw404'");
    
    for (const contract of cw404ContractsResult) {
      const detailsPayload = { cw404_info: {} };
      const { data, status } = await sendContractQuery(restAddress, contract.address, detailsPayload);

      if (status === 200 && data) {
        const insertDetailsSQL = `INSERT OR REPLACE INTO contract_details (contract_address, base_uri, max_supply, royalty_percentage) VALUES (?, ?, ?, ?)`;
        await dbRun(insertDetailsSQL, [contract.address, data.base_uri, data.max_edition, data.royalty_percentage]);

        log(`Stored CW404 details for contract ${contract.address}.`);
      } else {
        log(`No details found for CW404 contract ${contract.address}.`);
      }
    }

    log('Finished processing CW404 contract details');
  } catch (error) {
    log(`Error in fetchCW404Details: ${error.message}`);
    throw error;
  }
}

export async function fetchAndStorePointerData(pointerApi, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contractAddresses = contractsResult.map(row => row.address);

    // Chunk addresses into smaller requests to avoid 413 error
    const chunkSize = 10;
    for (let i = 0; i < contractAddresses.length; i += chunkSize) {
      const chunk = contractAddresses.slice(i, i + chunkSize);
      const payload = { addresses: chunk };

      try {
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
              isBaseAsset ? 1 : 0,   // Use 1 for true, 0 for false
              isPointer ? 1 : 0      // Use 1 for true, 0 for false
            ]);
            log(`Stored pointer data for address ${address}`);
          }
        }
      } catch (err) {
        log(`Error in pointer data request: ${err.message}`);
      }
    }

    log('Finished processing pointer data for all contracts');
  } catch (error) {
    log(`Error in fetchAndStorePointerData: ${error.message}`);
    throw error;
  }
}

// Fetch and store associated wallet addresses (EVM Address lookup)
export async function fetchAndStoreAssociatedWallets(evmRpcAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const ownersResult = await dbAll('SELECT DISTINCT owner FROM nft_owners');
    log(`Fetched ${ownersResult.length} unique owners from nft_owners table.`);

    for (const { owner } of ownersResult) {
      const payload = {
        jsonrpc: "2.0",
        method: "sei_getEVMAddress",
        params: [owner], // Use the Bech32 address for the lookup
        id: 1
      };

      try {
        // Send the query to the EVM RPC endpoint
        const response = await retryOperation(() => axios.post(evmRpcAddress, payload));
        
        // Check if the response contains a valid result
        if (response.status === 200 && response.data && response.data.result) {
          const evmAddress = response.data.result;

          // Insert the EVM address into the wallet_associations table
          const insertSQL = `INSERT OR REPLACE INTO wallet_associations (wallet_address, evm_address) VALUES (?, ?)`;
          await dbRun(insertSQL, [owner, evmAddress]);
          
          log(`Stored EVM address: ${evmAddress} for wallet: ${owner}`);
        } else {
          log(`Error fetching EVM address for wallet ${owner}: No valid result`);
        }
      } catch (error) {
        log(`Error fetching EVM address for wallet ${owner}: ${error.message}`);
      }
    }

    log('Finished processing associated wallets.');
  } catch (error) {
    log(`Error in fetchAndStoreAssociatedWallets: ${error.message}`);
    throw error;
  }
}
