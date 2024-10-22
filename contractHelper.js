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

// Fetch all code IDs and store them in the database using batch insert and parallel processing
export async function fetchAndStoreCodeIds(restAddress, db) {
  const dbRun = promisify(db.run).bind(db);
  try {
      const progress = await checkProgress(db, 'fetchCodeIds');
      if (progress.completed) {
          log('Skipping fetchAndStoreCodeIds: Already completed', 'INFO');
          return;
      }
      
      let startAfter = progress.last_processed || null;
      let allCodeInfos = [];
      
      while (true) {
          const response = await fetchPaginatedData(
              `${restAddress}/cosmwasm/wasm/v1/code`,
              'code_infos',
              {
                  paginationType: 'query',
                  useNextKey: true,
                  limit: config.paginationLimit,
                  paginationPayload: {
                      'pagination.reverse': false,
                      'pagination.limit': config.paginationLimit,
                      ...(startAfter && { 'pagination.key': startAfter }),
                  }
              }
          );

          if (!Array.isArray(response) || response.length === 0) {
              log('No more code IDs to fetch.', 'INFO');
              break;
          }
          
          // Batch insert code info
          const batchData = response.map(({ code_id, creator, data_hash, instantiate_permission }) => 
              [code_id, creator, data_hash, JSON.stringify(instantiate_permission)]
          );
          await batchInsert(dbRun, 'code_ids', ['code_id', 'creator', 'data_hash', 'instantiate_permission'], batchData);
          
          allCodeInfos = allCodeInfos.concat(response);
          startAfter = Buffer.from(response[response.length - 1].code_id).toString('base64');
          
          // End if less than paginationLimit
          if (response.length < config.paginationLimit) break;

          await updateProgress(db, 'fetchCodeIds', 0, response[response.length - 1].code_id);
      }
      
      if (allCodeInfos.length > 0) {
          log(`Total code IDs fetched and stored: ${allCodeInfos.length}`, 'INFO');
          await updateProgress(db, 'fetchCodeIds', 1);
      }
  } catch (error) {
      log(`Error in fetchAndStoreCodeIds: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Fetch contract addresses by code and store them in the database
export async function fetchAndStoreContractAddressesByCode(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  try {
      const progress = await checkProgress(db, 'fetchContractsByCode');
      if (progress.completed) {
          log('Skipping fetchAndStoreContractAddressesByCode: Already completed', 'INFO');
          return;
      }

      const codeIds = (await dbAll('SELECT code_id FROM code_ids')).map(row => row.code_id);
      const startIndex = progress.last_processed ? codeIds.indexOf(progress.last_processed) + 1 : 0;
      
      for (let i = startIndex; i < codeIds.length; i++) {
          const code_id = codeIds[i];
          const contracts = await fetchPaginatedData(
              `${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts`,
              'contracts',
              {
                  paginationType: 'query',
                  useNextKey: true,
                  limit: config.paginationLimit
              }
          );

          if (contracts.length > 0) {
              const batchData = contracts.map(contractAddress => [code_id, contractAddress, null]);
              await batchInsert(dbRun, 'contracts', ['code_id', 'address', 'type'], batchData);
              log(`Batch inserted ${batchData.length} contract addresses for code_id: ${code_id}`, 'INFO');
          } else {
              log(`No contract addresses found for code_id: ${code_id}`, 'INFO');
          }

          await updateProgress(db, 'fetchContractsByCode', 0, code_id);
      }
      await updateProgress(db, 'fetchContractsByCode', 1);
  } catch (error) {
      log(`Error in fetchAndStoreContractAddressesByCode: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Fetch and store metadata for each contract address
export async function fetchAndStoreContractMetadata(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const batchSize = 5;
  const delayBetweenBatches = 100;

  try {
      const progress = await checkProgress(db, 'fetchContractMetadata');
      if (progress.completed) {
          log('Skipping fetchAndStoreContractMetadata: Already completed', 'INFO');
          return;
      }

      const contractAddresses = (await dbAll('SELECT address FROM contracts')).map(row => row.address);
      const startIndex = progress.last_processed ? contractAddresses.indexOf(progress.last_processed) + 1 : 0;

      for (let i = startIndex; i < contractAddresses.length; i += batchSize) {
          const batch = contractAddresses.slice(i, i + batchSize);
          const fetchPromises = batch.map(async contractAddress => {
              try {
                  const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}`;
                  const response = await retryOperation(() => axios.get(url));
                  if (response.status === 200 && response.data?.contract_info) {
                      const { code_id, creator, admin, label } = response.data.contract_info;
                      return [contractAddress, code_id, creator, admin, label];
                  }
              } catch (error) {
                  log(`Failed to fetch contract info for ${contractAddress}: ${error.message}`, 'ERROR');
              }
              return null;
          });

          const results = (await Promise.all(fetchPromises)).filter(Boolean);
          if (results.length > 0) {
              await batchInsert(dbRun, 'contracts', ['address', 'code_id', 'creator', 'admin', 'label'], results);
              log(`Batch inserted ${results.length} contracts metadata`, 'INFO');
          }
          await updateProgress(db, 'fetchContractMetadata', 0, batch[batch.length - 1]);
          await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
      }
      await updateProgress(db, 'fetchContractMetadata', 1);
  } catch (error) {
      log(`Error in fetchAndStoreContractMetadata: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Fetch and store contract history per contract with parallel processing and batch insertions
export async function fetchAndStoreContractHistory(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const batchSize = 10; 

  try {
      const progress = await checkProgress(db, 'fetchContractHistory');
      if (progress.completed) {
          log('Skipping fetchAndStoreContractHistory: Already completed', 'INFO');
          return;
      }

      const contractAddresses = (await dbAll('SELECT address FROM contracts')).map(row => row.address);
      const startIndex = progress.last_processed ? contractAddresses.indexOf(progress.last_processed) + 1 : 0;

      for (let i = startIndex; i < contractAddresses.length; i += batchSize) {
          const batch = contractAddresses.slice(i, i + batchSize);
          const historyPromises = batch.map(async (address) => {
              try {
                  const historyEntries = await fetchPaginatedData(
                      `${restAddress}/cosmwasm/wasm/v1/contract/${address}/history`,
                      'entries',
                      {
                          paginationType: 'query',
                          useNextKey: true,
                          limit: config.paginationLimit
                      }
                  );

                  if (historyEntries.length > 0) {
                      const batchData = historyEntries.map(({ operation, code_id, updated, msg }) => [
                          address, operation, code_id, updated || null, JSON.stringify(msg)
                      ]);
                      await batchInsert(dbRun, 'contract_history', ['contract_address', 'operation', 'code_id', 'updated', 'msg'], batchData);
                      log(`Inserted ${batchData.length} history records for contract: ${address}`, 'INFO');
                  } else {
                      log(`No history entries found for contract: ${address}`, 'INFO');
                  }
                  await updateProgress(db, 'fetchContractHistory', 0, address);
              } catch (error) {
                  log(`Error fetching history for contract ${address}: ${error.message}`, 'ERROR');
              }
          });
          await Promise.all(historyPromises);
          log(`Processed a batch of ${batch.length} contracts`, 'INFO');
      }

      await updateProgress(db, 'fetchContractHistory', 1);
      log('Finished processing history for all contracts.', 'INFO');
  } catch (error) {
      log(`Error in fetchAndStoreContractHistory: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Identify contract types and store them in the database using parallel processing and batch inserts
export async function identifyContractTypes(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const batchSize = 50;
  const parallelRequests = 10;

  try {
      const progress = await checkProgress(db, 'identifyContractTypes');
      if (progress.completed) {
          log('Skipping identifyContractTypes: Already completed', 'INFO');
          return;
      }

      const contracts = (await dbAll('SELECT address FROM contracts')).map(row => row.address);
      const startIndex = progress.last_processed ? contracts.indexOf(progress.last_processed) + 1 : 0;

      let batchData = [];

      for (let i = startIndex; i < contracts.length; i += parallelRequests) {
          const batch = contracts.slice(i, i + parallelRequests);
          const typePromises = batch.map(async (contractAddress) => {
              let contractType = 'other';

              try {
                  const testPayload = { "a": "b" };
                  const response = await sendContractQuery(restAddress, contractAddress, testPayload);

                  if (response.status === 400 && response.data) {
                      const match = response.data.message?.match(/Error parsing into type (\S+)::msg::QueryMsg/);
                      if (match) {
                          contractType = match[1];
                          log(`Identified contract type ${contractType} for contract ${contractAddress}`, 'INFO');
                      } else {
                          log(`Unable to extract contract type for ${contractAddress} from 400 response`, 'DEBUG');
                      }
                  } else {
                      log(`Unexpected response for contract ${contractAddress}: ${response.status}`, 'DEBUG');
                  }
              } catch (error) {
                  log(`Failed to determine type for contract ${contractAddress}: ${error.message}`, 'DEBUG');
              }

              return [contractType, contractAddress];
          });

          const results = await Promise.all(typePromises);
          batchData.push(...results);

          if (batchData.length >= batchSize) {
              await batchInsert(dbRun, 'contracts', ['type', 'address'], batchData);
              batchData = [];
              await updateProgress(db, 'identifyContractTypes', 0, batch[batch.length - 1]);
          }
      }

      if (batchData.length > 0) {
          await batchInsert(dbRun, 'contracts', ['type', 'address'], batchData);
      }

      await updateProgress(db, 'identifyContractTypes', 1);
      log('Finished identifying types for all contracts.', 'INFO');
  } catch (error) {
      log(`Error in identifyContractTypes: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Fetch and store tokens for contracts of specific types using parallel processing and batch inserts
export async function fetchAndStoreTokensForContracts(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const parallelRequests = 10;

  try {
      const progress = await checkProgress(db, 'fetchTokens');
      const startIndex = progress.last_processed 
          ? (await dbAll("SELECT rowid FROM contracts WHERE address = ?", [progress.last_processed]))[0].rowid 
          : 0;

      const contractsResult = await dbAll(
          "SELECT rowid, address, type FROM contracts WHERE (token_ids IS NULL OR token_ids = '') AND (type LIKE 'cw404%' OR type LIKE 'cw721%' OR type LIKE 'cw1155%') AND rowid > ?", 
          [startIndex]
      );

      for (let i = 0; i < contractsResult.length; i += parallelRequests) {
          const batch = contractsResult.slice(i, i + parallelRequests);
          const tokenPromises = batch.map(async ({ address: contractAddress, type: contractType }) => {
              try {
                  const allTokensFetched = await fetchAllTokensForContract(restAddress, contractAddress, progress.last_fetched_token);

                  if (allTokensFetched.length > 0) {
                      const tokenIdsString = allTokensFetched.join(',');
                      await dbRun("UPDATE contracts SET token_ids = ? WHERE address = ?", [tokenIdsString, contractAddress]);
                      log(`Updated token_ids for contract ${contractAddress}: ${tokenIdsString}`, 'INFO');

                      const tokenInsertPromises = allTokensFetched.map(tokenId => 
                          dbRun("INSERT INTO contract_tokens (contract_address, token_id, contract_type) VALUES (?, ?, ?)", [contractAddress, tokenId, contractType])
                      );
                      await Promise.all(tokenInsertPromises);
                      log(`Inserted ${allTokensFetched.length} tokens into contract_tokens for contract ${contractAddress}`, 'INFO');
                  }
                  await updateProgress(db, 'fetchTokens', 0, contractAddress);
              } catch (error) {
                  log(`Error fetching tokens for contract ${contractAddress}: ${error.message}`, 'ERROR');
              }
          });

          await Promise.all(tokenPromises);
          log(`Processed a batch of ${batch.length} contracts`, 'INFO');
      }

      await updateProgress(db, 'fetchTokens', 1);
      log('Finished processing tokens for all relevant contracts.', 'INFO');
  } catch (error) {
      log(`Error in fetchAndStoreTokensForContracts: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Helper function to fetch all tokens for a specific contract with optimized pagination
async function fetchAllTokensForContract(restAddress, contractAddress, lastFetchedToken) {
  let startAfter = lastFetchedToken || null;
  let allTokens = [];
  let tokensFetched = 0;

  while (true) {
    const tokenQueryPayload = {
      all_tokens: {
        limit: 100,  // Set the pagination limit
        ...(startAfter && { start_after: startAfter })
      }
    };

    try {
      const response = await axios.post(`${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`, tokenQueryPayload, {
        headers: { 'Content-Type': 'application/json' }
      });

      // Handle the response
      if (response.status === 200 && response.data?.data?.tokens?.length > 0) {
        const tokenIds = response.data.data.tokens;
        allTokens = allTokens.concat(tokenIds);
        tokensFetched += tokenIds.length;
        log(`Fetched ${tokensFetched} tokens for contract ${contractAddress}`, 'INFO');

        // Update the startAfter parameter to the last token ID in the current batch
        startAfter = tokenIds[tokenIds.length - 1];

        // If fewer tokens than the limit were fetched, break the loop as it indicates the last page
        if (tokenIds.length < 100) {
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

/// Fetch owner wallet address for each token_id in each collection using parallel processing and batch inserts
export async function fetchAndStoreTokenOwners(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const parallelRequests = 10; // Number of parallel requests to fetch token ownership

  try {
    const progress = await checkProgress(db, 'fetchTokenOwners');
    if (progress.completed) {
      log('Skipping fetchAndStoreTokenOwners: Already completed', 'INFO');
      return;
    }

    log('Querying contract_tokens table...');
    const contractTokensResult = await getContractTokensToProcess(db, progress);
    log(`Fetched ${contractTokensResult.length} tokens from contract_tokens.`);

    for (let i = 0; i < contractTokensResult.length; i += parallelRequests) {
      const batch = contractTokensResult
        .slice(i, i + parallelRequests)
        .filter(({ contract_type }) => /721|1155|404/i.test(contract_type));

      if (batch.length === 0) {
        log('No valid NFT contracts to process in this batch.', 'INFO');
        continue;
      }

      const ownershipData = [];

      // Process token ownership in parallel, including retry logic for transient errors
      await processTokenOwnershipBatch(restAddress, batch, ownershipData);

      // Perform batch insert if ownership data exists
      if (ownershipData.length > 0) {
        try {
          await batchInsert(dbRun, 'nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData);
          log(`Inserted ${ownershipData.length} ownership records into nft_owners table.`, 'INFO');
        } catch (insertError) {
          log(`Error inserting ownership records: ${insertError.message}`, 'ERROR');
        }
      }

      // Update progress after processing the batch
      const lastToken = batch[batch.length - 1];
      await updateProgress(db, 'fetchTokenOwners', 0, lastToken.contract_address, lastToken.token_id);
      log(`Processed a batch of ${batch.length} tokens.`, 'INFO');
    }

    log('Finished processing token ownership for all contracts.', 'INFO');
    await updateProgress(db, 'fetchTokenOwners', 1, null, null);
  } catch (error) {
    log(`Error in fetchAndStoreTokenOwners: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to get contract tokens that need processing, with improved query handling
async function getContractTokensToProcess(db, progress) {
  const dbAll = promisify(db.all).bind(db);
  let contractTokensQuery = 'SELECT contract_address, token_id, contract_type FROM contract_tokens';
  let params = [];

  // Use the indexed columns for filtering if progress data is available
  if (progress.last_processed_contract_address && progress.last_processed_token_id) {
    contractTokensQuery += ' WHERE (contract_address > ? OR (contract_address = ? AND token_id > ?))';
    params = [
      progress.last_processed_contract_address,
      progress.last_processed_contract_address,
      progress.last_processed_token_id,
    ];
  }

  try {
    // Execute the query with parameters, making use of the index for faster access
    return await dbAll(contractTokensQuery, params);
  } catch (error) {
    log(`Error fetching contract tokens: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to process a batch of token ownerships using parallel processing and retries
async function processTokenOwnershipBatch(restAddress, batch, ownershipData, maxRetries = 3) {
  const fetchPromises = batch.map(({ contract_address: contractAddress, token_id: tokenId, contract_type: contractType }) => 
    fetchTokenOwnerWithRetry(restAddress, contractAddress, tokenId, contractType, ownershipData, maxRetries)
  );

  // Wait for all promises to complete, even if some fail
  await Promise.allSettled(fetchPromises);
}

// Helper function to fetch the owner of a token, with retries
async function fetchTokenOwnerWithRetry(restAddress, contractAddress, tokenId, contractType, ownershipData, maxRetries) {
  let attempts = 0;
  while (attempts < maxRetries) {
    try {
      const result = await fetchTokenOwner(restAddress, contractAddress, tokenId, contractType, ownershipData);
      if (result) {
        return; // Exit the loop if successful
      }
    } catch (err) {
      attempts++;
      log(`Retry ${attempts} for token ${tokenId} in contract ${contractAddress} due to error: ${err.message}`, 'WARN');
      if (attempts >= maxRetries) {
        log(`Failed to fetch owner for token ${tokenId} after ${maxRetries} attempts.`, 'ERROR');
      }
      // Wait before retrying (exponential backoff)
      await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, attempts)));
    }
  }
}

// Helper function to fetch the owner of a token
async function fetchTokenOwner(restAddress, contractAddress, tokenId, contractType, ownershipData) {
  if (!/721|1155|404/.test(contractType)) {
    log(`Skipping token ${tokenId} for contract ${contractAddress} as the contract type ${contractType} is not a supported NFT type.`, 'INFO');
    return false;
  }

  log(`Processing token ${tokenId} for contract ${contractAddress}...`, 'DEBUG');
  const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
  const headers = { 'x-cosmos-block-height': config.blockHeight.toString() };

  try {
    const response = await axios.post(`${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`, ownerQueryPayload, {
      headers: { 'Content-Type': 'application/json' }
    });

    if (response.status === 200 && response.data?.data?.owner) {
      const owner = response.data.data.owner;
      log(`Owner for token ${tokenId} found: ${owner}`, 'INFO');
      ownershipData.push([contractAddress, tokenId, owner, contractType]);
      return true;
    } else {
      log(`No valid owner found or unexpected response format for token ${tokenId} in contract ${contractAddress}.`, 'INFO');
      return false;
    }
  } catch (err) {
    if (err.response && err.response.status === 400) {
      log(`Skipping retry for 400 error: ${err.response.data?.message || err.message}`, 'INFO');
      return false; // Do not retry for 400 errors
    }
    log(`Error fetching token ownership for contract ${contractAddress}, token ${tokenId}: ${err.message}`, 'ERROR');
    throw err; // Rethrow for retry handling
  }
}

// Fetch and store CW404 contract details
export async function fetchCW404Details(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    log('Fetching CW404 contracts from the database...', 'DEBUG');
    const cw404Contracts = await dbAll("SELECT address FROM contracts WHERE type = 'cw404'");
    
    if (cw404Contracts.length === 0) {
      log('No CW404 contracts found to process.', 'INFO');
      return;
    }
    log(`Found ${cw404Contracts.length} CW404 contracts to process.`, 'DEBUG');

    // Process each CW404 contract
    for (const { address } of cw404Contracts) {
      try {
        const detailsPayload = { cw404_info: {} };
        const { data, status } = await sendContractQuery(restAddress, address, detailsPayload);

        if (status === 200 && data) {
          const insertSQL = `
            INSERT OR REPLACE INTO contract_details 
            (contract_address, base_uri, max_supply, royalty_percentage) 
            VALUES (?, ?, ?, ?)
          `;
          await dbRun(insertSQL, [address, data.base_uri, data.max_edition, data.royalty_percentage]);
          log(`Stored CW404 details for contract ${address}.`, 'INFO');
        } else {
          log(`No details found for CW404 contract ${address}.`, 'INFO');
        }
      } catch (error) {
        log(`Error processing CW404 contract ${address}: ${error.message}`, 'ERROR');
      }
    }

    log('Finished processing CW404 contract details.', 'INFO');
  } catch (error) {
    log(`Error in fetchCW404Details: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store pointer data for all contracts
export async function fetchAndStorePointerData(pointerApi, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    // Fetch all contract addresses from the database
    log('Fetching contract addresses from the database...', 'DEBUG');
    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contractAddresses = contractsResult.map(row => row.address);

    if (contractAddresses.length === 0) {
      log('No contracts found to process for pointer data.', 'INFO');
      return;
    }
    log(`Found ${contractAddresses.length} contracts to process for pointer data.`, 'DEBUG');

    // Process pointer data in chunks
    const chunkSize = 10;
    for (let i = 0; i < contractAddresses.length; i += chunkSize) {
      const chunk = contractAddresses.slice(i, i + chunkSize);
      const payload = { addresses: chunk };

      try {
        const response = await retryOperation(() => axios.post(pointerApi, payload));

        if (response.status === 200 && Array.isArray(response.data)) {
          // Prepare data for batch insertion
          const batchData = response.data.map(({ address, pointerAddress, pointeeAddress, isBaseAsset, isPointer, pointerType }) => [
            address,
            pointerAddress || null,
            pointeeAddress || null,
            isBaseAsset ? 1 : 0,
            isPointer ? 1 : 0,
            pointerType || null
          ]);

          // Perform batch insert into the pointer_data table, with the new column for pointer_type
          await batchInsert(
            dbRun, 
            'pointer_data', 
            ['contract_address', 'pointer_address', 'pointee_address', 'is_base_asset', 'is_pointer', 'pointer_type'], 
            batchData
          );

          log(`Stored pointer data for ${batchData.length} addresses in the current batch.`, 'DEBUG');
        } else {
          log(`Failed to fetch pointer data. Status: ${response.status}. Response data might be malformed or unexpected.`, 'ERROR');
        }
      } catch (error) {
        log(`Error processing pointer data chunk: ${error.message}`, 'ERROR');
      }
    }

    // Mark the step as completed in the indexer progress table
    await updateProgress(db, 'fetchPointerData', 1);
    log('Finished processing pointer data for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchAndStorePointerData: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store associated wallet addresses based on EVM address lookup
export async function fetchAndStoreAssociatedWallets(evmRpcAddress, db, concurrencyLimit = 5) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    log('Starting fetchAndStoreAssociatedWallets...', 'DEBUG');

    // Fetch unique owners from the 'nft_owners' table
    const owners = await dbAll('SELECT DISTINCT owner FROM nft_owners');
    if (owners.length === 0) {
      log('No unique owners found in nft_owners table.', 'INFO');
      return;
    }
    log(`Found ${owners.length} unique owners in nft_owners table.`, 'DEBUG');

    // Limit the number of concurrent requests to avoid overloading the endpoint
    const processBatch = async (batch) => {
      await Promise.all(batch.map(async (row) => {
        const owner = row.owner;
        log(`Fetching EVM address for owner: ${owner}`, 'DEBUG');

        const payload = {
          jsonrpc: "2.0",
          method: "sei_getEVMAddress",
          params: [owner],
          id: 1
        };

        try {
          // Attempt to fetch the EVM address with retry logic
          const response = await retryOperation(() => axios.post(evmRpcAddress, payload));
          if (response.status === 200 && response.data?.result) {
            const evmAddress = response.data.result;
            log(`Fetched EVM address for ${owner}: ${evmAddress}`, 'DEBUG');

            // Store the EVM address in the 'wallet_associations' table
            const insertSQL = `
              INSERT OR REPLACE INTO wallet_associations (wallet_address, evm_address)
              VALUES (?, ?)
            `;
            await dbRun(insertSQL, [owner, evmAddress]);
            log(`Stored EVM address ${evmAddress} for wallet ${owner}`, 'DEBUG');
          } else {
            log(`No EVM address found for owner: ${owner}`, 'INFO');
          }
        } catch (error) {
          log(`Error processing owner ${owner}: ${error.message}`, 'ERROR');
        }
      }));
    };

    // Process owners in batches to limit concurrency
    for (let i = 0; i < owners.length; i += concurrencyLimit) {
      const batch = owners.slice(i, i + concurrencyLimit);
      log(`Processing batch of ${batch.length} owners...`, 'DEBUG');
      await processBatch(batch);
    }
    log('Finished processing associated wallets.', 'INFO');
  } catch (error) {
    log(`Error in fetchAndStoreAssociatedWallets: ${error.message}`, 'ERROR');
    throw error;
  }
}
