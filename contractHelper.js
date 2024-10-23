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
export async function fetchCodeIds(restAddress, db) {
  const dbRun = promisify(db.run).bind(db);
  try {
    const progress = await checkProgress(db, 'fetchCodeIds');
    if (progress.completed) {
      log('Skipping fetchCodeIds: Already completed', 'INFO');
      return;
    }

    let nextKey = null;
    let allCodeInfos = [];

    while (true) {
      let response;
      try {
        response = await fetchPaginatedData(
          `${restAddress}/cosmwasm/wasm/v1/code`,
          'code_infos',
          {
            paginationType: 'query',
            useNextKey: true,
            limit: config.paginationLimit,
            paginationPayload: {
              'pagination.reverse': false,
              'pagination.limit': config.paginationLimit,
              ...(nextKey && { 'pagination.key': nextKey }),
            }
          }
        );
      } catch (error) {
        log(`Error fetching code IDs: ${error.message}`, 'ERROR');
        throw error;
      }

      if (!Array.isArray(response) || response.length === 0) {
        log('No more code IDs to fetch.', 'INFO');
        break;
      }

      // Batch insert code info into the correct columns
      const batchData = response.map(({ code_id, creator, data_hash, instantiate_permission }) => 
        [code_id, creator, data_hash, JSON.stringify(instantiate_permission)]
      );
      await batchInsert(
        dbRun, 
        'code_ids', 
        ['code_id', 'creator', 'data_hash', 'instantiate_permission'], 
        batchData
      );

      allCodeInfos = allCodeInfos.concat(response);

      // Check for pagination next key
      if (response.pagination?.next_key) {
        nextKey = response.pagination.next_key;
      } else {
        break;
      }

      await updateProgress(db, 'fetchCodeIds', 0, response[response.length - 1].code_id);
    }
    
    if (allCodeInfos.length > 0) {
      log(`Total code IDs fetched and stored: ${allCodeInfos.length}`, 'INFO');
      await updateProgress(db, 'fetchCodeIds', 1);
    }
  } catch (error) {
    log(`Error in fetchCodeIds: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch contract addresses by code and store them in the database
export async function fetchContractAddressesByCodeId(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  try {
    const progress = await checkProgress(db, 'fetchContractsByCode');
    if (progress.completed) {
      log('Skipping fetchContractAddressesByCodeId: Already completed', 'INFO');
      return;
    }

    const codeIds = (await dbAll('SELECT code_id FROM code_ids')).map(row => row.code_id);
    const startIndex = progress.last_processed ? codeIds.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < codeIds.length; i++) {
      const code_id = codeIds[i];
      let nextKey = null;
      let allContracts = [];

      while (true) {
        let response;
        try {
          response = await fetchPaginatedData(
            `${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts`,
            'contracts',
            {
              paginationType: 'query',
              useNextKey: true, // Use next_key for conventional pagination
              limit: config.paginationLimit,
              paginationPayload: {
                'pagination.limit': config.paginationLimit,
                ...(nextKey && { 'pagination.key': nextKey }),
              },
              logLevel: 'DETAILED', // Optional: 'DETAILED' or 'FINAL_ONLY'
            }
          );
        } catch (error) {
          log(`Error fetching contracts for code_id ${code_id}: ${error.message}`, 'ERROR');
          throw error;
        }

        if (!Array.isArray(response) || response.length === 0) {
          log(`No contracts found for code_id ${code_id}`, 'INFO');
          break;
        }

        const batchData = response.map(contractAddress => [code_id, contractAddress, null]);
        await batchInsert(dbRun, 'contracts', ['code_id', 'address', 'type'], batchData);
        log(`Inserted ${response.length} contracts for code_id ${code_id}`, 'INFO');
        allContracts = allContracts.concat(response);

        // Check for next key for further pagination
        if (response.pagination?.next_key) {
          nextKey = response.pagination.next_key;
        } else {
          break; // End if no more data
        }
      }

      // Update progress after each code_id processing
      await updateProgress(db, 'fetchContractsByCode', 0, code_id);
    }

    // Mark the entire step as complete
    await updateProgress(db, 'fetchContractsByCode', 1);
    log('All contract addresses fetched and stored successfully.', 'INFO');

  } catch (error) {
    log(`Error in fetchContractAddressesByCodeId: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store metadata for each contract address
export async function fetchContractMetadata(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const batchSize = 5;
  const delayBetweenBatches = 100;

  try {
    const progress = await checkProgress(db, 'fetchContractMetadata');
    if (progress.completed) {
      log('Skipping fetchContractMetadata: Already completed', 'INFO');
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
          } else {
            log(`Unexpected response status ${response.status} for contract ${contractAddress}`, 'ERROR');
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

      // Update progress with the last contract in the batch
      await updateProgress(db, 'fetchContractMetadata', 0, batch[batch.length - 1]);
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
    }

    // Mark the entire step as complete
    await updateProgress(db, 'fetchContractMetadata', 1);
    log('Finished processing metadata for all contracts.', 'INFO');

  } catch (error) {
    log(`Error in fetchContractMetadata: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store contract history per contract with parallel processing and batch insertions
export async function fetchContractHistory(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const batchSize = 10;

  try {
    const progress = await checkProgress(db, 'fetchContractHistory');
    if (progress.completed) {
      log('Skipping fetchContractHistory: Already completed', 'INFO');
      return;
    }

    const contractAddresses = (await dbAll('SELECT address FROM contracts')).map(row => row.address);
    const startIndex = progress.last_processed ? contractAddresses.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contractAddresses.length; i += batchSize) {
      const batch = contractAddresses.slice(i, i + batchSize);
      const historyPromises = batch.map(async address => {
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
          
          // Update progress after processing each contract
          await updateProgress(db, 'fetchContractHistory', 0, address);
        } catch (error) {
          log(`Error fetching history for contract ${address}: ${error.message}`, 'ERROR');
        }
      });

      await Promise.all(historyPromises);
      log(`Processed a batch of ${batch.length} contracts`, 'INFO');
    }

    // Mark the entire step as complete
    await updateProgress(db, 'fetchContractHistory', 1);
    log('Finished processing history for all contracts.', 'INFO');

  } catch (error) {
    log(`Error in fetchContractHistory: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Identify contract types and store them in the database using parallel processing and batch inserts
export async function identifyContractTypes(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const batchSize = 50;
  const parallelRequests = 10;
  const identifiedTypes = new Set(); // Track identified contract types

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
              if (!identifiedTypes.has(contractType)) {
                log(`New contract type identified: ${contractType}`, 'INFO');
                identifiedTypes.add(contractType);
              }
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

      // Insert into the database when the batch size reaches the limit
      if (batchData.length >= batchSize) {
        await batchInsert(dbRun, 'contracts', ['type', 'address'], batchData);
        batchData = [];
        await updateProgress(db, 'identifyContractTypes', 0, batch[batch.length - 1]);
      }
    }

    // Insert any remaining data in batchData
    if (batchData.length > 0) {
      await batchInsert(dbRun, 'contracts', ['type', 'address'], batchData);
    }

    // Mark the entire step as complete
    await updateProgress(db, 'identifyContractTypes', 1);
    log('Finished identifying types for all contracts.', 'INFO');

    // Log all identified contract types at the end
    if (identifiedTypes.size > 0) {
      log(`Identified contract types: ${Array.from(identifiedTypes).join(', ')}`, 'INFO');
    } else {
      log('No contract types were identified during processing.', 'INFO');
    }

  } catch (error) {
    log(`Error in identifyContractTypes: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch tokens, ownership data, and CW404 details in a single pass
export async function fetchTokensAndOwners(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const parallelRequests = 10;
  const tokenLimit = 100;

  try {
    const progress = await checkProgress(db, 'processTokens');
    const startIndex = progress.last_processed
      ? (await dbAll("SELECT rowid FROM contracts WHERE address = ?", [progress.last_processed]))[0].rowid
      : 0;

    const contractsResult = await dbAll(
      "SELECT rowid, address, type FROM contracts WHERE (token_ids IS NULL OR token_ids = '') AND (type LIKE 'cw404%' OR type LIKE 'cw721%' OR type LIKE 'cw1155%') AND rowid > ?",
      [startIndex]
    );

    for (let i = 0; i < contractsResult.length; i += parallelRequests) {
      const batch = contractsResult.slice(i, i + parallelRequests);
      const processingPromises = batch.map(async ({ address: contractAddress, type: contractType }) => {
        try {
          // Fetch all tokens for the contract
          const allTokensFetched = await fetchAllTokensWithOwnership(restAddress, contractAddress, contractType, tokenLimit, progress.last_fetched_token);

          if (allTokensFetched.tokens.length > 0) {
            // Update tokens in the database
            const tokenIdsString = allTokensFetched.tokens.join(',');
            await dbRun("UPDATE contracts SET token_ids = ? WHERE address = ?", [tokenIdsString, contractAddress]);
            log(`Updated token_ids for contract ${contractAddress}: ${tokenIdsString}`, 'INFO');

            // Insert token data into contract_tokens table
            const tokenInsertData = allTokensFetched.tokens.map(tokenId => [contractAddress, tokenId, contractType]);
            await batchInsert(dbRun, 'contract_tokens', ['contract_address', 'token_id', 'contract_type'], tokenInsertData);
            log(`Inserted ${allTokensFetched.tokens.length} tokens into contract_tokens for contract ${contractAddress}`, 'INFO');

            // Insert ownership data if available
            if (allTokensFetched.ownershipData.length > 0) {
              await batchInsert(dbRun, 'nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], allTokensFetched.ownershipData);
              log(`Inserted ${allTokensFetched.ownershipData.length} ownership records into nft_owners table.`, 'INFO');
            }
          }

          // Fetch CW404 details if applicable
          if (/cw404/i.test(contractType)) {
            await fetchAndStoreCW404Details(restAddress, contractAddress, dbRun);
          }

          // Update progress after each contract's processing
          await updateProgress(db, 'processTokens', 0, contractAddress, allTokensFetched.lastFetchedToken);
        } catch (error) {
          log(`Error processing contract ${contractAddress}: ${error.message}`, 'ERROR');
        }
      });

      await Promise.all(processingPromises);
      log(`Processed a batch of ${batch.length} contracts`, 'INFO');
    }

    // Mark the entire step as complete
    await updateProgress(db, 'processTokens', 1);
    log('Finished processing tokens and ownership for all relevant contracts.', 'INFO');
  } catch (error) {
    log(`Error in processTokensAndOwners: ${error.message}`, 'ERROR');
    throw error;
  }
}

async function fetchAllTokensWithOwnership(restAddress, contractAddress, contractType, tokenLimit) {
  let allTokens = [];
  let ownershipData = [];
  let lastTokenFetched = null;
  let retryCount = 0;
  const maxRetries = 3;

  // Initial query without pagination
  const tokenQueryPayload = { all_tokens: { limit: tokenLimit } };
  const requestUrl = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`;

  try {
    // Log request details at the start
    log(`Attempting to fetch tokens for ${contractAddress}`, 'INFO');
    log(`Request URL: ${requestUrl}`, 'INFO');
    log(`Payload: ${JSON.stringify(tokenQueryPayload)}`, 'INFO');

    const response = await axios.post(
      requestUrl,
      tokenQueryPayload,
      { headers: { 'Content-Type': 'application/json' } }
    );

    if (response.status === 200 && response.data?.data?.tokens?.length > 0) {
      const tokenIds = response.data.data.tokens;
      allTokens = allTokens.concat(tokenIds);
      log(`Fetched ${allTokens.length} tokens for contract ${contractAddress}`, 'INFO');
      lastTokenFetched = tokenIds[tokenIds.length - 1];

      if (/721|1155|404/i.test(contractType)) {
        const ownershipBatch = await fetchTokenOwnershipData(restAddress, contractAddress, tokenIds, contractType);
        ownershipData = ownershipData.concat(ownershipBatch);
      }

      // Paginate if necessary
      if (tokenIds.length === tokenLimit) {
        await paginateTokenFetch(restAddress, contractAddress, contractType, tokenLimit, lastTokenFetched, allTokens, ownershipData);
      }
    } else {
      log(`No tokens found for contract ${contractAddress}`, 'INFO');
    }
  } catch (error) {
    logErrorHandling(error, contractAddress, requestUrl, tokenQueryPayload, retryCount, maxRetries);
  }

  return { tokens: allTokens, ownershipData, lastFetchedToken: lastTokenFetched };
}

function logErrorHandling(error, contractAddress, requestUrl, tokenQueryPayload, retryCount, maxRetries) {
  // Log the request details even if an error occurs
  log(`Error fetching tokens for ${contractAddress}`, 'ERROR');
  log(`Request URL: ${requestUrl}`, 'ERROR');
  log(`Payload: ${JSON.stringify(tokenQueryPayload)}`, 'ERROR');

  if (error.response && error.response.data) {
    log(`Response Data: ${JSON.stringify(error.response.data)}`, 'ERROR');
  } else {
    log(`Error Message: ${error.message}`, 'ERROR');
  }

  if (error.response && error.response.status === 404 && retryCount < maxRetries) {
    retryCount++;
    log(`Retry ${retryCount}/${maxRetries} with simplified payload for contract ${contractAddress}`, 'WARN');
  }
}

async function paginateTokenFetch(restAddress, contractAddress, contractType, tokenLimit, startAfter, allTokens, ownershipData) {
  let retryCount = 0;
  const maxRetries = 3;

  while (true) {
    const tokenQueryPayload = {
      all_tokens: {
        limit: tokenLimit,
        start_after: startAfter
      }
    };
    const requestUrl = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`;

    try {
      log(`Fetching paginated tokens for ${contractAddress} with payload: ${JSON.stringify(tokenQueryPayload)}`, 'DEBUG');
      log(`Request URL: ${requestUrl}`, 'DEBUG');
      
      const response = await axios.post(
        requestUrl,
        tokenQueryPayload,
        { headers: { 'Content-Type': 'application/json' } }
      );

      if (response.status === 200 && response.data?.data?.tokens?.length > 0) {
        const tokenIds = response.data.data.tokens;
        allTokens.push(...tokenIds);
        log(`Fetched ${allTokens.length} total tokens for contract ${contractAddress}`, 'INFO');
        startAfter = tokenIds[tokenIds.length - 1];

        if (/721|1155|404/i.test(contractType)) {
          const ownershipBatch = await fetchTokenOwnershipData(restAddress, contractAddress, tokenIds, contractType);
          ownershipData.push(...ownershipBatch);
        }

        if (tokenIds.length < tokenLimit) break;
      } else {
        log(`No more tokens found for contract ${contractAddress}`, 'INFO');
        break;
      }
    } catch (error) {
      if (!handlePaginationError(error, contractAddress, requestUrl, tokenQueryPayload, retryCount, maxRetries)) break;
    }
  }
}

function handlePaginationError(error, contractAddress, requestUrl, tokenQueryPayload, retryCount, maxRetries) {
  log(`Error during pagination for contract ${contractAddress}: ${error.message}`, 'ERROR');
  log(`Request URL: ${requestUrl}`, 'ERROR');
  log(`Payload: ${JSON.stringify(tokenQueryPayload)}`, 'ERROR');

  if (error.response && error.response.status === 404 && retryCount < maxRetries) {
    retryCount++;
    log(`Retry ${retryCount}/${maxRetries} for pagination`, 'WARN');
    return true; // Continue retrying
  }

  return false; // Stop pagination attempts
}

// Fetch token ownership data for a batch of tokens
export async function fetchTokenOwnershipData(restAddress, contractAddress, tokenIds, contractType) {
  const ownershipData = [];
  const ownershipPromises = tokenIds.map(async tokenId => {
    try {
      const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
      const response = await retryOperation(() => axios.post(
        `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`,
        ownerQueryPayload,
        { headers: { 'Content-Type': 'application/json' } }
      ));

      if (response.status === 200 && response.data?.data?.owner) {
        const owner = response.data.data.owner;
        ownershipData.push([contractAddress, tokenId, owner, contractType]);
      }
    } catch (error) {
      log(`Error fetching ownership for token ${tokenId} in contract ${contractAddress}: ${error.message}`, 'ERROR');
    }
  });

  await Promise.allSettled(ownershipPromises);
  return ownershipData;
}

// Fetch and store CW404 contract details
export async function fetchCW404Details(restAddress, contractAddress, dbRun) {
  try {
    const detailsPayload = { cw404_info: {} };
    const response = await sendContractQuery(restAddress, contractAddress, detailsPayload);

    if (response.status === 200 && response.data) {
      const insertSQL = `
        INSERT OR REPLACE INTO contract_details 
        (contract_address, base_uri, max_supply, royalty_percentage) 
        VALUES (?, ?, ?, ?)
      `;
      await dbRun(insertSQL, [contractAddress, response.data.base_uri, response.data.max_edition, response.data.royalty_percentage]);
      log(`Stored CW404 details for contract ${contractAddress}.`, 'INFO');
    } else {
      log(`No details found for CW404 contract ${contractAddress}.`, 'INFO');
    }
  } catch (error) {
    log(`Error processing CW404 contract ${contractAddress}: ${error.message}`, 'ERROR');
  }
}

// Helper function to process pointer data
export async function fetchPointerData(pointerApi, contractAddresses, dbRun, chunkSize = 10) {
  for (let i = 0; i < contractAddresses.length; i += chunkSize) {
      const chunk = contractAddresses.slice(i, i + chunkSize);
      const payload = { addresses: chunk };
      
      try {
          const response = await retryOperation(() => axios.post(pointerApi, payload));
          
          if (response.status === 200 && Array.isArray(response.data)) {
              const batchData = response.data.map(({ address, pointerAddress, pointeeAddress, isBaseAsset, isPointer, pointerType }) => [
                  address,
                  pointerAddress || null,
                  pointeeAddress || null,
                  isBaseAsset ? 1 : 0,
                  isPointer ? 1 : 0,
                  pointerType || null
              ]);
              
              await batchInsert(
                  dbRun, 
                  'pointer_data', 
                  ['contract_address', 'pointer_address', 'pointee_address', 'is_base_asset', 'is_pointer', 'pointer_type'], 
                  batchData
              );
              
              log(`Stored pointer data for ${batchData.length} addresses in the current batch.`, 'DEBUG');
          } else {
              log(`Unexpected response or empty data while fetching pointer data. Status: ${response.status}`, 'ERROR');
          }
      } catch (error) {
          log(`Error processing pointer data chunk: ${error.message}`, 'ERROR');
      }
  }
}

// Helper function to process associated wallets
export async function fetchAssociatedWallets(evmRpcAddress, db, concurrencyLimit = 5) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  
  try {
      log('Starting fetchAssociatedWallets...', 'DEBUG');
      
      // Fetch unique owners from the 'nft_owners' table
      const owners = await dbAll('SELECT DISTINCT owner FROM nft_owners');
      if (owners.length === 0) {
          log('No unique owners found in nft_owners table.', 'INFO');
          return;
      }
      log(`Found ${owners.length} unique owners in nft_owners table.`, 'DEBUG');
      
      // Process owners in batches to limit concurrency
      for (let i = 0; i < owners.length; i += concurrencyLimit) {
          const batch = owners.slice(i, i + concurrencyLimit);
          await Promise.all(batch.map(async ({ owner }) => {
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
                      await batchInsert(dbRun, 'wallet_associations', ['wallet_address', 'evm_address'], [[owner, evmAddress]]);
                      log(`Stored EVM address ${evmAddress} for wallet ${owner}`, 'DEBUG');
                  } else {
                      log(`No EVM address found for owner: ${owner}`, 'INFO');
                  }
              } catch (error) {
                  log(`Error processing owner ${owner}: ${error.message}`, 'ERROR');
              }
          }));
      }
      log('Finished processing associated wallets.', 'INFO');
  } catch (error) {
      log(`Error in fetchAssociatedWallets: ${error.message}`, 'ERROR');
      throw error;
  }
}
