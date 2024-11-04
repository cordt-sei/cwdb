import { 
  fetchPaginatedData, 
  sendContractQuery, 
  retryOperation, 
  log,
  batchInsertOrUpdate,
  checkProgress,
  updateProgress,
  db
} from './utils.js';
import axios from 'axios';
import pLimit from 'p-limit';
import { config } from './config.js';

// fetch all code IDs
export async function fetchCodeIds(restAddress) {
  try {
    const progress = checkProgress('fetchCodeIds');
    if (progress.completed) {
      log('Skipping fetchCodeIds: Already completed', 'INFO');
      return;
    }

    let nextKey = null;
    let totalRecorded = 0;
    let batchCount = 0; // Track the number of batches for clearer pagination context

    while (true) {
      const response = await fetchPaginatedData(
        `${restAddress}/cosmwasm/wasm/v1/code`,
        'code_infos',
        {
          limit: config.paginationLimit,
          nextKey,
          retries: config.retryConfig.retries,
          delay: config.retryConfig.delay,
          backoffFactor: config.retryConfig.backoffFactor,
        }
      );

      if (!Array.isArray(response) || response.length === 0) {
        log('All code IDs recorded; no additional data found.', 'INFO');
        break;
      }

      const batchData = response.map(({ code_id, creator, instantiate_permission }) => 
        [code_id, creator, JSON.stringify(instantiate_permission)]
      );

      // Batch insert and log each processed batch
      db.transaction(() => {
        batchInsertOrUpdate('code_ids', ['code_id', 'creator', 'instantiate_permission'], batchData, 'code_id');
      })();

      batchCount += 1;
      totalRecorded += batchData.length;

      log(`Batch ${batchCount}: Recorded ${batchData.length} code IDs.`, 'INFO');

      nextKey = response.pagination?.next_key || null;
      if (!nextKey) {
        log('No further pagination key found; pagination ended.', 'INFO');
        break;
      } else {
        log(`Pagination continues; moving to the next page for code IDs.`, 'INFO');
      }

      updateProgress('fetchCodeIds', 0, response[response.length - 1].code_id);
    }

    // Final log for total number of code IDs recorded
    if (totalRecorded > 0) {
      log(`Total code IDs fetched and stored: ${totalRecorded}`, 'INFO');
      updateProgress('fetchCodeIds', 1);
    } else {
      log('No new code IDs recorded.', 'INFO');
    }
  } catch (error) {
    log(`Error in fetchCodeIds: ${error.message}`, 'ERROR');
    throw error;
  }
}

// fetchContractAddressesByCodeId with unified progress tracking identifier
export async function fetchContractAddressesByCodeId(restAddress) {
  try {
    const progress = checkProgress('fetchContractsByCode');  // Unified identifier
    if (progress.completed) {
      log('Skipping fetchContractsByCode: Already completed', 'INFO');
      return;
    }

    const codeIds = db.prepare('SELECT code_id FROM code_ids').all().map(row => row.code_id);
    const startIndex = progress.last_processed ? codeIds.indexOf(progress.last_processed) + 1 : 0;
    const limit = pLimit(config.concurrencyLimit);
    let totalContracts = 0;

    const fetchPromises = codeIds.slice(startIndex).map(code_id => limit(async () => {
      log(`Fetching contracts for code_id ${code_id}`, 'INFO');

      let allContracts = [];
      let nextKey = null;
      let page = 1;

      while (true) {
        const paginatedUrl = `${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts${nextKey ? `?pagination.key=${encodeURIComponent(nextKey)}` : ''}`;
        log(`Fetching data for code_id ${code_id}, page ${page}`, 'DEBUG');

        const response = await fetchPaginatedData(paginatedUrl, 'contracts', {
          limit: config.paginationLimit,
          retries: config.retryConfig.retries,
          delay: config.retryConfig.delay,
          backoffFactor: config.retryConfig.backoffFactor,
        });

        if (response.length > 0) {
          allContracts.push(...response);
          log(`Fetched ${response.length} items for code_id ${code_id} on page ${page}`, 'DEBUG');
        } else {
          log(`No more contracts found for code_id ${code_id} on page ${page}`, 'INFO');
          break;
        }

        if (response.length < config.paginationLimit) break;
        nextKey = response.pagination?.next_key || null;
        if (!nextKey) break;
        page += 1;
      }

      const contractCount = allContracts.length;
      totalContracts += contractCount;

      if (contractCount > 0) {
        const batchData = allContracts.map(addr => [code_id, addr, null]);
        await batchInsertOrUpdate('contracts', ['code_id', 'address', 'type'], batchData, 'address');
        log(`Recorded ${contractCount} contracts for code_id ${code_id}`, 'INFO');
      } else {
        log(`No contracts found for code_id ${code_id}`, 'INFO');
      }

      updateProgress('fetchContractsByCode', 0, code_id);  // Unified identifier
    }));

    await Promise.allSettled(fetchPromises);
    updateProgress('fetchContractsByCode', 1);  // Unified identifier
    log(`Completed fetching contract addresses for all code IDs. Total contracts recorded: ${totalContracts}`, 'INFO');
  } catch (error) {
    log(`Error in fetchContractAddressesByCodeId: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch contract history for each contract address concurrently
export async function fetchContractHistory(restAddress) {
  try {
    const contracts = db.prepare('SELECT address FROM contracts').all().map(row => row.address);
    const limit = pLimit(config.concurrencyLimit);

    const historyPromises = contracts.map(contractAddress => limit(async () => {
      log(`Fetching contract history for ${contractAddress}`, 'INFO');
      let nextKey = null;

      while (true) {
        const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/history`;
        const requestUrl = nextKey ? `${url}?pagination.key=${encodeURIComponent(nextKey)}` : url;

        const response = await retryOperation(() => axios.get(requestUrl));
        if (!response || response.status !== 200 || !response.data) {
          log(`Failed to fetch history for ${contractAddress}. Status: ${response?.status}`, 'ERROR');
          break;
        }

        const { entries, pagination } = response.data;
        if (!Array.isArray(entries) || entries.length === 0) {
          log(`No history entries found for ${contractAddress}`, 'INFO');
          break;
        }

        // Prepare data for batch insert with proper escaping for SQLite
        const insertData = entries.map(entry => [
          contractAddress,
          entry.operation || '', // Handle nulls with fallback
          entry.code_id || '',   // Handle nulls with fallback
          entry.updated || '',   // Handle nulls with fallback
          JSON.stringify(entry.msg).replace(/"/g, '""').replace(/\\/g, '\\\\') // Double escape quotes and backslashes
        ]);

        try {
          await batchInsertOrUpdate(
            'contract_history',
            ['contract_address', 'operation', 'code_id', 'updated', 'msg'],
            insertData,
            'contract_address'
          );
          log(`Inserted ${entries.length} history entries for ${contractAddress}`, 'DEBUG');
        } catch (dbError) {
          log(`Error inserting history for ${contractAddress}: ${dbError.message}`, 'ERROR');
          log(`Insert data: ${JSON.stringify(insertData)}`, 'ERROR'); // Log insert data for debugging
        }

        nextKey = pagination?.next_key || null;
        if (!nextKey) break;
      }
    }));

    await Promise.allSettled(historyPromises);
    log('Completed fetching contract history for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchContractHistory: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store metadata for each contract address in concurrent batches
export async function fetchContractMetadata(restAddress) {
  const batchSize = 50;  // Using 50 as a reasonable batch size
  const delayBetweenBatches = 100;

  try {
    // Check the current progress for this step
    const progress = checkProgress('fetchContractMetadata');
    if (progress.completed) {
      log('Skipping fetchContractMetadata: Already completed', 'INFO');
      return;
    }

    // Fetch all contract addresses and determine the starting point
    const contractAddresses = db.prepare('SELECT address FROM contracts').all().map(row => row.address);
    const totalContracts = contractAddresses.length;
    const startIndex = progress.last_processed ? contractAddresses.indexOf(progress.last_processed) + 1 : 0;

    log(`Starting fetchContractMetadata for ${totalContracts} total contracts. Resuming from index ${startIndex}.`, 'INFO');

    for (let i = startIndex; i < totalContracts; i += batchSize) {
      const batch = contractAddresses.slice(i, i + batchSize);
      const limit = pLimit(config.concurrencyLimit);

      const fetchPromises = batch.map(contractAddress => limit(async () => {
        try {
          const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}`;
          const response = await retryOperation(() => axios.get(url));

          if (response?.status === 200 && response.data?.contract_info) {
            const { code_id, creator, admin, label } = response.data.contract_info;
            return [contractAddress, code_id, creator, admin, label];
          }
        } catch (error) {
          log(`Failed to fetch metadata for ${contractAddress}: ${error.message}`, 'ERROR');
        }
        return null;
      }));

      const results = (await Promise.all(fetchPromises)).filter(Boolean); // Filter out any null results

      if (results.length > 0) {
        await batchInsertOrUpdate('contracts', ['address', 'code_id', 'creator', 'admin', 'label'], results, 'address');
        log(`Batch inserted metadata for ${results.length} contracts (${i + results.length} out of ${totalContracts})`, 'INFO');
      } else {
        log(`No valid metadata found for batch starting with contract ${batch[0]}`, 'INFO');
      }

      // Update progress with the last processed contract address
      await updateProgress('fetchContractMetadata', 0, batch[batch.length - 1]);
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches)); // Throttle between batches
    }

    // Mark the step as completed after processing all contracts
    await updateProgress('fetchContractMetadata', 1);
    log(`Finished processing metadata for all ${totalContracts} contracts.`, 'INFO');
  } catch (error) {
    log(`Error in fetchContractMetadata: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Identify contract types concurrently with batch inserts and progress logging
export async function identifyContractTypes(restAddress) {
  try {
    const contracts = db.prepare('SELECT address FROM contracts').all().map(row => row.address);
    const progress = checkProgress('identifyContractTypes');
    const startIndex = progress.last_processed ? contracts.indexOf(progress.last_processed) + 1 : 0;
    const batchSize = 50;
    let batchData = [];
    let processedCount = 0;

    const limit = pLimit(config.concurrencyLimit);

    const typePromises = contracts.slice(startIndex).map(contractAddress => limit(async () => {
      let contractType;
      const testPayload = { "a": "b" }; // Intentionally incorrect payload to trigger an error response

      try {
        // Send a query to check the contract type
        const response = await sendContractQuery(restAddress, contractAddress, testPayload, false, true);

        // Log the entire response message for debugging purposes
        if (response?.message) {
          log(`Debug: Full error message for ${contractAddress}: ${response.message}`, 'DEBUG');
          
          // Attempt to extract the contract type from the error message, if available
          const match = response.message.match(/Error parsing into type ([\w]+)::/);
          if (match) {
            contractType = match[1]; // Capture only the base type
            log(`Identified contract type for ${contractAddress}: ${contractType}`, 'INFO');
          } else {
            log(`No recognizable type in error message for contract ${contractAddress}`, 'INFO');
          }          
        } else {
          log(`No 'message' field in response for contract ${contractAddress}`, 'DEBUG');
        }

        // If no contract type was identified, set to 'unknown'
        if (!contractType) {
          log(`Unexpected format for contract ${contractAddress}. Full response: ${JSON.stringify(response)}`, 'DEBUG');
          contractType = 'unknown';
        }

        // Add to batch data
        batchData.push([contractAddress, contractType]);
        processedCount++;

        // Insert batch data when reaching batch size
        if (batchData.length >= batchSize) {
          await batchInsertOrUpdate('contracts', ['address', 'type'], batchData, 'address');
          batchData = []; // Clear batch after insert
          await updateProgress('identifyContractTypes', 0, contractAddress); // Update last processed contract
        }

        // Periodic logging for every 100 contracts or at completion
        if (processedCount % 100 === 0 || processedCount === contracts.length) {
          log(`Progress: Processed ${processedCount} / ${contracts.length} contracts`, 'INFO');
        }
      } catch (error) {
        // Log any unexpected errors that aren't related to type parsing
        if (error?.response?.status !== 400) {
          log(`Error determining contract type for ${contractAddress}: ${error.message}`, 'ERROR');
        }
      }
    }));

    await Promise.allSettled(typePromises);

    // Insert remaining contracts if any are left in the batch
    if (batchData.length > 0) {
      await batchInsertOrUpdate('contracts', ['address', 'type'], batchData, 'address');
      log(`Final batch inserted contract types for ${batchData.length} contracts`, 'DEBUG');
    }

    // Mark the step as completed
    await updateProgress('identifyContractTypes', 1); 
    log(`Finished identifying contract types for all contracts.`, 'INFO');
  } catch (error) {
    log(`Error in identifyContractTypes: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch tokens and their owners for relevant contracts
export async function fetchTokensAndOwners(restAddress) {
  const delayBetweenBatches = 100;
  const concurrencyLimit = config.concurrencyLimit || 5; // Max concurrent ownership requests
  const limit = pLimit(concurrencyLimit);

  try {
    const progress = checkProgress('fetchTokensAndOwners');
    const contracts = db.prepare("SELECT address, type FROM contracts WHERE type IN ('cw721', 'cw1155')").all();
    const startIndex = progress.last_processed ? contracts.findIndex(contract => contract.address === progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contracts.length; i++) {
      const { address: contractAddress, type: contractType } = contracts[i];
      let allTokens = [];
      let ownershipData = [];
      let lastTokenFetched = null;

      log(`Fetching tokens for contract ${contractAddress}`, 'INFO');

      // Paginated token fetch loop
      while (true) {
        const tokenQueryPayload = {
          all_tokens: {
            limit: config.paginationLimit,
            ...(lastTokenFetched && { start_after_id: lastTokenFetched })
          }
        };

        const response = await sendContractQuery(restAddress, contractAddress, tokenQueryPayload);

        if (response?.tokens?.length > 0) {
          const tokenIds = response.tokens;
          allTokens.push(...tokenIds);
          lastTokenFetched = tokenIds[tokenIds.length - 1];
          log(`Fetched ${tokenIds.length} tokens for contract ${contractAddress}. Total: ${allTokens.length}`, 'DEBUG');

          // Ownership lookup for each token in parallel
          if (/721|1155/i.test(contractType)) {
            const ownershipPromises = tokenIds.map(tokenId => limit(async () => {
              const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
              const ownerResponse = await retryOperation(() => sendContractQuery(restAddress, contractAddress, ownerQueryPayload));

              if (ownerResponse?.owner) {
                ownershipData.push([contractAddress, tokenId, ownerResponse.owner, contractType]);
                log(`Fetched ownership for token ${tokenId}: owner is ${ownerResponse.owner}`, 'DEBUG');
              }
            }));

            await Promise.allSettled(ownershipPromises); // Wait for all ownership fetches for current tokens
          }

          // Break out if fewer tokens than the limit were fetched, indicating the last page
          if (tokenIds.length < config.paginationLimit) break;
        } else {
          log(`No more tokens found for contract ${contractAddress}`, 'INFO');
          break;
        }
      }

      if (allTokens.length > 0) {
        // Batch insert token IDs and ownership data
        await batchInsertOrUpdate('contracts', ['address', 'token_ids'], [[contractAddress, allTokens.join(',')]], 'address');
        log(`Updated token_ids for ${contractAddress}`, 'INFO');

        const tokenInsertData = allTokens.map(tokenId => [contractAddress, tokenId, contractType]);
        await batchInsertOrUpdate('contract_tokens', ['contract_address', 'token_id', 'contract_type'], tokenInsertData, ['contract_address', 'token_id']);
        log(`Inserted ${allTokens.length} tokens into contract_tokens for ${contractAddress}`, 'INFO');

        if (ownershipData.length > 0) {
          await batchInsertOrUpdate('nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData, ['collection_address', 'token_id']);
          log(`Inserted ${ownershipData.length} ownership records into nft_owners`, 'INFO');
        }
      }

      // Update progress to avoid re-fetching on restart
      await updateProgress('fetchTokensAndOwners', 0, contractAddress, lastTokenFetched);
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches)); // Delay to manage API rate limits
    }

    updateProgress('fetchTokensAndOwners', 1); // Mark progress as complete
    log('Finished processing tokens and ownership for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchTokensAndOwners: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch pointer data and store it in the database
export async function fetchPointerData(pointerApi, contractAddresses, chunkSize = 10) {
  // Check if contractAddresses is a valid array
  if (!Array.isArray(contractAddresses) || contractAddresses.length === 0) {
    log(`Error: 'contractAddresses' is either undefined or not an array`, 'ERROR');
    return; // Exit function if contractAddresses is invalid
  }

  for (let i = 0; i < contractAddresses.length; i += chunkSize) {
    const chunk = contractAddresses.slice(i, i + chunkSize); // Get a chunk of addresses for each batch
    const payload = { addresses: chunk }; // Prepare payload

    try {
      // Send the batch request to the pointer API
      const response = await retryOperation(() => axios.post(pointerApi, payload));

      if (response.status === 200 && Array.isArray(response.data)) {
        // Parse the response to extract necessary fields for each address
        const batchData = response.data.map(({ address, pointerAddress, pointeeAddress, isBaseAsset, isPointer, pointerType }) => [
          address,
          pointerAddress || null,
          pointeeAddress || null,
          isBaseAsset ? 1 : 0,
          isPointer ? 1 : 0,
          pointerType || null
        ]);

        // Insert the batch data into the database
        await batchInsertOrUpdate('pointer_data', ['contract_address', 'pointer_address', 'pointee_address', 'is_base_asset', 'is_pointer', 'pointer_type'], batchData, 'contract_address');
        log(`Stored pointer data for ${batchData.length} addresses`, 'DEBUG');
      } else {
        log(`Unexpected or empty data while fetching pointer data. Status: ${response.status}`, 'ERROR');
      }
    } catch (error) {
      log(`Error processing pointer data chunk: ${error.message}`, 'ERROR');
    }
  }
  log('Finished fetching pointer data for all addresses.', 'INFO');
}

// Helper function to process associated wallets
export async function fetchAssociatedWallets(evmRpcAddress, concurrencyLimit = 5) {
  try {
    log('Starting fetchAssociatedWallets...', 'DEBUG');
    
    const owners = db.prepare('SELECT DISTINCT owner FROM nft_owners').all();
    if (owners.length === 0) {
      log('No unique owners found in nft_owners table.', 'DEBUG');
      return;
    }
    
    log(`Found ${owners.length} unique owners`, 'DEBUG');
    
    for (let i = 0; i < owners.length; i += concurrencyLimit) {
      const batch = owners.slice(i, i + concurrencyLimit);
      await Promise.all(batch.map(async ({ owner }) => {
        log(`Fetching EVM address for ${owner}`, 'DEBUG');
        
        const payload = {
          jsonrpc: "2.0",
          method: "sei_getEVMAddress",
          params: [owner],
          id: 1
        };
        
        try {
          const response = await retryOperation(() => axios.post(evmRpcAddress, payload));
          
          if (response.status === 200 && response.data?.result) {
            await batchInsertOrUpdate('wallet_associations', ['wallet_address', 'evm_address'], [[owner, response.data.result]], 'wallet_address');
            log(`Stored EVM address for ${owner}`, 'DEBUG');
          } else {
            log(`No EVM address found for ${owner}`, 'DEBUG');
          }
        } catch (error) {
          log(`Error processing ${owner}: ${error.message}`, 'ERROR');
        }
      }));
    }
    
    log('Finished processing associated wallets.', 'INFO');
  } catch (error) {
    log(`Error in fetchAssociatedWallets: ${error.message}`, 'ERROR');
    throw error;
  }
}

export { axios };