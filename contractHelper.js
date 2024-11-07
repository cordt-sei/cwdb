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

/// Fetch and store metadata for each contract address in concurrent batches
export async function fetchContractMetadata(restAddress) {
  const batchSize = 50;  // Using 50 as a reasonable batch size
  const initialDelayBetweenBatches = 50;
  const dynamicConcurrencyLimit = config.concurrencyLimit || 10; // Start with a high concurrency limit

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
      const limit = pLimit(dynamicConcurrencyLimit);

      // Perform concurrent requests for the current batch
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

      // Batch insert results into the database
      if (results.length > 0) {
        await batchInsertOrUpdate('contracts', ['address', 'code_id', 'creator', 'admin', 'label'], results, 'address');
        log(`Batch inserted metadata for ${results.length} contracts (${i + results.length} out of ${totalContracts})`, 'INFO');
      } else {
        log(`No valid metadata found for batch starting with contract ${batch[0]}`, 'INFO');
      }

      // Update progress with the last processed contract address after each batch
      await updateProgress('fetchContractMetadata', 0, batch[batch.length - 1]);
      
      // Adjust delay between batches based on the concurrency limit or load
      const delayBetweenBatches = dynamicConcurrencyLimit > 10 ? initialDelayBetweenBatches / 2 : initialDelayBetweenBatches;
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
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
  const concurrencyLimit = config.concurrencyLimit || 5;
  const limit = pLimit(concurrencyLimit);
  const blockHeightHeader = { 'x-cosmos-block-height': config.blockHeight.toString() };

  try {
    const progress = checkProgress('fetchTokensAndOwners');
    const contracts = db.prepare("SELECT address, type FROM contracts WHERE type = 'cw721_base' OR type = 'cw1155' OR type = 'cw404' OR type = 'cw20_base'").all();
    const startIndex = progress.last_processed ? contracts.findIndex(contract => contract.address === progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contracts.length; i++) {
      const { address: contractAddress, type: contractType } = contracts[i];
      let allTokens = [];
      let ownershipData = [];
      let lastTokenFetched = null;

      log(`Fetching tokens for contract ${contractAddress} of type ${contractType}`, 'INFO');

      if (contractType === 'cw20_base') {
        // Query token_info for supply details
        const tokenInfoResponse = await sendContractQuery(restAddress, contractAddress, { token_info: {} });
        log(`Token info response for ${contractAddress}: ${JSON.stringify(tokenInfoResponse)}`, 'DEBUG');

        const totalSupply = tokenInfoResponse?.data?.data?.total_supply;

        if (!totalSupply) {
          log(`No supply or unsupported contract spec for cw20 contract ${contractAddress}. Skipping...`, 'ERROR');
          await updateProgress('fetchTokensAndOwners', 0, contractAddress);
          continue;
        }

        log(`Recorded total supply for cw20 contract ${contractAddress}: ${totalSupply}`, 'INFO');
        await batchInsertOrUpdate('contracts', ['address', 'tokens_minted'], [[contractAddress, totalSupply]], 'address');

        let allAccounts = [];
        let paginationKey = null;

        do {
          const accountsQueryPayload = { all_accounts: { limit: config.paginationLimit, ...(paginationKey && { start_after: paginationKey }) } };
          const accountsResponse = await sendContractQuery(restAddress, contractAddress, accountsQueryPayload, false, false, blockHeightHeader);
          log(`Accounts response for ${contractAddress}: ${JSON.stringify(accountsResponse)}`, 'DEBUG');

          if (accountsResponse?.data?.accounts?.length > 0) {
            const accounts = accountsResponse.data.accounts;
            allAccounts.push(...accounts);
            paginationKey = accounts[accounts.length - 1];
            log(`Fetched ${accounts.length} accounts for cw20 contract ${contractAddress}. Accounts: ${accounts.join(', ')}`, 'DEBUG');
          } else {
            paginationKey = null;
          }
        } while (paginationKey);

        const cw20OwnershipPromises = allAccounts.map(account => limit(async () => {
          const balanceQueryPayload = { balance: { address: account } };
          const balanceResponse = await retryOperation(() =>
            sendContractQuery(restAddress, contractAddress, balanceQueryPayload, false, false, blockHeightHeader)
          );
          log(`Balance response for account ${account} in ${contractAddress}: ${JSON.stringify(balanceResponse)}`, 'DEBUG');

          const balance = balanceResponse?.data?.balance;
          if (balance) {
            ownershipData.push([contractAddress, account, balance]);
            log(`Recorded balance for cw20 contract ${contractAddress}: owner=${account}, balance=${balance}`, 'DEBUG');
          } else {
            log(`Failed to retrieve balance for account ${account} in cw20 contract ${contractAddress}.`, 'ERROR');
          }
        }));

        await Promise.allSettled(cw20OwnershipPromises);

        if (ownershipData.length > 0) {
          await batchInsertOrUpdate('cw20_owners', ['contract_address', 'owner_address', 'balance'], ownershipData, ['contract_address', 'owner_address']);
          log(`Inserted ${ownershipData.length} ownership records into cw20_owners for ${contractAddress}`, 'INFO');
        }

        await updateProgress('fetchTokensAndOwners', 0, contractAddress);
        await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
        continue;
      }

      // Process cw1155 contracts with metadata logging and token insertion
      if (contractType === 'cw1155') {
        do {
          const tokenQueryPayload = { all_tokens: { limit: config.paginationLimit, ...(lastTokenFetched && { start_after: lastTokenFetched }) } };
          const response = await sendContractQuery(restAddress, contractAddress, tokenQueryPayload, false, false, blockHeightHeader);
          log(`Token response for ${contractAddress}: ${JSON.stringify(response)}`, 'DEBUG');

          if (response?.data?.data?.length > 0) {
            const tokens = response.data.data;
            const tokenInsertData = tokens.map(token => [
              contractAddress,
              token.token_id,
              token.info.token_uri,
              JSON.stringify(token.info.extension)
            ]);
            await batchInsertOrUpdate('contract_tokens', ['contract_address', 'token_id', 'token_uri', 'metadata'], tokenInsertData, ['contract_address', 'token_id']);
            log(`Inserted ${tokens.length} cw1155 tokens for contract ${contractAddress}`, 'INFO');

            lastTokenFetched = tokens[tokens.length - 1].token_id;
            allTokens.push(...tokens.map(token => token.token_id));
          } else {
            lastTokenFetched = null;
          }
        } while (lastTokenFetched);

        log(`Fetched ${allTokens.length} tokens for cw1155 contract ${contractAddress}`, 'INFO');
        await updateProgress('fetchTokensAndOwners', 0, contractAddress);
        await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
        continue;
      }

      // Process other contract types (cw721_base, cw404)
      while (true) {
        const tokenQueryPayload = {
          all_tokens: {
            limit: config.paginationLimit,
            ...(lastTokenFetched && { start_after: lastTokenFetched })
          }
        };
        console.log(restAddress, contractAddress, tokenQueryPayload);

        const response = await sendContractQuery(restAddress, contractAddress, tokenQueryPayload, false, false, blockHeightHeader);
        log(`Token response for ${contractAddress}: ${JSON.stringify(response)}`, 'DEBUG');

        if (response?.data?.data?.tokens?.length > 0) {
          const tokenIds = response.data.data.tokens;
          allTokens.push(...tokenIds);
          lastTokenFetched = tokenIds[tokenIds.length - 1];
          log(`Fetched ${tokenIds.length} tokens for contract ${contractAddress}. Token IDs: ${tokenIds.join(', ')}`, 'DEBUG');

          if (/(cw721_base|cw1155|cw404)/i.test(contractType)) {
            const ownershipPromises = tokenIds.map(tokenId => limit(async () => {
              const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
              const ownerResponse = await retryOperation(() =>
                sendContractQuery(restAddress, contractAddress, ownerQueryPayload, false, false, blockHeightHeader)
              );
              log(`Ownership response for token ${tokenId} in ${contractAddress}: ${JSON.stringify(ownerResponse)}`, 'DEBUG');

              if (ownerResponse?.data.data.owner) {
                ownershipData.push([contractAddress, tokenId, ownerResponse.data.data.owner, contractType]);
                log(`Recorded ownership for token ${tokenId} in ${contractAddress}: owner=${ownerResponse.data.data.owner}`, 'DEBUG');
              }
            }));

            await Promise.allSettled(ownershipPromises);
          }

          if (tokenIds.length < config.paginationLimit) break;
        } else {
          log(`No more tokens found for contract ${contractAddress}`, 'INFO');
          break;
        }
      }

      if (allTokens.length > 0) {
        await batchInsertOrUpdate('contracts', ['address', 'tokens_minted'], [[contractAddress, allTokens.join(',')]], 'address');
        log(`Updated tokens_minted for ${contractAddress} with total tokens: ${allTokens.length}`, 'INFO');

        const tokenInsertData = allTokens.map(tokenId => [contractAddress, tokenId, contractType]);
        await batchInsertOrUpdate('contract_tokens', ['contract_address', 'token_id', 'contract_type'], tokenInsertData, ['contract_address', 'token_id']);
        log(`Inserted ${allTokens.length} tokens into contract_tokens for ${contractAddress}`, 'INFO');

        if (ownershipData.length > 0) {
          await batchInsertOrUpdate('nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData, ['collection_address', 'token_id']);
          log(`Inserted ${ownershipData.length} ownership records into nft_owners for ${contractAddress}`, 'INFO');
        }
      }

      await updateProgress('fetchTokensAndOwners', 0, contractAddress, lastTokenFetched);
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
    }

    updateProgress('fetchTokensAndOwners', 1);
    log('Finished processing tokens and ownership for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchTokensAndOwners: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch pointer data and store it in the database
export async function fetchPointerData(pointerApi) {
  const chunkSize = config.chunkSize || 25; // Default to 25 if not defined

  // Retrieve all addresses from the contracts table and split them into an array
  const addressResult = db.prepare('SELECT address FROM contracts').all();
  const contractAddresses = addressResult.map(row => row.address);

  // Check if contractAddresses array is valid
  if (contractAddresses.length === 0) {
    log(`Error: No contract addresses found in the database`, 'ERROR');
    return; // Exit function if no addresses are found
  }

  // Insert contract addresses into the pointer_data table if not already present
  await db.transaction(() => {
    const insertQuery = db.prepare(`
      INSERT OR IGNORE INTO pointer_data (contract_address) VALUES (?)`);
    contractAddresses.forEach(address => insertQuery.run(address));
  })();

  // Process the addresses in chunks and send requests to pointerApi
  for (let i = 0; i < contractAddresses.length; i += chunkSize) {
    const chunk = contractAddresses.slice(i, i + chunkSize); // Prepare a chunk of addresses
    const payload = { addresses: chunk }; // Payload with addresses array

    try {
      // Send the batch request to the pointer API with retry logic
      const response = await retryOperation(() => axios.post(pointerApi, payload));

      if (response && response.status === 200 && Array.isArray(response.data)) {
        // Parse the response for each address and prepare data for insertion
        const batchData = response.data.map(({ address, pointerAddress, pointeeAddress, isBaseAsset, isPointer, pointerType }) => [
          address,
          pointerAddress || null,
          pointeeAddress || null,
          isBaseAsset ? 1 : 0,
          isPointer ? 1 : 0,
          pointerType || null
        ]);

        // Insert or update batch data in the pointer_data table
        await batchInsertOrUpdate(
          'pointer_data',
          ['contract_address', 'pointer_address', 'pointee_address', 'is_base_asset', 'is_pointer', 'pointer_type'],
          batchData,
          'contract_address'
        );
        log(`Stored pointer data for ${batchData.length} addresses`, 'DEBUG');
      } else {
        log(`Unexpected or empty data while fetching pointer data. Status: ${response ? response.status : 'No response'}`, 'ERROR');
      }
    } catch (error) {
      log(`Error processing pointer data chunk: ${error.message}`, 'ERROR');
    }
  }
  log('Finished fetching pointer data for all addresses.', 'INFO');
}

// fetch all evm wallet addresses for owners in nft_owners
export async function fetchAssociatedWallets(evmRpcAddress, concurrencyLimit = 5) {
  try {
    log('Starting fetchAssociatedWallets function...', 'INFO');
    
    // Get the last processed owner from indexer_progress
    const progress = checkProgress('fetchAssociatedWallets');
    let lastProcessedOwner = progress.last_processed;
    log(`Resuming from last processed owner: ${lastProcessedOwner || 'None'}`, 'INFO');

    // Select distinct owners from `nft_owners` who haven't been processed or are greater than `lastProcessedOwner`
    const owners = db.prepare(`
      SELECT DISTINCT owner 
      FROM nft_owners
      WHERE owner NOT IN (SELECT wallet_address FROM wallet_associations)
      AND owner > ?
      ORDER BY owner ASC
    `).all(lastProcessedOwner || '');

    if (owners.length === 0) {
      log('No unprocessed owners found in nft_owners table.', 'INFO');
      return;
    }

    log(`Found ${owners.length} unique owners to process`, 'INFO');
    
    for (let i = 0; i < owners.length; i += concurrencyLimit) {
      const batch = owners.slice(i, i + concurrencyLimit);
      log(`Processing batch ${Math.floor(i / concurrencyLimit) + 1} of ${Math.ceil(owners.length / concurrencyLimit)}`, 'INFO');
      
      await Promise.all(batch.map(async ({ owner }) => {
        log(`Fetching EVM address for owner: ${owner}`, 'DEBUG');
        
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
            log(`Stored EVM address for owner: ${owner}`, 'INFO');
          } else {
            log(`No EVM address found for owner: ${owner}`, 'WARN');
          }
        } catch (error) {
          log(`Error processing owner ${owner}: ${error.message}`, 'ERROR');
        }
      }));

      // Update the progress with the last processed owner in this batch
      const lastOwnerInBatch = batch[batch.length - 1].owner;
      updateProgress('fetchAssociatedWallets', 0, lastOwnerInBatch);
      log(`Updated progress for batch ${Math.floor(i / concurrencyLimit) + 1}`, 'DEBUG');
    }

    // Mark the step as completed if all owners were processed
    updateProgress('fetchAssociatedWallets', 1);
    log('Finished processing all associated wallets.', 'INFO');
  } catch (error) {
    log(`Critical error in fetchAssociatedWallets: ${error.message}`, 'ERROR');
    throw error;
  }
}

export { axios };
