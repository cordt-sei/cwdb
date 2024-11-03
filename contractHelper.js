// contractHelper.js

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
    let allCodeInfos = [];

    while (true) {
      let response;
      try {
        // Ensure nextKey is URL-encoded
        const encodedNextKey = nextKey ? encodeURIComponent(nextKey) : null;

        response = await fetchPaginatedData(
          `${restAddress}/cosmwasm/wasm/v1/code`,
          'code_infos',
          {
            paginationType: 'query',
            useNextKey: true,
            limit: config.paginationLimit,
            nextKey: encodedNextKey // Properly encode and pass the nextKey
          }
        );
      } catch (error) {
        log(`Error fetching code IDs: ${error.message}`, 'ERROR');
        throw error;
      }

      if (!Array.isArray(response) || response.length === 0) {
        log('All code IDs recorded.', 'INFO');
        break;
      }

      const batchData = response.map(({ code_id, creator, instantiate_permission }) => 
        [code_id, creator, JSON.stringify(instantiate_permission)]
      );
      batchInsertOrUpdate('code_ids', ['code_id', 'creator', 'instantiate_permission'], batchData, 'code_id');
      allCodeInfos = allCodeInfos.concat(response);

      log(`Recorded ${batchData.length} code IDs.`, 'INFO');

      // Update nextKey for the next iteration
      nextKey = response.pagination?.next_key || null;

      // Log nextKey for debugging if necessary
      log(`Next pagination key: ${nextKey}`, 'DEBUG');

      if (!nextKey) break;

      updateProgress('fetchCodeIds', 0, response[response.length - 1].code_id);
    }

    if (allCodeInfos.length > 0) {
      log(`Total code IDs fetched and stored: ${allCodeInfos.length}`, 'INFO');
      updateProgress('fetchCodeIds', 1);
    } else {
      log('No new code IDs recorded.', 'INFO');
    }
  } catch (error) {
    log(`Error in fetchCodeIds: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch contract addresses by code and store them in the database
export async function fetchContractAddressesByCodeId(restAddress) {
  try {
    const progress = checkProgress('fetchContractsByCode');
    if (progress.completed) {
      log('Skipping fetchContractAddressesByCodeId: Already completed', 'INFO');
      return;
    }

    const codeIds = db.prepare('SELECT code_id FROM code_ids').all().map(row => row.code_id);
    const startIndex = progress.last_processed ? codeIds.indexOf(progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < codeIds.length; i++) {
      const code_id = codeIds[i];
      let nextKey = null;

      log(`Getting contracts for ID ${code_id}`, 'INFO');

      while (true) {
        let response;
        try {
          response = await fetchPaginatedData(
            `${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts`,
            'contracts',
            {
              paginationType: 'query',
              useNextKey: true,
              limit: config.paginationLimit,
              nextKey: nextKey // Carry forward the nextKey for pagination
            }
          );
        } catch (error) {
          log(`Error fetching contracts for ID ${code_id}: ${error.message}`, 'ERROR');
          throw error;
        }

        if (!Array.isArray(response) || response.length === 0) {
          log(`No contracts found for ID ${code_id}`, 'INFO');
          break;
        }

        const batchData = response.map(contractAddress => [code_id, contractAddress, null]);
        batchInsertOrUpdate('contracts', ['code_id', 'address', 'type'], batchData, 'address');

        log(`Recorded ${batchData.length} contracts for ID ${code_id}`, 'DEBUG');

        // Update nextKey for the next pagination iteration
        nextKey = response.pagination?.next_key || null;

        if (!nextKey) break; // Stop if there is no more pagination key
      }

      updateProgress('fetchContractsByCode', 0, code_id);
    }

    updateProgress('fetchContractsByCode', 1);
    log('Finished fetching and storing contract addresses for all code IDs.', 'INFO');
  } catch (error) {
    log(`Error in fetchContractAddressesByCodeId: ${error.message}`, 'ERROR');
    throw error;
  }
}

export async function fetchContractHistory(restAddress) {
  try {
    const contracts = db.prepare('SELECT address FROM contracts').all().map(row => row.address);
    for (const contractAddress of contracts) {
      let nextKey = null;

      log(`Fetching contract history for ${contractAddress}`, 'INFO');

      while (true) {
        try {
          // Build the request URL with pagination
          const url = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/history`;
          const requestUrl = nextKey ? `${url}?pagination.key=${encodeURIComponent(nextKey)}` : url;

          // Fetch contract history data
          const response = await axios.get(requestUrl);
          if (response.status !== 200 || !response.data) {
            log(`Failed to fetch contract history for ${contractAddress}. Status: ${response.status}`, 'ERROR');
            break;
          }

          const { entries, pagination } = response.data;
          if (!Array.isArray(entries) || entries.length === 0) {
            log(`No history entries found for ${contractAddress}`, 'INFO');
            break;
          }

          // Insert history entries into the database
          const insertData = entries.map(entry => [
            contractAddress,
            entry.operation,
            entry.code_id,
            entry.updated,
            JSON.stringify(entry.msg)
          ]);

          db.transaction(() => {
            const insertStmt = db.prepare(`
              INSERT OR REPLACE INTO contract_history 
              (contract_address, operation, code_id, updated, msg) 
              VALUES (?, ?, ?, ?, ?)
            `);
            for (const data of insertData) {
              insertStmt.run(data);
            }
          })();

          log(`Inserted ${entries.length} history entries for ${contractAddress}`, 'INFO');

          // Check if there is a next page
          if (pagination && pagination.next_key) {
            nextKey = pagination.next_key;
          } else {
            break;
          }

        } catch (error) {
          log(`Error fetching contract history for ${contractAddress}: ${error.message}`, 'ERROR');
          break; // Exit the loop if an error occurs
        }
      }
    }
    log('Completed fetching contract history for all contracts.', 'INFO');
  } catch (error) {
    log(`Failed in fetchContractHistory: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch and store metadata for each contract address
export async function fetchContractMetadata(restAddress) {
  const batchSize = 5;
  const delayBetweenBatches = 100;

  try {
    const progress = checkProgress('fetchContractMetadata');
    if (progress.completed) {
      log('Skipping fetchContractMetadata: Already completed', 'INFO');
      return;
    }

    const contractAddresses = db.prepare('SELECT address FROM contracts').all().map(row => row.address);
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
            log(`Unexpected response status ${response.status} for contract ${contractAddress}`, 'DEBUG');
          }
        } catch (error) {
          log(`Failed to fetch contract info for ${contractAddress}: ${error.message}`, 'ERROR');
        }
        return null;
      });

      const results = (await Promise.all(fetchPromises)).filter(Boolean);
      if (results.length > 0) {
        batchInsertOrUpdate('contracts', ['address', 'code_id', 'creator', 'admin', 'label'], results, 'address');
        log(`Batch inserted metadata for ${results.length} contracts`, 'DEBUG');
      }

      updateProgress('fetchContractMetadata', 0, batch[batch.length - 1]);
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
    }

    updateProgress('fetchContractMetadata', 1);
    log('Finished processing metadata for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchContractMetadata: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Identify contract types and store them in the database
export async function identifyContractTypes(restAddress) {
  try {
    const contracts = db.prepare('SELECT address FROM contracts').all().map(row => row.address);

    for (const contractAddress of contracts) {
      let contractType = 'other';
      const testPayload = { "a": "b" };

      try {
        const response = await sendContractQuery(restAddress, contractAddress, testPayload);

        if (response && response.message) {
          // Properly extract contract type from 400 response message
          const match = response.message.match(/Error parsing into type (\S+)::/);
          if (match) {
            contractType = match[1];
            log(`Identified contract type for ${contractAddress}: ${contractType}`, 'INFO');
            log(`Extracted contract type information: ${response.message}`, 'DEBUG');
          } else {
            log(`Unable to extract contract type for ${contractAddress} from 400 response`, 'DEBUG');
          }
        } else {
          log(`No valid response data for ${contractAddress}`, 'DEBUG');
        }

        batchInsertOrUpdate('contracts', ['address', 'type'], [[contractAddress, contractType]], 'address');
      } catch (error) {
        log(`Error determining contract type for ${contractAddress}: ${error.message}`, 'ERROR');
      }
    }

    log('Finished identifying contract types.', 'INFO');
  } catch (error) {
    log(`Error in identifyContractTypes: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Fetch tokens and their owners for relevant contracts
export async function fetchTokensAndOwners(restAddress) {
  const delayBetweenBatches = 100;

  try {
    const progress = checkProgress('fetchTokensAndOwners');
    const contracts = db.prepare("SELECT address, type FROM contracts WHERE type IN ('cw721', 'cw1155')").all().map(row => row);
    const startIndex = progress.last_processed ? contracts.findIndex(contract => contract.address === progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contracts.length; i++) {
      const { address: contractAddress, type: contractType } = contracts[i];
      let allTokens = [];
      let ownershipData = [];
      let lastTokenFetched = null;

      while (true) {
        const tokenQueryPayload = {
          all_tokens: {
            limit: config.paginationLimit,
            ...(lastTokenFetched && { start_after_id: lastTokenFetched })
          }
        };

        try {
          const response = await sendContractQuery(
            restAddress,
            contractAddress,
            tokenQueryPayload
          );

          if (response && response.tokens?.length > 0) {
            const tokenIds = response.tokens;
            allTokens.push(...tokenIds);
            lastTokenFetched = tokenIds[tokenIds.length - 1];

            log(`Fetched ${tokenIds.length} tokens for contract ${contractAddress}. Total: ${allTokens.length}`, 'INFO');
            log(`Full token list fetched: ${JSON.stringify(tokenIds)}`, 'DEBUG');

            if (/721|1155/i.test(contractType)) {
              const ownershipPromises = tokenIds.map(async tokenId => {
                try {
                  const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
                  const ownerResponse = await retryOperation(() => sendContractQuery(
                    restAddress,
                    contractAddress,
                    ownerQueryPayload,
                    false, // Use GET request
                    { 'x-cosmos-block-header': config.blockHeight.toString() } // Pass custom header
                  ));

                  if (ownerResponse && ownerResponse.owner) {
                    const owner = ownerResponse.owner;
                    ownershipData.push([contractAddress, tokenId, owner, contractType]);
                    log(`Fetched ownership for token ${tokenId}: owner is ${owner}`, 'DEBUG');
                  }
                } catch (ownershipError) {
                  log(`Error fetching ownership for token ${tokenId}: ${ownershipError.message}`, 'ERROR');
                }
              });
              await Promise.allSettled(ownershipPromises);
            }

            if (tokenIds.length < config.paginationLimit) break;
          } else {
            log(`No more tokens found for contract ${contractAddress}`, 'INFO');
            break;
          }
        } catch (fetchError) {
          log(`Error fetching tokens for ${contractAddress}: ${fetchError.message}`, 'ERROR');
          break;
        }
      }

      if (allTokens.length > 0) {
        const tokenIdsString = allTokens.join(',');
        await batchInsertOrUpdate('contracts', ['address', 'token_ids'], [[contractAddress, tokenIdsString]], 'address');
        log(`Updated token_ids for ${contractAddress}`, 'INFO');

        const tokenInsertData = allTokens.map(tokenId => [contractAddress, tokenId, contractType]);
        await batchInsertOrUpdate('contract_tokens', ['contract_address', 'token_id', 'contract_type'], tokenInsertData, ['contract_address', 'token_id']);
        log(`Inserted ${allTokens.length} tokens into contract_tokens for ${contractAddress}`, 'INFO');

        if (ownershipData.length > 0) {
          await batchInsertOrUpdate('nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData, ['collection_address', 'token_id']);
          log(`Inserted ${ownershipData.length} ownership records into nft_owners`, 'INFO');
        }
      }

      updateProgress('fetchTokensAndOwners', 0, contractAddress, lastTokenFetched);
      await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
    }

    log('Finished processing tokens and ownership for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchTokensAndOwners: ${error.message}`, 'ERROR');
    throw error;
  }
}

// Helper function to process pointer data
export async function fetchPointerData(pointerApi, contractAddresses, chunkSize = 10) {
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
              
              await batchInsertOrUpdate('pointer_data', ['contract_address', 'pointer_address', 'pointee_address', 'is_base_asset', 'is_pointer', 'pointer_type'], batchData, 'contract_address');
              log(`Stored pointer data for ${batchData.length} addresses`, 'DEBUG');
          } else {
              log(`Unexpected or empty data while fetching pointer data. Status: ${response.status}`, 'ERROR');
          }
      } catch (error) {
          log(`Error processing pointer data chunk: ${error.message}`, 'ERROR');
      }
  }
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
                      const evmAddress = response.data.result;
                      await batchInsertOrUpdate('wallet_associations', ['wallet_address', 'evm_address'], [[owner, evmAddress]], 'wallet_address');
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

export {axios}