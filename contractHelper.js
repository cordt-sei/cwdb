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
              log('All code IDs recorded.', 'INFO');
              break;
          }
          
          const batchData = response.map(({ code_id, creator, instantiate_permission }) => 
          [code_id, creator, JSON.stringify(instantiate_permission)]
          );
          await batchInsert(dbRun, 'code_ids', ['code_id', 'creator', 'instantiate_permission'], batchData);
          allCodeInfos = allCodeInfos.concat(response);
          
          log(`Recorded ${batchData.length} code IDs.`, 'INFO');
          
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
      } else {
          log('No new code IDs recorded.', 'INFO');
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
          
          log(`Gettting contracts for ID ${code_id}`, 'INFO');
          
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
                          paginationPayload: {
                              'pagination.limit': config.paginationLimit,
                              ...(nextKey && { 'pagination.key': nextKey }),
                          }
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
              await batchInsert(dbRun, 'contracts', ['code_id', 'address', 'type'], batchData);
              
              log(`Recorded ${batchData.length} contracts for ID ${code_id}`, 'DEBUG'),
              log(`Recorded all associated contracts`, 'INFO');
              // Check for next key for further pagination
              if (response.pagination?.next_key) {
                  nextKey = response.pagination.next_key;
              } else {
                  break;
              }
          }
          
          // Update progress after each code_id processing
          await updateProgress(db, 'fetchContractsByCode', 0, code_id);
      }
      
      await updateProgress(db, 'fetchContractsByCode', 1);
      log('Finished fetching and storing contract addresses for all code IDs.', 'INFO');
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
                      log(`Unexpected response status ${response.status} for contract ${contractAddress}`, 'DEBUG');
                  }
              } catch (error) {
                  log(`Failed to fetch contract info for ${contractAddress}: ${error.message}`, 'ERROR');
              }
              return null;
          });
          
          const results = (await Promise.all(fetchPromises)).filter(Boolean);
          if (results.length > 0) {
              await batchInsert(dbRun, 'contracts', ['address', 'code_id', 'creator', 'admin', 'label'], results);
              log(`Batch inserted metadata for ${results.length} contracts`, 'DEBUG');
          }
          
          // Update progress with the last contract in the batch
          await updateProgress(db, 'fetchContractMetadata', 0, batch[batch.length - 1]);
          await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
      }
      
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
                      log(`Inserted ${batchData.length} history records for contract: ${address}`, 'DEBUG');
                  } else {
                      log(`No history entries found for contract: ${address}`, 'DEBUG');
                  }
                  
                  // Update progress after processing each contract
                  await updateProgress(db, 'fetchContractHistory', 0, address);
              } catch (error) {
                  log(`Error fetching history for contract ${address}: ${error.message}`, 'ERROR');
              }
          });
          
          await Promise.all(historyPromises);
          log(`Processed batch of ${batch.length} contracts`, 'DEBUG');
      }
      
      // Mark the entire step as complete
      await updateProgress(db, 'fetchContractHistory', 1);
      log('Finished processing history for all contracts.', 'INFO');
  } catch (error) {
      log(`Error in fetchContractHistory: ${error.message}`, 'ERROR');
      throw error;
  }
}

// Identify contract types and store them in the database
export async function identifyContractTypes(restAddress, db) {
  const dbRun = promisify(db.run).bind(db);
  
  try {
    const contracts = (await db.all('SELECT address FROM contracts')).map(row => row.address);
    
    for (const contractAddress of contracts) {
      let contractType = 'other';
      const testPayload = { "a": "b" };
      
      try {
        const response = await sendContractQuery(restAddress, contractAddress, testPayload);
        
        if (response && response.message) {
          const match = response.message.match(/Error parsing into type (\S+)::/);
          if (match) {
            contractType = match[1];
            log(`Identified contract type for ${contractAddress}: ${contractType}`, 'DEBUG');
          } else {
            log(`Unable to extract contract type for ${contractAddress} from 400 response`, 'DEBUG');
          }
        } else {
          log(`No valid response data for ${contractAddress}`, 'DEBUG');
        }
        
        // Update the database with the identified contract type
        await dbRun("UPDATE contracts SET type = ? WHERE address = ?", [contractType, contractAddress]);
        
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
export async function fetchTokensAndOwners(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  const parallelRequests = 5;
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
        let allTokens = [];
        let ownershipData = [];
        let lastTokenFetched = null;
        
        try {
          const requestUrl = `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart`;
          let retryCount = 0;
          const maxRetries = 3;
          
          while (true) {
            // Prepare the payload for fetching tokens
            const tokenQueryPayload = {
              all_tokens: {
                limit: tokenLimit,
                ...(lastTokenFetched && { start_after_id: lastTokenFetched })
              }
            };
            const encodedPayload = Buffer.from(JSON.stringify(tokenQueryPayload)).toString('base64');
            
            try {
              log(`Fetching tokens for ${contractAddress} starting at ${lastTokenFetched || 'initial'}`, 'DEBUG');
              
              const response = await axios.post(requestUrl, { base64_encoded_payload: encodedPayload });
              
              if (response.status === 200 && response.data?.data?.tokens?.length > 0) {
                const tokenIds = response.data.data.tokens;
                allTokens.push(...tokenIds);
                lastTokenFetched = tokenIds[tokenIds.length - 1];
                
                log(`Fetched ${tokenIds.length} tokens for contract ${contractAddress}. Total: ${allTokens.length}`, 'DEBUG');
                
                // Fetch ownership data if needed
                if (/721|1155|404/i.test(contractType)) {
                  const ownershipPromises = tokenIds.map(async tokenId => {
                    try {
                      const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
                      const ownerResponse = await retryOperation(() => axios.post(requestUrl, ownerQueryPayload));
                      
                      if (ownerResponse.status === 200 && ownerResponse.data?.data?.owner) {
                        const owner = ownerResponse.data.data.owner;
                        ownershipData.push([contractAddress, tokenId, owner, contractType]);
                      }
                    } catch (ownershipError) {
                      log(`Error fetching ownership for token ${tokenId}: ${ownershipError.message}`, 'ERROR');
                    }
                  });
                  
                  await Promise.allSettled(ownershipPromises);
                }
                
                // If fewer tokens than the limit were fetched, break the pagination loop
                if (tokenIds.length < tokenLimit) break;
              } else {
                log(`No more tokens found for contract ${contractAddress}`, 'DEBUG');
                break;
              }
            } catch (fetchError) {
              log(`Error fetching tokens for ${contractAddress}: ${fetchError.message}`, 'ERROR');
              
              if (fetchError.response?.status === 404 && retryCount < maxRetries) {
                retryCount++;
                log(`Retrying ${retryCount}/${maxRetries} for ${contractAddress}`, 'DEBUG');
                continue;
              }
              
              break;
            }
          }
          
          // Update tokens in the database if any were fetched
          if (allTokens.length > 0) {
            const tokenIdsString = allTokens.join(',');
            await dbRun("UPDATE contracts SET token_ids = ? WHERE address = ?", [tokenIdsString, contractAddress]);
            log(`Updated token_ids for ${contractAddress}`, 'DEBUG');
            
            // Insert token data into contract_tokens table
            const tokenInsertData = allTokens.map(tokenId => [contractAddress, tokenId, contractType]);
            await batchInsert(dbRun, 'contract_tokens', ['contract_address', 'token_id', 'contract_type'], tokenInsertData);
            log(`Inserted ${allTokens.length} tokens into contract_tokens for ${contractAddress}`, 'DEBUG');
            
            // Insert ownership data if available
            if (ownershipData.length > 0) {
              await batchInsert(dbRun, 'nft_owners', ['collection_address', 'token_id', 'owner', 'contract_type'], ownershipData);
              log(`Inserted ${ownershipData.length} ownership records into nft_owners`, 'DEBUG');
            }
          }
          
          // Fetch CW404 details if applicable
          if (/cw404/i.test(contractType)) {
            await fetchAndStoreCW404Details(restAddress, contractAddress, dbRun);
          }
          
          // Update progress after processing
          await updateProgress(db, 'processTokens', 0, contractAddress, lastTokenFetched);
        } catch (error) {
          log(`Error processing contract ${contractAddress}: ${error.message}`, 'ERROR');
        }
      });
      
      await Promise.all(processingPromises);
      log(`Processed batch of ${batch.length} contracts`, 'DEBUG');
    }
    
    // Mark the entire step as complete
    await updateProgress(db, 'processTokens', 1);
    log('Finished processing tokens and ownership for all contracts.', 'INFO');
  } catch (error) {
    log(`Error in fetchTokensAndOwners: ${error.message}`, 'ERROR');
    throw error;
  }
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
          log(`Stored CW404 details for ${contractAddress}`, 'DEBUG');
      } else {
          log(`No details found for CW404 contract ${contractAddress}`, 'DEBUG');
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
              
              await batchInsert(dbRun, 'pointer_data', ['contract_address', 'pointer_address', 'pointee_address', 'is_base_asset', 'is_pointer', 'pointer_type'], batchData);
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
export async function fetchAssociatedWallets(evmRpcAddress, db, concurrencyLimit = 5) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);
  
  try {
      log('Starting fetchAssociatedWallets...', 'DEBUG');
      
      const owners = await dbAll('SELECT DISTINCT owner FROM nft_owners');
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
                      await batchInsert(dbRun, 'wallet_associations', ['wallet_address', 'evm_address'], [[owner, evmAddress]]);
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
