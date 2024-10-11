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

// Fetch all minted tokens for specified NFT contracts
export async function fetchAndStoreTokensForContracts(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const progress = await checkProgress(db, 'fetchTokens');
    if (progress.completed) {
      log('Skipping fetchAndStoreTokensForContracts: Already completed');
      return;
    }

    // Fetch all contracts that are either CW404, CW721, or galxe_nft
    const contractsResult = await dbAll("SELECT address, type FROM contracts WHERE type IN ('cw404', 'cw721_base', 'galxe_nft')");
    const startIndex = progress.last_processed ? contractsResult.findIndex(c => c.address === progress.last_processed) + 1 : 0;

    for (let i = startIndex; i < contractsResult.length; i++) {
      const { address: contractAddress, type: contractType } = contractsResult[i];
      const tokenQueryPayload = { all_tokens: {} };
      let hasMore = true;
      let nextKey = null;

      while (hasMore) {
        const payload = {};
        if (nextKey) {
          payload['pagination.key'] = nextKey;
        }

        // Fetch tokens data for the contract using `all_tokens` query
        try {
          const tokens = await fetchPaginatedData(
            `${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${Buffer.from(JSON.stringify(tokenQueryPayload)).toString('base64')}`,
            payload,
            'tokens'
          );

          if (tokens && tokens.length > 0) {
            log(`Fetched ${tokens.length} tokens for contract ${contractAddress}`);

            // Insert each token ID as a separate row in `contract_tokens` table
            for (const tokenId of tokens) {
              await dbRun(`INSERT OR REPLACE INTO contract_tokens (contract_address, token_id, contract_type) VALUES (?, ?, ?)`, [contractAddress, tokenId, contractType]);
            }

            // Update the `token_ids` column in the `contracts` table with all the fetched tokens
            await dbRun(`UPDATE contracts SET token_ids = ? WHERE address = ?`, [tokens.join(','), contractAddress]);
          } else {
            log(`No tokens found for contract ${contractAddress}`);
          }

          nextKey = tokens.pagination?.next_key || null;
          hasMore = tokens.length === config.paginationLimit && nextKey;

        } catch (error) {
          // Handle specific errors like 'out of gas' or 'Generic error'
          if (error.response && error.response.data?.message?.includes('out of gas')) {
            log(`Out of gas for contract ${contractAddress}. Updating type to 'pointer'.`);
            await dbRun(`UPDATE contracts SET type = ? WHERE address = ?`, ['pointer', contractAddress]);
            break;  // Skip this contract and move on
          } else if (error.response && error.response.data?.message?.includes('Generic error')) {
            log(`Generic error for contract ${contractAddress}. Updating type to 'pointer'.`);
            await dbRun(`UPDATE contracts SET type = ? WHERE address = ?`, ['pointer', contractAddress]);
            break;  // Skip this contract and move on
          } else if (error.response && error.response.data?.message?.includes("cw404")) {
            log(`CW404 contract detected. Relabeling contract ${contractAddress} as cw404_wrapper.`);
            await dbRun(`UPDATE contracts SET type = ? WHERE address = ?`, ['cw404_wrapper', contractAddress]);
            break;
          } else {
            log(`Error fetching tokens for contract ${contractAddress}: ${error.message}`);
            throw error;  // Re-throw unknown errors
          }
        }
      }

      // Update progress after processing each contract
      await updateProgress(db, 'fetchTokens', 0, contractAddress);
    }

    log('Finished processing tokens for all CW404, CW721, and galxe_nft contracts.');
    await updateProgress(db, 'fetchTokens', 1, null);

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
    if (progress.completed) {
      log('Skipping fetchAndStoreTokenOwners: Already completed');
      return;
    }

    // Query contract_tokens table to get the contract address and token_id
    const contractTokensResult = await dbAll('SELECT contract_address, token_id, contract_type FROM contract_tokens');

    for (let i = 0; i < contractTokensResult.length; i++) {
      const { contract_address: contractAddress, token_id: tokenId, contract_type: contractType } = contractTokensResult[i];

      // Prepare payload for querying the owner of the token
      const ownerQueryPayload = { owner_of: { token_id: tokenId.toString() } };
      const headers = { 'x-cosmos-block-height': config.blockHeight.toString() };

      try {
        const { data, status } = await sendContractQuery(restAddress, contractAddress, ownerQueryPayload, headers);

        if (status === 200 && data && data.owner) {
          const owner = data.owner;

          // Insert owner details into the nft_owners table
          const insertOwnerSQL = `
            INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner, contract_type)
            VALUES (?, ?, ?, ?)
          `;
          await dbRun(insertOwnerSQL, [contractAddress, tokenId, owner, contractType]);

          log(`Recorded ownership: Token ${tokenId} owned by ${owner} in ${contractType} contract ${contractAddress}.`);
        }
      } catch (err) {
        log(`Error fetching token ownership for contract ${contractAddress}, token ${tokenId}: ${err.message}`);
      }
    }

    log('Finished processing token ownership for all contracts');
    await updateProgress(db, 'fetchTokenOwners', 1, null);
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

    for (const { owner } of ownersResult) {
      const payload = {
        jsonrpc: "2.0",
        method: "sei_getSeiAddress",
        params: [owner],
        id: 1
      };

      const response = await retryOperation(() => axios.post(evmRpcAddress, payload));

      if (response.status === 200 && response.data.result) {
        const evmAddress = response.data.result;

        const insertSQL = `INSERT OR REPLACE INTO wallet_associations (wallet_address, evm_address) VALUES (?, ?)`;
        await dbRun(insertSQL, [owner, evmAddress]);

        log(`Stored EVM address: ${evmAddress} for wallet: ${owner}`);
      } else {
        log(`Error fetching EVM address for wallet ${owner}: ${response.status}`);
      }
    }

    log('Finished processing associated wallets');
  } catch (error) {
    log(`Error in fetchAndStoreAssociatedWallets: ${error.message}`);
    throw error;
  }
}