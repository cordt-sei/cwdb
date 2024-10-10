import { fetchPaginatedData, sendContractQuery, retryOperation, log } from './utils.js';
import axios from 'axios';
import { promisify } from 'util';

// Fetch all code IDs and store them in the database
export async function fetchAndStoreCodeIds(restAddress, db) {
  const dbRun = promisify(db.run).bind(db);

  try {
    const codeInfos = await fetchPaginatedData(`${restAddress}/cosmwasm/wasm/v1/code`, null, {}, 'code_infos');

    for (const codeInfo of codeInfos) {
      const { code_id, creator } = codeInfo;
      const insertSQL = `INSERT OR REPLACE INTO code_ids (code_id, creator) VALUES (?, ?)`;
      await dbRun(insertSQL, [code_id, creator]);
      log(`Stored code_id: ${code_id} from creator ${creator}`);
    }

    log(`Total code_ids fetched: ${codeInfos.length}`);
  } catch (error) {
    log(`Error in fetchAndStoreCodeIds: ${error.message}`);
    throw error;
  }
}

// Fetch contracts by code and store them in the database
export async function fetchAndStoreContractsByCode(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    // Fetch all code IDs from the database
    const codeIdsResult = await dbAll('SELECT code_id FROM code_ids');
    const codeIds = codeIdsResult.map(row => row.code_id);

    for (const code_id of codeIds) {
      const contracts = await fetchPaginatedData(`${restAddress}/cosmwasm/wasm/v1/code/${code_id}/contracts`, null, {}, 'contracts');

      for (const contractAddress of contracts) {
        const insertSQL = `INSERT OR REPLACE INTO contracts (code_id, address) VALUES (?, ?)`;
        await dbRun(insertSQL, [code_id, contractAddress]);

        log(`Stored contract address: ${contractAddress} for code_id: ${code_id}`);
      }

      log(`Processed ${contracts.length} contracts for code_id: ${code_id}`);
    }

    log(`Finished processing contracts for all ${codeIds.length} code IDs`);
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
    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contractAddresses = contractsResult.map(row => row.address);

    for (const address of contractAddresses) {
      const historyEntries = await fetchPaginatedData(`${restAddress}/cosmwasm/wasm/v1/contract/${address}/history`, null, {}, 'entries');

      for (const entry of historyEntries) {
        const { operation, code_id, updated, msg } = entry;
        const insertSQL = `INSERT OR REPLACE INTO contract_history (contract_address, operation, code_id, updated, msg) VALUES (?, ?, ?, ?, ?)`;
        await dbRun(insertSQL, [address, operation, code_id, updated || null, JSON.stringify(msg)]);

        log(`Stored history entry for contract ${address}, operation: ${operation}`);
      }
    }

    log(`Finished processing history for all ${contractAddresses.length} contracts`);
  } catch (error) {
    log(`Error in fetchAndStoreContractHistory: ${error.message}`);
    throw error;
  }
}

// Identify contract types
export async function identifyContractTypes(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contracts = contractsResult.map(row => row.address);

    for (const contractAddress of contracts) {
      const invalidPayload = { a: "b" };  // Intentionally invalid payload
      const { data, status } = await sendContractQuery(restAddress, contractAddress, invalidPayload);

      if (status === 400 && data.message) {
        const contractType = data.message.match(/cw[0-9]+/i)?.[0] || 'other';
        const updateSQL = `UPDATE contracts SET type = ? WHERE address = ?`;
        await dbRun(updateSQL, [contractType, contractAddress]);

        log(`Identified contract ${contractAddress} as ${contractType}`);
      } else {
        log(`Failed to identify type for contract ${contractAddress}. Marking as 'other'.`);
        const updateSQL = `UPDATE contracts SET type = 'other' WHERE address = ?`;
        await dbRun(updateSQL, [contractAddress]);
      }
    }

    log(`Finished identifying types for all ${contracts.length} contracts`);
  } catch (error) {
    log(`Error in identifyContractTypes: ${error.message}`);
    throw error;
  }
}

// Fetch all token IDs for CW721, CW404, and CW1155 contracts and record them in the database
export async function fetchAndStoreTokensForContracts(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const contractsResult = await dbAll("SELECT address, type FROM contracts WHERE type IN ('cw721', 'cw404', 'cw1155')");
    
    for (const contract of contractsResult) {
      const { address: contractAddress, type: contractType } = contract;
      const tokenQueryPayload = { all_tokens: {} };
      const tokens = await fetchPaginatedData(`${restAddress}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${Buffer.from(JSON.stringify(tokenQueryPayload)).toString('base64')}`, null, {}, 'tokens');

      if (tokens.length > 0) {
        const tokenIdsStr = tokens.join(',');

        const insertContractSQL = `INSERT OR REPLACE INTO contract_tokens (contract_address, extra_data, contract_type) VALUES (?, ?, ?)`;
        await dbRun(insertContractSQL, [contractAddress, tokenIdsStr, contractType]);

        log(`Stored tokens for ${contractType} contract ${contractAddress}: ${tokens.length} tokens.`);
      } else {
        log(`No tokens found for ${contractType} contract ${contractAddress}.`);
      }
    }

    log(`Finished processing tokens for all CW721, CW404, and CW1155 contracts`);
  } catch (error) {
    log(`Error in fetchAndStoreTokensForContracts: ${error.message}`);
    throw error;
  }
}

// Fetch and store owner information for each token in all contracts
export async function fetchAndStoreTokenOwners(restAddress, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const contractTokensResult = await dbAll('SELECT contract_address, extra_data, contract_type FROM contract_tokens');

    for (const contractToken of contractTokensResult) {
      const { contract_address: contractAddress, extra_data: tokenIdsStr, contract_type: contractType } = contractToken;
      const tokenIds = tokenIdsStr.split(',');

      for (const token_id of tokenIds) {
        const ownerQueryPayload = { owner_of: { token_id: token_id.toString() } };
        const headers = { 'x-cosmos-block-height': config.blockHeight.toString() };
        const { data, status } = await sendContractQuery(restAddress, contractAddress, ownerQueryPayload, headers);

        if (status === 200 && data && data.owner) {
          const owner = data.owner;

          const insertOwnerSQL = `INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner, contract_type) VALUES (?, ?, ?, ?)`;
          await dbRun(insertSQL, [contractAddress, token_id, owner, contractType]);

          log(`Recorded ownership: Token ${token_id} owned by ${owner} in ${contractType} contract ${contractAddress}.`);
        } else {
          log(`No owner found for token ID: ${token_id} in ${contractType} contract ${contractAddress}.`);
        }
      }
    }

    log('Finished processing token ownership for all contracts');
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

// Fetch and store pointer address data
export async function fetchAndStorePointerData(pointerApi, db) {
  const dbAll = promisify(db.all).bind(db);
  const dbRun = promisify(db.run).bind(db);

  try {
    const contractsResult = await dbAll('SELECT address FROM contracts');
    const contractAddresses = contractsResult.map(row => row.address);

    const payload = { addresses: contractAddresses };
    const response = await retryOperation(() => axios.post(pointerApi, payload));

    if (response.status === 200 && response.data) {
      for (const entry of response.data) {
        const { address, pointerAddress, pointeeAddress } = entry;

        const insertSQL = `INSERT OR REPLACE INTO pointer_data (contract_address, pointer_address, pointee_address) VALUES (?, ?, ?)`;
        await dbRun(insertSQL, [address, pointerAddress || null, pointeeAddress || null]);

        log(`Stored pointer data for address ${address}`);
      }
    } else {
      log(`Error querying pointer API: ${response.status}`);
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