const axios = require('axios');
const { Buffer } = require('buffer');
const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');

// Helper function to send a smart contract query
async function sendContractQuery(rpcEndpoint, contractAddress, payload) {
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const url = `${rpcEndpoint}/cosmwasm/wasm/v1/contract/${contractAddress}/smart/${payloadBase64}`;

  try {
    const response = await axios.get(url);
    return { data: response.data, status: response.status };
  } catch (error) {
    return { error: error.response.data, status: error.response.status };
  }
}

// Function to query all tokens for a CW721 contract
async function queryAllTokens(rpcEndpoint, contractAddress) {
  console.log(`Querying tokens for CW721 contract ${contractAddress}...`);

  const tokenQueryPayload = { all_tokens: {} };
  const { data, status } = await sendContractQuery(rpcEndpoint, contractAddress, tokenQueryPayload);

  if (status === 200 && data && data.tokens) {
    console.log(`Found tokens for ${contractAddress}: ${data.tokens}`);
    return data.tokens;
  } else {
    console.log(`No tokens found for contract ${contractAddress}.`);
    return [];
  }
}

// Stage 1: Fetch all token IDs and store them in the database for each CW721 contract
async function fetchAndStoreTokensForContracts(rpcEndpoint, db) {
  const sql = "SELECT address FROM contracts WHERE type = 'CW721'";
  const cw721Contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of cw721Contracts) {
    const contractAddress = contract.address;
    const tokens = await queryAllTokens(rpcEndpoint, contractAddress);

    if (tokens.length > 0) {
      const tokenIdsStr = tokens.join(',');

      // Insert the token list into the contract_tokens table
      const insertContractSQL = `INSERT OR REPLACE INTO contract_tokens (contract_address, extra_data) VALUES (?, ?)`;
      await promisify(db.run).bind(db)(insertContractSQL, [contractAddress, tokenIdsStr]);

      console.log(`Stored tokens for contract ${contractAddress}.`);
    }
  }
}

// Stage 2: Fetch and store owner information for each token in the database
async function fetchAndStoreOwnersForTokens(rpcEndpoint, db) {
  const sql = "SELECT contract_address, extra_data FROM contract_tokens";
  const contractTokens = await promisify(db.all).bind(db)(sql);

  for (const contractToken of contractTokens) {
    const contractAddress = contractToken.contract_address;
    const tokenIds = contractToken.extra_data.split(',');

    for (const token_id of tokenIds) {
      const owner = await queryTokenOwner(rpcEndpoint, contractAddress, token_id);

      if (owner) {
        const insertOwnerSQL = `INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner) VALUES (?, ?, ?)`;
        await promisify(db.run).bind(db)(insertOwnerSQL, [contractAddress, token_id, owner]);

        console.log(`Recorded ownership: Token ${token_id} owned by ${owner}`);
      }
    }
  }
}

// Function to query the owner of each token
async function queryTokenOwner(rpcEndpoint, contractAddress, token_id) {
  const ownerQueryPayload = { owner_of: { token_id } };
  const { data, status } = await sendContractQuery(rpcEndpoint, contractAddress, ownerQueryPayload);

  if (status === 200 && data && data.owner) {
    console.log(`Token ID: ${token_id}, Owner: ${data.owner}`);
    return data.owner;
  } else {
    console.log(`No owner found for token ID: ${token_id}`);
    return null;
  }
}

module.exports = { fetchAndStoreTokensForContracts, fetchAndStoreOwnersForTokens };
