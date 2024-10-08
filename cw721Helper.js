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
    // Log more details if the query fails
    console.error(`Error querying contract ${contractAddress}: ${error.message}`);
    if (error.response) {
      console.error(`Status Code: ${error.response.status}, Data: ${JSON.stringify(error.response.data)}`);
    }
    return { error: error.response?.data || error.message, status: error.response?.status || 500 };
  }
}

// Function to determine the contract type
async function determineContractType(rpcEndpoint, contractAddress) {
  const testPayload = { a: "b" }; // Example invalid payload
  const { error, status } = await sendContractQuery(rpcEndpoint, contractAddress, testPayload);

  if (status === 400 && error && error.message) {
    const errorMessage = error.message.toLowerCase();

    if (errorMessage.includes('cw721')) {
      return 'CW721';
    } else if (errorMessage.includes('cw20')) {
      return 'CW20';
    } else if (errorMessage.includes('erc1155')) {
      return 'CW1155';
    } else if (errorMessage.includes('unknown variant')) {
      return 'CW404'; // Indicating an unknown contract type
    }
  }
  return null;
}

// Function to query all tokens for a CW721 contract
async function queryAllTokens(rpcEndpoint, contractAddress) {
  console.log(`Querying tokens for CW721 contract ${contractAddress}...`);

  const tokenQueryPayload = { all_tokens: {} };
  const { data, error, status } = await sendContractQuery(rpcEndpoint, contractAddress, tokenQueryPayload);

  if (status === 200 && data && data.data && Array.isArray(data.data.tokens)) {
    console.log(`Found tokens for ${contractAddress}: ${data.data.tokens}`);
    return data.data.tokens;
  } else {
    console.log(`No tokens found for contract ${contractAddress}. Status: ${status}, Error: ${error || 'No data returned'}`);
    return [];
  }
}

// Function to query the owner of each token
async function queryTokenOwner(rpcEndpoint, contractAddress, token_id) {
  console.log(`Querying owner for token ID: ${token_id} in contract: ${contractAddress}`);

  // Ensure token_id is passed as a string
  const ownerQueryPayload = { owner_of: { token_id: token_id.toString() } };
  
  const { data, error, status } = await sendContractQuery(rpcEndpoint, contractAddress, ownerQueryPayload);

  if (status === 200 && data && data.data && data.data.owner) {
    console.log(`Token ID: ${token_id}, Owner: ${data.data.owner}`);
    return data.data.owner;
  } else {
    console.log(`No owner found for token ID: ${token_id}. Status: ${status}, Error: ${error || 'No data returned'}`);
    return null;
  }
}

// Stage 1: Fetch all token IDs for all CW721 contracts and record them
async function fetchAllTokensForContracts(rpcEndpoint, db) {
  const sql = "SELECT address FROM contracts WHERE type = 'CW721'";
  const cw721Contracts = await promisify(db.all).bind(db)(sql);

  for (const contract of cw721Contracts) {
    const contractAddress = contract.address;
    const tokens = await queryAllTokens(rpcEndpoint, contractAddress);

    if (tokens.length > 0) {
      const tokenIdsStr = tokens.join(',');

      // Store token IDs in the contract_tokens table
      const insertContractSQL = `INSERT OR REPLACE INTO contract_tokens (contract_address, extra_data) VALUES (?, ?)`;
      await promisify(db.run).bind(db)(insertContractSQL, [contractAddress, tokenIdsStr]);

      console.log(`Stored tokens for contract ${contractAddress}.`);
    }
  }
}

// Stage 2: Fetch and store owner information for each token
async function fetchTokenOwners(rpcEndpoint, db) {
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

// Main function to handle the CW721 contract
async function handleContract(rpcEndpoint, contractAddress, db) {
  const tokens = await queryAllTokens(rpcEndpoint, contractAddress);

  if (Array.isArray(tokens) && tokens.length > 0) {
    const tokenIdsStr = tokens.join(',');

    const insertContractSQL = `INSERT OR REPLACE INTO contract_tokens (contract_address, extra_data) VALUES (?, ?)`;
    await promisify(db.run).bind(db)(insertContractSQL, [contractAddress, tokenIdsStr]);

    console.log(`Inserted contract tokens for ${contractAddress}.`);

    for (const token_id of tokens) {
      const owner = await queryTokenOwner(rpcEndpoint, contractAddress, token_id);

      if (owner) {
        const insertOwnerSQL = `INSERT OR REPLACE INTO nft_owners (collection_address, token_id, owner)
          VALUES (?, ?, ?)`;
        await promisify(db.run).bind(db)(insertOwnerSQL, [contractAddress, token_id, owner]);

        console.log(`Recorded ownership: Token ${token_id} owned by ${owner}`);
      }
    }
  } else {
    console.log(`No tokens found or an error occurred for contract ${contractAddress}.`);
  }
}

module.exports = { handleContract, determineContractType, fetchAllTokensForContracts, fetchTokenOwners };
