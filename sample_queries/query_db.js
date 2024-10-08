const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');

// Open the database
const db = new sqlite3.Database('./smart_contracts.db', sqlite3.OPEN_READONLY, (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log('Connected to the smart_contracts database.');
});

// Promisify the db.all method
const dbAll = promisify(db.all).bind(db);

// Example queries
async function runQueries() {
  try {
    // Count total number of code infos
    const codeInfoCount = await dbAll('SELECT COUNT(*) as count FROM code_infos');
    console.log('Total number of code infos:', codeInfoCount[0].count);

    // Get the first 5 code infos
    const codeInfos = await dbAll('SELECT * FROM code_infos LIMIT 5');
    console.log('First 5 code infos:', codeInfos);

    // Count total number of contracts
    const contractCount = await dbAll('SELECT COUNT(*) as count FROM contracts');
    console.log('Total number of contracts:', contractCount[0].count);

    // Get the first 5 contracts
    const contracts = await dbAll('SELECT * FROM contracts LIMIT 5');
    console.log('First 5 contracts:', contracts);

    // Join query to get contracts with their code info
    const contractsWithCodeInfo = await dbAll(`
      SELECT c.address, c.code_id, ci.creator, ci.data_hash
      FROM contracts c
      JOIN code_infos ci ON c.code_id = ci.code_id
      LIMIT 5
    `);
    console.log('First 5 contracts with code info:', contractsWithCodeInfo);

  } catch (err) {
    console.error('Error running queries:', err);
  } finally {
    // Close the database connection
    db.close((err) => {
      if (err) {
        console.error(err.message);
      }
      console.log('Closed the database connection.');
    });
  }
}

runQueries();