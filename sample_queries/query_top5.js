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

async function getTopCodeIds() {
  try {
    const query = `
      SELECT 
        c.code_id,
        COUNT(c.address) as contract_count,
        ci.creator,
        ci.data_hash
      FROM 
        contracts c
      JOIN 
        code_infos ci ON c.code_id = ci.code_id
      GROUP BY 
        c.code_id
      ORDER BY 
        contract_count DESC
      LIMIT 5
    `;

    const results = await dbAll(query);
    
    console.log('Top 5 Code IDs by number of contract addresses:');
    results.forEach((row, index) => {
      console.log(`\n${index + 1}. Code ID: ${row.code_id}`);
      console.log(`   Number of Contracts: ${row.contract_count}`);
      console.log(`   Creator: ${row.creator}`);
      console.log(`   Data Hash: ${row.data_hash}`);
    });

  } catch (err) {
    console.error('Error running query:', err);
  } finally {
    // Close the database connection
    db.close((err) => {
      if (err) {
        console.error(err.message);
      }
      console.log('\nClosed the database connection.');
    });
  }
}

getTopCodeIds();