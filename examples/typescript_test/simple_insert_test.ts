import { Client } from 'pg';
import { dbConfig } from './src/config';

async function testSimpleInsert() {
  const client = new Client(dbConfig);
  try {
    console.log('Connecting to database...');
    await client.connect();
    console.log('Connected successfully');
    
    // Try to create a simple table
    console.log('Creating table...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS simple_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL
      )
    `);
    
    // Try simple insert without RETURNING
    console.log('Inserting data without RETURNING...');
    const insertResult = await client.query(
      'INSERT INTO simple_table (name) VALUES ($1)',
      ['simple_test']
    );
    console.log('Simple insert result:', insertResult);
    
    // Try to select data
    console.log('Selecting data...');
    const selectResult = await client.query('SELECT * FROM simple_table');
    console.log('Select result:', selectResult.rows);
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.end();
  }
}

testSimpleInsert();