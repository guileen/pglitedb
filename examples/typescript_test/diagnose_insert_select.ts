import { Client } from 'pg';
import { dbConfig } from './src/config';

async function diagnoseInsertSelect() {
  const client = new Client(dbConfig);
  try {
    console.log('Connecting to database...');
    await client.connect();
    console.log('Connected successfully');
    
    // Create a fresh table
    console.log('Creating fresh table...');
    await client.query('DROP TABLE IF EXISTS test_insert_select');
    const createResult = await client.query(`
      CREATE TABLE test_insert_select (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        value INTEGER
      )
    `);
    console.log('Table created');
    
    // Insert data with explicit transaction
    console.log('Inserting data with explicit transaction...');
    await client.query('BEGIN');
    const insertResult = await client.query(
      'INSERT INTO test_insert_select (name, value) VALUES ($1, $2)',
      ['test1', 100]
    );
    console.log('Insert result:', insertResult);
    await client.query('COMMIT');
    
    // Check row count
    console.log('Checking row count...');
    const countResult = await client.query('SELECT COUNT(*) as count FROM test_insert_select');
    console.log('Count result:', countResult.rows);
    
    // Select all data
    console.log('Selecting all data...');
    const selectResult = await client.query('SELECT * FROM test_insert_select');
    console.log('Select result:', selectResult.rows);
    
    // Try selecting with WHERE clause
    console.log('Selecting with WHERE clause...');
    const whereResult = await client.query('SELECT * FROM test_insert_select WHERE name = $1', ['test1']);
    console.log('WHERE result:', whereResult.rows);
    
  } catch (error: any) {
    console.error('Error:', error);
  } finally {
    await client.end();
  }
}

diagnoseInsertSelect();