import { Client } from 'pg';
import { dbConfig } from './src/config';

async function testDetailed() {
  const client = new Client(dbConfig);
  try {
    console.log('Connecting to database...');
    await client.connect();
    console.log('Connected successfully');
    
    // Try to create a simple table and log each step
    console.log('Creating table...');
    const createResult = await client.query(`
      CREATE TABLE IF NOT EXISTS debug_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('Create table result:', createResult);
    
    // Check if table exists
    console.log('Checking tables...');
    const tablesResult = await client.query(`
      SELECT tablename FROM pg_tables WHERE tablename = 'debug_table'
    `);
    console.log('Tables result:', tablesResult.rows);
    
    // Try to insert data
    console.log('Inserting data...');
    const insertResult = await client.query(
      'INSERT INTO debug_table (name) VALUES ($1) RETURNING *',
      ['test_entry']
    );
    console.log('Insert result:', insertResult);
    
    // Try to select data
    console.log('Selecting data...');
    const selectResult = await client.query('SELECT * FROM debug_table');
    console.log('Select result:', selectResult.rows);
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.end();
  }
}

testDetailed();