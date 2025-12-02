import { Client } from 'pg';
import { dbConfig } from './src/config';

async function test() {
  const client = new Client(dbConfig);
  try {
    await client.connect();
    console.log('Connected successfully');
    
    // Try to create a simple table
    const result = await client.query(`
      CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL
      )
    `);
    console.log('Table created:', result);
    
    // Try to insert data
    const insertResult = await client.query(
      'INSERT INTO test_table (name) VALUES ($1) RETURNING *',
      ['test_name']
    );
    console.log('Insert result:', insertResult.rows);
    
    // Try to select data
    const selectResult = await client.query('SELECT * FROM test_table');
    console.log('Select result:', selectResult.rows);
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.end();
  }
}

test();