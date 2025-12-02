import { Client } from 'pg';
import { dbConfig } from './src/config';

async function diagnoseCreateTable() {
  const client = new Client(dbConfig);
  try {
    console.log('Connecting to database...');
    await client.connect();
    console.log('Connected successfully');
    
    // Try to drop table if exists
    console.log('Dropping table if exists...');
    try {
      await client.query('DROP TABLE IF EXISTS diagnose_table');
      console.log('Table dropped successfully');
    } catch (error: any) {
      console.log('Table does not exist or drop failed:', error.message);
    }
    
    // Try to create table
    console.log('Creating table...');
    const createResult = await client.query(`
      CREATE TABLE diagnose_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('Create table result:', createResult);
    
    // Check if table exists
    console.log('Checking if table exists...');
    const checkResult = await client.query(`
      SELECT tablename FROM pg_tables WHERE tablename = 'diagnose_table'
    `);
    console.log('Table check result:', checkResult.rows);
    
    // Also check all tables
    console.log('Checking all tables...');
    const allTablesResult = await client.query(`
      SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    `);
    console.log('All tables:', allTablesResult.rows);
    
    // Try to insert data
    console.log('Inserting data...');
    const insertResult = await client.query(
      'INSERT INTO diagnose_table (name) VALUES ($1)',
      ['test_entry']
    );
    console.log('Insert result:', insertResult);
    
    // Try to select data
    console.log('Selecting data...');
    const selectResult = await client.query('SELECT * FROM diagnose_table');
    console.log('Select result:', selectResult.rows);
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.end();
  }
}

diagnoseCreateTable();