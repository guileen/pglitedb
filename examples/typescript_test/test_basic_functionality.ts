import { Client } from 'pg';
import { dbConfig } from './src/config';

async function testBasicFunctionality() {
  const client = new Client(dbConfig);
  try {
    console.log('Connecting to database...');
    await client.connect();
    console.log('Connected successfully');
    
    // Test simple SELECT
    console.log('Testing simple SELECT...');
    const selectResult = await client.query('SELECT 1 as test');
    console.log('Simple SELECT result:', selectResult.rows);
    
    // Test simple arithmetic
    console.log('Testing simple arithmetic...');
    const arithmeticResult = await client.query('SELECT 1 + 1 as sum');
    console.log('Arithmetic result:', arithmeticResult.rows);
    
  } catch (error: any) {
    console.error('Error:', error);
  } finally {
    await client.end();
  }
}

testBasicFunctionality();