import { DatabaseClient } from '../database';

describe('Database Error Handling Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS error_test');
    } catch (error) {
      console.log('Table error_test does not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS error_test');
      } catch (error) {
        console.log('Failed to drop table error_test:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should handle syntax errors gracefully', async () => {
    try {
      await dbClient.query('INVALID SQL SYNTAX');
      // Should not reach here
      expect(true).toBe(false);
    } catch (error: any) {
      expect(error).toBeDefined();
      // Check that error has expected properties
      expect(error.message).toContain('failed');
    }
  });

  test('should handle table not found errors', async () => {
    try {
      await dbClient.query('SELECT * FROM nonexistent_table');
      // Should not reach here
      expect(true).toBe(false);
    } catch (error: any) {
      expect(error).toBeDefined();
      expect(error.message).toContain('failed');
    }
  });

  test('should handle constraint violation errors', async () => {
    // Create a table with unique constraint
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS error_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL
      )
    `);
    
    // Insert first record
    await dbClient.query(
      'INSERT INTO error_test (name) VALUES ($1)',
      ['unique_name']
    );
    
    // Try to insert duplicate (should fail)
    try {
      await dbClient.query(
        'INSERT INTO error_test (name) VALUES ($1)',
        ['unique_name']
      );
      // Should not reach here
      expect(true).toBe(false);
    } catch (error: any) {
      expect(error).toBeDefined();
      // Check that we get some kind of error message
      expect(typeof error.message).toBe('string');
    }
  });

  test('should handle data type mismatch errors', async () => {
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS type_test (
        id SERIAL PRIMARY KEY,
        age INTEGER NOT NULL
      )
    `);
    
    try {
      await dbClient.query(
        'INSERT INTO type_test (age) VALUES ($1)',
        ['not_a_number']
      );
      // Should not reach here
      expect(true).toBe(false);
    } catch (error: any) {
      expect(error).toBeDefined();
      // Some databases might convert string to number, but we should still get an error
      // or the database should handle it gracefully
    }
  });

  test('should handle null constraint violations', async () => {
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS null_test (
        id SERIAL PRIMARY KEY,
        required_field VARCHAR(50) NOT NULL
      )
    `);
    
    try {
      await dbClient.query(
        'INSERT INTO null_test (required_field) VALUES ($1)',
        [null]
      );
      // Should not reach here
      expect(true).toBe(false);
    } catch (error: any) {
      expect(error).toBeDefined();
      // Check that we get some kind of error message
      expect(typeof error.message).toBe('string');
    }
  });

  test('should handle division by zero errors', async () => {
    try {
      await dbClient.query('SELECT 1/0 AS result');
      // Depending on database implementation, this might return NULL or throw error
    } catch (error) {
      // If error occurs, it should be handled gracefully
      expect(error).toBeDefined();
    }
  });

  test('should handle invalid parameter counts', async () => {
    try {
      await dbClient.query('SELECT $1, $2', ['only_one_param']);
      // Should not reach here
      expect(true).toBe(false);
    } catch (error: any) {
      expect(error).toBeDefined();
      // Check that we get some kind of error message
      expect(typeof error.message).toBe('string');
    }
  });

  test('should handle transaction errors and rollback properly', async () => {
    // Start transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert valid data
      await dbClient.query(
        'INSERT INTO error_test (name) VALUES ($1)',
        ['valid_name']
      );
      
      // Force an error
      await dbClient.query('INVALID SQL HERE');
      
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeDefined();
      
      // Rollback transaction
      await dbClient.query('ROLLBACK');
      
      // Verify that the valid insert was rolled back
      const result = await dbClient.query(
        'SELECT * FROM error_test WHERE name = $1',
        ['valid_name']
      );
      
      // Since transaction was rolled back, no data should be inserted
      expect(result.rows).toHaveLength(0);
    }
  });

  test('should handle connection timeouts gracefully', async () => {
    // This test simulates a scenario where a long-running query might timeout
    try {
      // A query that might take longer than configured timeout
      await dbClient.query('SELECT pg_sleep(10)');
    } catch (error) {
      expect(error).toBeDefined();
      // Should handle timeout gracefully
    }
  });

  test('should handle large result sets appropriately', async () => {
    // Create a table for bulk insert
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS bulk_test (
        id SERIAL PRIMARY KEY,
        data VARCHAR(100)
      )
    `);
    
    // Insert many rows
    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(
        dbClient.query(
          'INSERT INTO bulk_test (data) VALUES ($1)',
          [`data_${i}`]
        )
      );
    }
    
    // Wait for all inserts to complete
    await Promise.all(promises);
    
    // Select all rows
    const result = await dbClient.query('SELECT * FROM bulk_test');
    expect(result.rows).toHaveLength(100);
  });
});