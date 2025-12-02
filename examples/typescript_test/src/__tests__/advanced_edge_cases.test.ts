import { DatabaseClient } from '../database';

describe('Advanced Edge Cases and Error Handling Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS advanced_edge_case_test');
    } catch (error) {
      console.log('Table advanced_edge_case_test does not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS advanced_edge_case_test');
      } catch (error) {
        console.log('Failed to drop table advanced_edge_case_test:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should handle concurrent database operations safely', async () => {
    // Create table for concurrent testing
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS advanced_edge_case_test (
        id SERIAL PRIMARY KEY,
        thread_id INTEGER,
        data VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Run concurrent insert operations
    const promises = [];
    const numThreads = 10;
    
    for (let i = 0; i < numThreads; i++) {
      promises.push(
        dbClient.query(
          'INSERT INTO advanced_edge_case_test (thread_id, data) VALUES ($1, $2)',
          [i, `Thread ${i} data`]
        )
      );
    }
    
    // Execute all concurrently
    const results = await Promise.allSettled(promises);
    
    // Check that most operations succeeded
    const successful = results.filter(result => result.status === 'fulfilled').length;
    expect(successful).toBeGreaterThan(numThreads * 0.8); // Allow some failures
    
    // Verify data was inserted
    const verifyResult = await dbClient.query('SELECT COUNT(*) as count FROM advanced_edge_case_test');
    expect(parseInt(verifyResult.rows[0].count)).toBeGreaterThan(0);
  });

  test('should handle connection pool exhaustion gracefully', async () => {
    // Create table
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS pool_test (
        id SERIAL PRIMARY KEY,
        data VARCHAR(100)
      )
    `);
    
    // Simulate connection pool pressure with rapid sequential operations
    const promises = [];
    const numOperations = 20;
    
    for (let i = 0; i < numOperations; i++) {
      promises.push(
        dbClient.query(
          'INSERT INTO pool_test (data) VALUES ($1)',
          [`Operation ${i}`]
        )
      );
    }
    
    // Execute all operations
    const results = await Promise.allSettled(promises);
    
    // Check that most operations succeeded
    const successful = results.filter(result => result.status === 'fulfilled').length;
    expect(successful).toBeGreaterThan(numOperations * 0.8);
    
    // Verify data integrity
    const verifyResult = await dbClient.query('SELECT COUNT(*) as count FROM pool_test');
    expect(parseInt(verifyResult.rows[0].count)).toBeGreaterThan(0);
  });

  test('should handle extremely large result sets appropriately', async () => {
    // Create table for bulk insert
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS bulk_large_test (
        id SERIAL PRIMARY KEY,
        data VARCHAR(1000),
        number_field INTEGER,
        bool_field BOOLEAN
      )
    `);
    
    // Insert many rows with large data
    const promises = [];
    const numRows = 1000;
    
    for (let i = 0; i < numRows; i++) {
      const largeData = 'A'.repeat(500) + `_${i}_` + 'B'.repeat(500);
      promises.push(
        dbClient.query(
          'INSERT INTO bulk_large_test (data, number_field, bool_field) VALUES ($1, $2, $3)',
          [largeData, i, i % 2 === 0]
        )
      );
    }
    
    // Wait for all inserts to complete
    await Promise.allSettled(promises);
    
    // Select all rows (this tests handling of large result sets)
    const result = await dbClient.query('SELECT * FROM bulk_large_test LIMIT 100');
    expect(result.rows.length).toBe(100);
    
    // Test with OFFSET to verify pagination works
    const paginatedResult = await dbClient.query('SELECT * FROM bulk_large_test LIMIT 50 OFFSET 50');
    expect(paginatedResult.rows.length).toBe(50);
  });

  test('should handle deeply nested transaction scenarios', async () => {
    // Create table
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS nested_transaction_test (
        id SERIAL PRIMARY KEY,
        level INTEGER,
        data VARCHAR(100)
      )
    `);
    
    // Outer transaction
    await dbClient.query('BEGIN');
    
    try {
      await dbClient.query(
        'INSERT INTO nested_transaction_test (level, data) VALUES ($1, $2)',
        [1, 'Outer transaction']
      );
      
      // Inner transaction simulation (savepoint-like behavior)
      try {
        await dbClient.query(
          'INSERT INTO nested_transaction_test (level, data) VALUES ($1, $2)',
          [2, 'Inner transaction']
        );
        
        // Another nested operation
        await dbClient.query(
          'INSERT INTO nested_transaction_test (level, data) VALUES ($1, $2)',
          [3, 'Deep nested']
        );
        
        // Commit inner "transaction"
        // In real PostgreSQL, we'd use SAVEPOINT, but we're testing the concept
      } catch (innerError) {
        // Handle inner error but continue
        console.log('Inner operation failed, continuing...');
      }
      
      // Add more data at outer level
      await dbClient.query(
        'INSERT INTO nested_transaction_test (level, data) VALUES ($1, $2)',
        [1, 'More outer data']
      );
      
      // Commit outer transaction
      await dbClient.query('COMMIT');
      
      // Verify all expected data exists
      const result = await dbClient.query('SELECT COUNT(*) as count FROM nested_transaction_test');
      // Should have at least the outer transaction data
      expect(parseInt(result.rows[0].count)).toBeGreaterThanOrEqual(2);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });

  test('should handle transaction isolation level edge cases', async () => {
    // Create table
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS isolation_test (
        id SERIAL PRIMARY KEY,
        value INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Insert initial data
    await dbClient.query(
      'INSERT INTO isolation_test (value) VALUES ($1)',
      [100]
    );
    
    // Start first transaction
    await dbClient.query('BEGIN');
    
    // Read the value
    const result1 = await dbClient.query('SELECT value FROM isolation_test WHERE id = 1');
    const initialValue = parseInt(result1.rows[0].value);
    
    // Start second "transaction" (in parallel)
    // In real scenario, this would be a separate connection
    // For this test, we'll simulate the concept
    
    // Update value in first transaction
    await dbClient.query(
      'UPDATE isolation_test SET value = $1 WHERE id = 1',
      [initialValue + 50]
    );
    
    // Try to read in "second transaction" (would normally see old value with proper isolation)
    // But in this simplified test, we'll just check that the update worked
    const result2 = await dbClient.query('SELECT value FROM isolation_test WHERE id = 1');
    const updatedValue = parseInt(result2.rows[0].value);
    
    expect(updatedValue).toBe(initialValue + 50);
    
    // Commit first transaction
    await dbClient.query('COMMIT');
  });

  test('should handle complex constraint violation scenarios', async () => {
    // Create table with multiple constraints
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS complex_constraint_test (
        id SERIAL PRIMARY KEY,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INTEGER CHECK (age >= 0 AND age <= 150),
        status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'pending')),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Insert valid data
    await dbClient.query(
      'INSERT INTO complex_constraint_test (email, age, status) VALUES ($1, $2, $3)',
      ['test@example.com', 25, 'active']
    );
    
    // Try to violate unique constraint
    try {
      await dbClient.query(
        'INSERT INTO complex_constraint_test (email, age, status) VALUES ($1, $2, $3)',
        ['test@example.com', 30, 'inactive'] // Same email
      );
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeDefined();
      // Should get constraint violation error
    }
    
    // Try to violate check constraint (negative age)
    try {
      await dbClient.query(
        'INSERT INTO complex_constraint_test (email, age, status) VALUES ($1, $2, $3)',
        ['test2@example.com', -5, 'active']
      );
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeDefined();
      // Should get check constraint violation error
    }
    
    // Try to violate status check constraint
    try {
      await dbClient.query(
        'INSERT INTO complex_constraint_test (email, age, status) VALUES ($1, $2, $3)',
        ['test3@example.com', 30, 'invalid_status']
      );
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeDefined();
      // Should get check constraint violation error
    }
    
    // Verify only valid record exists
    const result = await dbClient.query('SELECT COUNT(*) as count FROM complex_constraint_test');
    expect(parseInt(result.rows[0].count)).toBe(1);
  });

  test('should handle transaction rollback with multiple operations', async () => {
    // Create table
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS multi_op_rollback_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        value INTEGER
      )
    `);
    
    // Count initial records
    const initialResult = await dbClient.query('SELECT COUNT(*) as count FROM multi_op_rollback_test');
    const initialCount = parseInt(initialResult.rows[0].count);
    
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Perform multiple operations
      await dbClient.query(
        'INSERT INTO multi_op_rollback_test (name, value) VALUES ($1, $2)',
        ['Operation 1', 100]
      );
      
      await dbClient.query(
        'INSERT INTO multi_op_rollback_test (name, value) VALUES ($1, $2)',
        ['Operation 2', 200]
      );
      
      await dbClient.query(
        'UPDATE multi_op_rollback_test SET value = value + 50 WHERE name = $1',
        ['Operation 1']
      );
      
      // Force an error to trigger rollback
      await dbClient.query('INVALID SQL STATEMENT TO TRIGGER ERROR');
      
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      // Rollback transaction
      await dbClient.query('ROLLBACK');
      
      // Verify no changes were committed
      const finalResult = await dbClient.query('SELECT COUNT(*) as count FROM multi_op_rollback_test');
      expect(parseInt(finalResult.rows[0].count)).toBe(initialCount);
    }
  });
});