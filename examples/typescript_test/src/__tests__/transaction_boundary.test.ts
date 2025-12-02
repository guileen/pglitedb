import { DatabaseClient } from '../database';

describe('Transaction Boundary Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS transaction_test');
    } catch (error) {
      console.log('Table transaction_test does not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS transaction_test');
      } catch (error) {
        console.log('Failed to drop table transaction_test:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should handle basic transaction commit', async () => {
    // Create table
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS transaction_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL
      )
    `);
    
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert data
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Transaction Commit Test']
      );
      
      // Commit transaction
      await dbClient.query('COMMIT');
      
      // Verify data was committed
      const result = await dbClient.query(
        'SELECT * FROM transaction_test WHERE name = $1',
        ['Transaction Commit Test']
      );
      expect(result.rows).toHaveLength(1);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });

  test('should handle transaction rollback', async () => {
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert data
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Transaction Rollback Test']
      );
      
      // Force rollback
      await dbClient.query('ROLLBACK');
      
      // Verify data was not committed
      const result = await dbClient.query(
        'SELECT * FROM transaction_test WHERE name = $1',
        ['Transaction Rollback Test']
      );
      expect(result.rows).toHaveLength(0);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });

  test('should handle nested transaction operations', async () => {
    // Begin outer transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert first record
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Nested Transaction 1']
      );
      
      // Begin inner transaction (savepoint)
      await dbClient.query('BEGIN');
      
      try {
        // Insert second record
        await dbClient.query(
          'INSERT INTO transaction_test (name) VALUES ($1)',
          ['Nested Transaction 2']
        );
        
        // Commit inner transaction
        await dbClient.query('COMMIT');
      } catch (error) {
        await dbClient.query('ROLLBACK');
        throw error;
      }
      
      // Commit outer transaction
      await dbClient.query('COMMIT');
      
      // Verify both records were committed
      const result = await dbClient.query('SELECT COUNT(*) as count FROM transaction_test');
      expect(parseInt(result.rows[0].count)).toBeGreaterThanOrEqual(2);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });

  test('should handle transaction with multiple operations', async () => {
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Multiple insert operations
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Multi Op 1']
      );
      
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Multi Op 2']
      );
      
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Multi Op 3']
      );
      
      // Update operation
      await dbClient.query(
        'UPDATE transaction_test SET name = $1 WHERE name = $2',
        ['Updated Multi Op 1', 'Multi Op 1']
      );
      
      // Commit all operations
      await dbClient.query('COMMIT');
      
      // Verify results
      const countResult = await dbClient.query('SELECT COUNT(*) as count FROM transaction_test');
      expect(parseInt(countResult.rows[0].count)).toBeGreaterThanOrEqual(3);
      
      const updatedResult = await dbClient.query(
        'SELECT * FROM transaction_test WHERE name = $1',
        ['Updated Multi Op 1']
      );
      expect(updatedResult.rows).toHaveLength(1);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });

  test('should handle transaction rollback on error', async () => {
    const initialCountResult = await dbClient.query('SELECT COUNT(*) as count FROM transaction_test');
    const initialCount = parseInt(initialCountResult.rows[0].count);
    
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert data
      await dbClient.query(
        'INSERT INTO transaction_test (name) VALUES ($1)',
        ['Error Test']
      );
      
      // Force an error (invalid SQL)
      await dbClient.query('INVALID SQL STATEMENT');
      
      // This should not be reached
      expect(true).toBe(false);
    } catch (error) {
      // Rollback on error
      await dbClient.query('ROLLBACK');
      
      // Verify no data was committed
      const finalCountResult = await dbClient.query('SELECT COUNT(*) as count FROM transaction_test');
      expect(parseInt(finalCountResult.rows[0].count)).toBe(initialCount);
    }
  });
});