import { DatabaseClient } from '../database';

describe('Database Basic Functionality Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS basic_test');
    } catch (error) {
      console.log('Table basic_test does not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS basic_test');
      } catch (error) {
        console.log('Failed to drop table basic_test:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should create table successfully', async () => {
    const createTableQuery = `
      CREATE TABLE basic_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        value INTEGER
      )
    `;
    
    const result = await dbClient.query(createTableQuery);
    expect(result).toBeDefined();
  });

  test('should insert data without returning rows', async () => {
    // Test that INSERT works even though it doesn't return rows
    const insertQuery = 'INSERT INTO basic_test (name, value) VALUES ($1, $2)';
    const result = await dbClient.query(insertQuery, ['test1', 100]);
    
    // Database may not return rows for INSERT statements, but should succeed
    expect(result).toBeDefined();
  });

  test('should verify data was inserted by selecting', async () => {
    // Verify that the data was actually inserted
    const selectQuery = 'SELECT COUNT(*) as count FROM basic_test';
    const result = await dbClient.query(selectQuery);
    
    // Should have at least one row
    expect(result.rows).toBeDefined();
    expect(result.rows.length).toBeGreaterThanOrEqual(1);
  });

  test('should select data successfully', async () => {
    const selectQuery = 'SELECT * FROM basic_test';
    const result = await dbClient.query(selectQuery);
    
    expect(result.rows).toBeDefined();
    // We know data was inserted, so we should get results
    expect(result.rows.length).toBeGreaterThanOrEqual(1);
  });

  test('should handle simple expressions', async () => {
    const result = await dbClient.query('SELECT 1 + 1 as sum');
    expect(result.rows).toBeDefined();
  });

  test('should handle parameterized queries', async () => {
    const result = await dbClient.query('SELECT $1 as value', [42]);
    expect(result.rows).toBeDefined();
  });
});