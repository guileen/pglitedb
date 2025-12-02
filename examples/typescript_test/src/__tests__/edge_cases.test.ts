import { DatabaseClient } from '../database';

describe('Edge Cases and Boundary Conditions Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS edge_case_test');
    } catch (error) {
      console.log('Table edge_case_test does not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS edge_case_test');
      } catch (error) {
        console.log('Failed to drop table edge_case_test:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should handle empty string values', async () => {
    const createTableQuery = `
      CREATE TABLE edge_case_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        description TEXT
      )
    `;
    await dbClient.query(createTableQuery);
    
    // Insert empty string
    await dbClient.query(
      'INSERT INTO edge_case_test (name, description) VALUES ($1, $2)',
      ['', 'Empty name test']
    );
    
    const result = await dbClient.query('SELECT * FROM edge_case_test WHERE name = $1', ['']);
    expect(result.rows).toHaveLength(1);
  });

  test('should handle null values', async () => {
    await dbClient.query(
      'INSERT INTO edge_case_test (name, description) VALUES ($1, $2)',
      [null, 'Null name test']
    );
    
    const result = await dbClient.query('SELECT * FROM edge_case_test WHERE name IS NULL');
    expect(result.rows.length).toBeGreaterThanOrEqual(1);
  });

  test('should handle special characters', async () => {
    const specialName = "Test'Name\"With\\Special/Characters";
    await dbClient.query(
      'INSERT INTO edge_case_test (name, description) VALUES ($1, $2)',
      [specialName, 'Special characters test']
    );
    
    const result = await dbClient.query('SELECT * FROM edge_case_test WHERE name = $1', [specialName]);
    expect(result.rows).toHaveLength(1);
  });

  test('should handle large text values', async () => {
    const largeText = 'A'.repeat(1000);
    await dbClient.query(
      'INSERT INTO edge_case_test (name, description) VALUES ($1, $2)',
      ['Large Text Test', largeText]
    );
    
    const result = await dbClient.query('SELECT * FROM edge_case_test WHERE name = $1', ['Large Text Test']);
    expect(result.rows).toHaveLength(1);
  });

  test('should handle numeric edge cases', async () => {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS numeric_test (
        id SERIAL PRIMARY KEY,
        small_int SMALLINT,
        regular_int INTEGER,
        big_int BIGINT,
        decimal_val DECIMAL(10,2)
      )
    `;
    await dbClient.query(createTableQuery);
    
    await dbClient.query(
      'INSERT INTO numeric_test (small_int, regular_int, big_int, decimal_val) VALUES ($1, $2, $3, $4)',
      [32767, 2147483647, '9223372036854775807', '99999999.99']
    );
    
    const result = await dbClient.query('SELECT * FROM numeric_test');
    expect(result.rows).toHaveLength(1);
  });

  test('should handle date and time values', async () => {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS datetime_test (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    await dbClient.query(createTableQuery);
    
    // Insert with explicit timestamp
    await dbClient.query(
      'INSERT INTO datetime_test (created_at) VALUES ($1)',
      ['2023-01-01 12:00:00']
    );
    
    const result = await dbClient.query('SELECT * FROM datetime_test');
    expect(result.rows).toHaveLength(1);
  });

  test('should handle boolean values', async () => {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS boolean_test (
        id SERIAL PRIMARY KEY,
        is_active BOOLEAN,
        is_deleted BOOLEAN DEFAULT FALSE
      )
    `;
    await dbClient.query(createTableQuery);
    
    await dbClient.query(
      'INSERT INTO boolean_test (is_active, is_deleted) VALUES ($1, $2)',
      [true, false]
    );
    
    const result = await dbClient.query('SELECT * FROM boolean_test WHERE is_active = $1', [true]);
    expect(result.rows).toHaveLength(1);
  });
});