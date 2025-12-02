import { DatabaseClient } from '../database';

describe('Basic Database Connectivity Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
  });

  afterAll(async () => {
    if (dbClient) {
      await dbClient.disconnect();
    }
  });

  test('should connect to database', async () => {
    expect(dbClient).toBeDefined();
  });

  test('should create and drop a simple table', async () => {
    // Create a simple table
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS test_simple (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL
      )
    `;
    
    const createResult = await dbClient.query(createTableQuery);
    expect(createResult).toBeDefined();
    
    // Drop the table
    const dropTableQuery = 'DROP TABLE IF EXISTS test_simple';
    const dropResult = await dbClient.query(dropTableQuery);
    expect(dropResult).toBeDefined();
  });
});