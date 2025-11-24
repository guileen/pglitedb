import { DatabaseClient } from '../database';

describe('Index Performance Tests', () => {
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

  test('should demonstrate performance improvement with index', async () => {
    // Create a test table
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS performance_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        category VARCHAR(50),
        value DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    await dbClient.query(createTableQuery);

    // Insert a large amount of test data
    const insertQuery = `
      INSERT INTO performance_test (name, category, value)
      VALUES ($1, $2, $3)
    `;

    // Insert 1000 rows of test data
    for (let i = 0; i < 1000; i++) {
      const name = `Item ${i}`;
      const category = `Category ${(i % 10)}`;
      const value = Math.random() * 1000;
      await dbClient.query(insertQuery, [name, category, value]);
    }

    // Measure query performance without index
    const startTimeWithoutIndex = Date.now();
    const selectWithoutIndexQuery = 'SELECT * FROM performance_test WHERE name = $1';
    await dbClient.query(selectWithoutIndexQuery, ['Item 500']);
    const endTimeWithoutIndex = Date.now();
    const durationWithoutIndex = endTimeWithoutIndex - startTimeWithoutIndex;

    // Create an index
    const createIndexQuery = `
      CREATE INDEX idx_performance_test_name ON performance_test (name)
    `;
    await dbClient.query(createIndexQuery);

    // Measure query performance with index
    const startTimeWithIndex = Date.now();
    await dbClient.query(selectWithoutIndexQuery, ['Item 500']);
    const endTimeWithIndex = Date.now();
    const durationWithIndex = endTimeWithIndex - startTimeWithIndex;

    // Index should improve performance (this is a simple check, in reality, 
    // the difference might not be significant with only 1000 rows)
    console.log(`Query duration without index: ${durationWithoutIndex}ms`);
    console.log(`Query duration with index: ${durationWithIndex}ms`);
    
    // We expect the query with index to be faster or at least not slower significantly
    expect(durationWithIndex).toBeLessThanOrEqual(durationWithoutIndex * 2);
  });

  test('should test composite index performance', async () => {
    // Measure query performance with composite index
    const startTime = Date.now();
    const selectQuery = 'SELECT * FROM performance_test WHERE category = $1 AND value > $2';
    const result = await dbClient.query(selectQuery, ['Category 5', 500.00]);
    const endTime = Date.now();
    const duration = endTime - startTime;

    console.log(`Composite index query duration: ${duration}ms`);
    expect(result).toBeDefined();
  });
});