import { DatabaseClient } from '../database';

describe('Index Related Tests', () => {
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

  test('should create index on table', async () => {
    // Create a test table
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        category VARCHAR(50),
        price DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    await dbClient.query(createTableQuery);

    // Create an index on the name column
    const createIndexQuery = `
      CREATE INDEX idx_products_name ON products (name)
    `;
    const result = await dbClient.query(createIndexQuery);
    expect(result).toBeDefined();
  });

  test('should create composite index', async () => {
    // Create a composite index on category and price columns
    const createCompositeIndexQuery = `
      CREATE INDEX idx_products_category_price ON products (category, price)
    `;
    const result = await dbClient.query(createCompositeIndexQuery);
    expect(result).toBeDefined();
  });

  test('should query using index', async () => {
    // Insert test data
    const insertQuery = `
      INSERT INTO products (name, category, price)
      VALUES ($1, $2, $3)
    `;
    const testData = [
      ['Laptop', 'Electronics', 999.99],
      ['Phone', 'Electronics', 599.99],
      ['Book', 'Education', 29.99],
      ['Desk', 'Furniture', 199.99]
    ];

    for (const data of testData) {
      await dbClient.query(insertQuery, data);
    }

    // Query using the index
    const selectQuery = 'SELECT * FROM products WHERE name = $1';
    const result = await dbClient.query(selectQuery, ['Laptop']);
    
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].name).toBe('Laptop');
    expect(result.rows[0].category).toBe('Electronics');
    expect(result.rows[0].price).toBe(999.99);
  });

  test('should query using composite index', async () => {
    // Query using the composite index
    const selectQuery = 'SELECT * FROM products WHERE category = $1 AND price < $2';
    const result = await dbClient.query(selectQuery, ['Electronics', 800.00]);
    
    expect(result.rows).toHaveLength(2);
    // Should return both Laptop and Phone
    const categories = result.rows.map((row: any) => row.category);
    expect(categories).toEqual(['Electronics', 'Electronics']);
  });
});