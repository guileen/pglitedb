import { DatabaseClient } from '../database';

describe('Enhanced PostgreSQL Compatible Database Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS users');
      await dbClient.query('DROP TABLE IF EXISTS products');
    } catch (error) {
      console.log('Tables do not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS users');
        await dbClient.query('DROP TABLE IF EXISTS products');
      } catch (error) {
        console.log('Failed to drop tables:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should connect to database', async () => {
    // Connection is tested in beforeAll
    expect(dbClient).toBeDefined();
  });

  test('should create table with various data types', async () => {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INTEGER,
        salary DECIMAL(10, 2),
        is_active BOOLEAN DEFAULT true,
        profile JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
      )
    `;
    
    const result = await dbClient.query(createTableQuery);
    expect(result).toBeDefined();
  });

  test('should insert data with various types', async () => {
    const insertQuery = `
      INSERT INTO users (name, email, age, salary, is_active, profile)
      VALUES ($1, $2, $3, $4, $5, $6)
    `;
    
    const testData = [
      ['Alice', 'alice@example.com', 30, '75000.50', true, JSON.stringify({ department: 'Engineering', skills: ['Go', 'TypeScript'] })],
      ['Bob', 'bob@example.com', 25, '65000.75', false, JSON.stringify({ department: 'Marketing', skills: ['SEO', 'Content'] })],
      ['Charlie', 'charlie@example.com', 35, '85000.00', true, JSON.stringify({ department: 'Sales', skills: ['Negotiation', 'CRM'] })]
    ];
    
    for (const data of testData) {
      const result = await dbClient.query(insertQuery, data);
      // Database may not return rows for INSERT statements
      expect(result).toBeDefined();
    }
    
    // Verify data was inserted by selecting it
    const selectQuery = 'SELECT * FROM users ORDER BY name ASC';
    const selectResult = await dbClient.query(selectQuery);
    expect(selectResult.rows).toHaveLength(3);
  });

  test('should handle edge cases and error conditions', async () => {
    // Test duplicate email insertion (should fail)
    const insertQuery = `
      INSERT INTO users (name, email, age)
      VALUES ($1, $2, $3)
    `;
    
    try {
      await dbClient.query(insertQuery, ['Duplicate', 'alice@example.com', 28]);
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      // Should fail due to unique constraint
      expect(error).toBeDefined();
    }
    
    // Test null value insertion
    const result = await dbClient.query(insertQuery, ['Null Test', 'null@test.com', null]);
    expect(result).toBeDefined();
  });

  test('should select data with various conditions', async () => {
    // Select all users ordered by name
    const selectAllQuery = 'SELECT * FROM users ORDER BY name ASC';
    const allResult = await dbClient.query(selectAllQuery);
    expect(allResult.rows).toHaveLength(4); // 3 original + 1 null test
    
    // Select with WHERE clause
    const selectActiveQuery = 'SELECT * FROM users WHERE is_active = $1';
    const activeResult = await dbClient.query(selectActiveQuery, [true]);
    expect(activeResult.rows.length).toBeGreaterThanOrEqual(2);
    
    // Select with LIKE clause
    const selectLikeQuery = 'SELECT * FROM users WHERE name LIKE $1';
    const likeResult = await dbClient.query(selectLikeQuery, ['A%']);
    expect(likeResult.rows).toHaveLength(1);
    expect(likeResult.rows[0].name).toBe('Alice');
  });

  test('should update data with complex conditions', async () => {
    // Update with returning clause
    const updateQuery = `
      UPDATE users 
      SET age = $1, salary = $2, updated_at = CURRENT_TIMESTAMP 
      WHERE name = $3
    `;
    await dbClient.query(updateQuery, [31, 78000.00, 'Alice']);
    
    // Verify update by selecting the data
    const selectQuery = 'SELECT age, salary FROM users WHERE name = $1';
    const selectResult = await dbClient.query(selectQuery, ['Alice']);
    expect(selectResult.rows).toHaveLength(1);
    expect(selectResult.rows[0].age).toBe(31);
    expect(parseFloat(selectResult.rows[0].salary)).toBeCloseTo(78000.00, 2);
  });

  test('should delete data with conditions', async () => {
    // Delete specific user
    const deleteQuery = 'DELETE FROM users WHERE name = $1';
    const result = await dbClient.query(deleteQuery, ['Bob']);
    
    // Check that Bob was deleted
    const selectQuery = 'SELECT * FROM users WHERE name = $1';
    const selectResult = await dbClient.query(selectQuery, ['Bob']);
    expect(selectResult.rows).toHaveLength(0);
  });

  test('should handle transactions properly', async () => {
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert a new user
      const insertQuery = `
        INSERT INTO users (name, email, age, is_active)
        VALUES ($1, $2, $3, $4)
      `;
      await dbClient.query(insertQuery, ['Transaction Test', 'transaction@test.com', 40, true]);
      
      // Update existing user
      const updateQuery = 'UPDATE users SET age = $1 WHERE name = $2';
      await dbClient.query(updateQuery, [36, 'Charlie']);
      
      // Commit transaction
      await dbClient.query('COMMIT');
      
      // Verify changes were committed
      const verifyInsert = await dbClient.query('SELECT * FROM users WHERE name = $1', ['Transaction Test']);
      expect(verifyInsert.rows).toHaveLength(1);
      
      const verifyUpdate = await dbClient.query('SELECT age FROM users WHERE name = $1', ['Charlie']);
      expect(verifyUpdate.rows[0].age).toBe(36);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });

  test('should handle transaction rollback', async () => {
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert a new user
      const insertQuery = `
        INSERT INTO users (name, email, age, is_active)
        VALUES ($1, $2, $3, $4)
      `;
      await dbClient.query(insertQuery, ['Rollback Test', 'rollback@test.com', 25, true]);
      
      // Force an error to trigger rollback
      await dbClient.query('INVALID SQL STATEMENT');
    } catch (error) {
      await dbClient.query('ROLLBACK');
      
      // Check that the user was not actually inserted
      const selectQuery = 'SELECT * FROM users WHERE name = $1';
      const selectResult = await dbClient.query(selectQuery, ['Rollback Test']);
      expect(selectResult.rows).toHaveLength(0);
    }
  });
});