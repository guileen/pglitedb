import { DatabaseClient } from '../database';

describe('PostgreSQL Compatible Database Tests', () => {
  let dbClient: DatabaseClient;

  beforeAll(async () => {
    dbClient = new DatabaseClient();
    await dbClient.connect();
    
    // Clean up any existing tables first
    try {
      await dbClient.query('DROP TABLE IF EXISTS users');
    } catch (error) {
      console.log('Table users does not exist, continuing...');
    }
  });

  afterAll(async () => {
    if (dbClient) {
      // Clean up tables after tests
      try {
        await dbClient.query('DROP TABLE IF EXISTS users');
      } catch (error) {
        console.log('Failed to drop table users:', error);
      }
      await dbClient.disconnect();
    }
  });

  test('should connect to database', async () => {
    // Connection is tested in beforeAll
    expect(dbClient).toBeDefined();
  });

  test('should create table', async () => {
    // Clean up any existing data first
    try {
      await dbClient.query('DELETE FROM users');
    } catch (error) {
      console.log('No existing data to clean up');
    }
    
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INTEGER,
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    
    const result = await dbClient.query(createTableQuery);
    expect(result).toBeDefined();
  });

  test('should insert data', async () => {
    const insertQuery = `
      INSERT INTO users (name, email, age, active)
      VALUES ($1, $2, $3, $4)
      RETURNING id, name, email, age, active
    `;
    
    const testData = [
      ['Alice', 'alice@example.com', 30, true],
      ['Bob', 'bob@example.com', 25, false],
      ['Zara', 'zara@example.com', 28, true]
    ];
    
    for (const data of testData) {
      const result = await dbClient.query(insertQuery, data);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe(data[0]);
      expect(result.rows[0].email).toBe(data[1]);
      expect(result.rows[0].age).toBe(data[2]);
      expect(result.rows[0].active).toBe(data[3]);
    }
  });

  test('should select data', async () => {
    const selectQuery = 'SELECT * FROM users ORDER BY name ASC';
    const result = await dbClient.query(selectQuery);
    
    expect(result.rows).toHaveLength(3);
    expect(result.rows[0].name).toBe('Alice');
    expect(result.rows[1].name).toBe('Bob');
    expect(result.rows[2].name).toBe('Zara');
  });

  test('should select with WHERE clause', async () => {
    const selectQuery = 'SELECT * FROM users WHERE active = $1';
    const result = await dbClient.query(selectQuery, [true]);
    
    expect(result.rows).toHaveLength(2);
    // Both Alice and Zara should be active
    const names = result.rows.map((row: any) => row.name).sort();
    expect(names).toEqual(['Alice', 'Zara']);
  });

  test('should update data', async () => {
    const updateQuery = 'UPDATE users SET age = $1 WHERE name = $2 RETURNING *';
    const result = await dbClient.query(updateQuery, [35, 'Alice']);
    
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].age).toBe(35);
  });

  test('should delete data', async () => {
    const deleteQuery = 'DELETE FROM users WHERE name = $1';
    const result = await dbClient.query(deleteQuery, ['Bob']);
    
    // Check that Bob was deleted
    const selectQuery = 'SELECT * FROM users WHERE name = $1';
    const selectResult = await dbClient.query(selectQuery, ['Bob']);
    expect(selectResult.rows).toHaveLength(0);
  });

  test('should handle transactions', async () => {
    // Begin transaction
    await dbClient.query('BEGIN');
    
    try {
      // Insert a new user
      const insertQuery = `
        INSERT INTO users (name, email, age, active)
        VALUES ($1, $2, $3, $4)
        RETURNING id
      `;
      const result = await dbClient.query(insertQuery, ['Transaction Test', 'transaction@test.com', 40, true]);
      
      expect(result.rows).toHaveLength(1);
      
      // Rollback transaction
      await dbClient.query('ROLLBACK');
      
      // Check that the user was not actually inserted
      const selectQuery = 'SELECT * FROM users WHERE name = $1';
      const selectResult = await dbClient.query(selectQuery, ['Transaction Test']);
      expect(selectResult.rows).toHaveLength(0);
    } catch (error) {
      await dbClient.query('ROLLBACK');
      throw error;
    }
  });
});