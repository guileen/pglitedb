import { Client } from 'pg';

describe('PostgreSQL Client Tests', () => {
  let client: Client;

  beforeAll(async () => {
    client = new Client({
      host: 'localhost',
      port: 5433,
      user: 'admin',
      password: 'password',
      database: 'testdb',
    });
    await client.connect();
  });

  afterAll(async () => {
    await client.end();
  });

  test('CREATE TABLE', async () => {
    await client.query('DROP TABLE IF EXISTS test_products');
    await client.query(`
      CREATE TABLE test_products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        price DECIMAL(10, 2),
        in_stock BOOLEAN DEFAULT true
      )
    `);
    console.log('✅ Created table test_products');
  });

  test('INSERT', async () => {
    const result = await client.query(
      'INSERT INTO test_products (name, price, in_stock) VALUES ($1, $2, $3) RETURNING id',
      ['Laptop', 999.99, true]
    );
    expect(result.rows.length).toBe(1);
    console.log(`✅ Inserted product: ID=${result.rows[0].id}`);
  });

  test('SELECT', async () => {
    const result = await client.query(
      'SELECT id, name, price, in_stock FROM test_products WHERE name = $1',
      ['Laptop']
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].name).toBe('Laptop');
    console.log(`✅ Selected product: ${result.rows[0].name}`);
  });

  test('UPDATE', async () => {
    await client.query(
      'UPDATE test_products SET price = $1 WHERE name = $2',
      [899.99, 'Laptop']
    );
    const result = await client.query(
      'SELECT price FROM test_products WHERE name = $1',
      ['Laptop']
    );
    expect(parseFloat(result.rows[0].price)).toBeCloseTo(899.99, 2);
    console.log('✅ Updated product price');
  });

  test('Transaction', async () => {
    await client.query('BEGIN');
    await client.query(
      'UPDATE test_products SET price = $1 WHERE name = $2',
      [799.99, 'Laptop']
    );
    await client.query('COMMIT');
    
    const result = await client.query(
      'SELECT price FROM test_products WHERE name = $1',
      ['Laptop']
    );
    expect(parseFloat(result.rows[0].price)).toBeCloseTo(799.99, 2);
    console.log('✅ Transaction committed');
  });

  test('DELETE', async () => {
    await client.query('DELETE FROM test_products WHERE name = $1', ['Laptop']);
    const result = await client.query('SELECT COUNT(*) FROM test_products');
    expect(parseInt(result.rows[0].count)).toBe(0);
    console.log('✅ Deleted product');
  });
});
