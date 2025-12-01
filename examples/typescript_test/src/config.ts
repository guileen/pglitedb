// Database configuration
export const dbConfig = {
  host: 'localhost',
  port: 5433,
  database: 'testdb',
  user: 'admin',
  password: 'password',
  // Add connection timeout configurations
  connectionTimeoutMillis: 5000, // 5 seconds connection timeout
  idleTimeoutMillis: 30000,      // 30 seconds idle timeout
  query_timeout: 10000,          // 10 seconds default query timeout
};

// Timeout configurations for different query types
export const queryTimeouts = {
  simple: 5000,      // 5 seconds for simple queries
  complex: 15000,    // 15 seconds for complex queries
  ddl: 60000,        // 60 seconds for DDL operations
  admin: 300000,     // 5 minutes for administrative operations
  transaction: 30000 // 30 seconds for transaction operations
};