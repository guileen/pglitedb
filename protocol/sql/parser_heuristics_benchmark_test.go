package sql

import (
	"testing"
)

func BenchmarkParserHeuristics_SimpleQueries(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Test queries that should be handled by the simple parser
	simpleQueries := []string{
		"SELECT id, name FROM users WHERE age > 25",
		"INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"SELECT * FROM products WHERE price < 100 LIMIT 10",
		"INSERT INTO orders (user_id, product_id) VALUES (1, 2)",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := simpleQueries[i%len(simpleQueries)]
		result := parser.shouldUseSimpleParser(query)
		if !result {
			b.Fatalf("Expected simple parser for query: %s", query)
		}
	}
}

func BenchmarkParserHeuristics_ComplexQueries(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Test queries that should fall back to the full parser
	complexQueries := []string{
		"SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
		"SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)",
		"WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100) SELECT sum(n) FROM t",
		"SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
		"SELECT name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := complexQueries[i%len(complexQueries)]
		result := parser.shouldUseSimpleParser(query)
		if result {
			b.Fatalf("Expected full parser for query: %s", query)
		}
	}
}

func BenchmarkParserHeuristics_ParameterizedQueries(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Test parameterized queries which should be handled efficiently
	paramQueries := []string{
		"SELECT id, name FROM users WHERE age > $1 AND active = $2",
		"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
		"UPDATE users SET name = $1, email = $2 WHERE id = $3",
		"DELETE FROM users WHERE id = $1 AND created_at < $2",
		"SELECT * FROM products WHERE category = $1 AND price BETWEEN $2 AND $3",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := paramQueries[i%len(paramQueries)]
		result := parser.shouldUseSimpleParser(query)
		if !result {
			b.Fatalf("Expected simple parser for parameterized query: %s", query)
		}
	}
}

func BenchmarkParserHeuristics_LikelySimpleCheck(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Mix of simple and complex queries
	testQueries := []string{
		"SELECT id FROM users",
		"SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
		"INSERT INTO test (a) VALUES (1)",
		"SELECT department, COUNT(*) FROM employees GROUP BY department",
		"UPDATE test SET a = 1 WHERE b = 2",
		"WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100) SELECT sum(n) FROM t",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := testQueries[i%len(testQueries)]
		result := parser.isLikelySimpleQuery(query)
		// We don't assert the result as the heuristic may evolve
		_ = result
	}
}