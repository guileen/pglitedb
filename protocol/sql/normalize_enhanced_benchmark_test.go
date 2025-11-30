package sql

import (
	"testing"
)

func BenchmarkNormalizeQueryEnhanced(b *testing.B) {
	// Complex query with various SQL constructs to test normalization
	query := "SELECT u.id, u.name, u.email, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id WHERE u.age > 25 AND u.status = 'active' AND p.created_at BETWEEN '2023-01-01' AND '2023-12-31' ORDER BY p.created_at DESC LIMIT 100 OFFSET 10"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NormalizeQuery(query)
	}
}

func BenchmarkNormalizeQueryEnhancedVsOriginal(b *testing.B) {
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
		"SELECT u.id, u.name FROM users u INNER JOIN posts p ON u.id = p.user_id WHERE u.age > 25 AND u.status = 'active'", // complex query
		"select u.id, u.name from users u inner join posts p on u.id = p.user_id where u.age > 25 and u.status = 'active'", // lowercase complex
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_ = NormalizeQuery(query)
	}
}