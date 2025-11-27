package pgserver

import (
	"testing"
)

func BenchmarkParameterBindingWithAST(b *testing.B) {
	query := "SELECT * FROM users WHERE id = $1 AND name = $2 AND age > $3 AND city = $4 AND active = $5"
	params := []interface{}{123, "Alice", 25, "Beijing", true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := BindParametersInQuery(query, params)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// This benchmark tests the old regex-based approach for comparison
func BenchmarkRegexParameterBinding(b *testing.B) {
	// This would be the old implementation
	// For comparison purposes only
	query := "SELECT * FROM users WHERE id = $1 AND name = $2 AND age > $3 AND city = $4 AND active = $5"
	params := []interface{}{123, "Alice", 25, "Beijing", true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate the old regex-based approach
		result := query
		for j := len(params) - 1; j >= 0; j-- {
			placeholder := "$" + string(rune('1'+j))
			// This is a simplified version - actual implementation would be more complex
			_ = placeholder
		}
		_ = result
	}
}

func BenchmarkComplexQueryParameterBinding(b *testing.B) {
	// More complex query with nested conditions
	query := `SELECT u.name, u.email, p.title 
	          FROM users u 
	          JOIN posts p ON u.id = p.user_id 
	          WHERE u.age > $1 
	          AND u.city IN ($2, $3, $4) 
	          AND p.created_at BETWEEN $5 AND $6 
	          AND u.active = $7 
	          ORDER BY p.created_at DESC 
	          LIMIT $8`
	params := []interface{}{18, "Beijing", "Shanghai", "Guangzhou", "2023-01-01", "2023-12-31", true, 10}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := BindParametersInQuery(query, params)
		if err != nil {
			b.Fatal(err)
		}
	}
}