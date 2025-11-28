// Package context provides context pooling functionality for PGLiteDB
package context

import (
	"testing"
	"time"
)

// TestRequestContextPool tests the RequestContextPool implementation
func TestRequestContextPool(t *testing.T) {
	// Initialize the pool
	Initialize()

	// Test Get
	ctx1 := GetRequestContext()
	if ctx1 == nil {
		t.Error("Expected non-nil RequestContext from pool")
	}

	// Test Put
	PutRequestContext(ctx1)

	// Test Get again (should get the same object)
	ctx2 := GetRequestContext()
	if ctx2 == nil {
		t.Error("Expected non-nil RequestContext from pool")
	}

	// Verify the context was reset
	if ctx2.RequestID != "" || ctx2.UserID != "" || ctx2.Timestamp != (time.Time{}) {
		t.Error("Expected RequestContext to be reset")
	}

	// Verify maps are empty
	if len(ctx2.Metadata) != 0 || len(ctx2.Headers) != 0 {
		t.Error("Expected RequestContext maps to be empty after reset")
	}
}

// TestTransactionContextPool tests the TransactionContextPool implementation
func TestTransactionContextPool(t *testing.T) {
	// Initialize the pool
	Initialize()

	// Test Get
	ctx1 := GetTransactionContext()
	if ctx1 == nil {
		t.Error("Expected non-nil TransactionContext from pool")
	}

	// Test Put
	PutTransactionContext(ctx1)

	// Test Get again (should get the same object)
	ctx2 := GetTransactionContext()
	if ctx2 == nil {
		t.Error("Expected non-nil TransactionContext from pool")
	}

	// Verify the context was reset
	if ctx2.TxID != "" || ctx2.Isolation != "" || ctx2.StartTime != (time.Time{}) {
		t.Error("Expected TransactionContext to be reset")
	}

	if ctx2.ReadOnly != false || ctx2.AutoCommit != false {
		t.Error("Expected TransactionContext boolean fields to be reset")
	}
}

// TestQueryContextPool tests the QueryContextPool implementation
func TestQueryContextPool(t *testing.T) {
	// Initialize the pool
	Initialize()

	// Test Get
	ctx1 := GetQueryContext()
	if ctx1 == nil {
		t.Error("Expected non-nil QueryContext from pool")
	}

	// Test Put
	PutQueryContext(ctx1)

	// Test Get again (should get the same object)
	ctx2 := GetQueryContext()
	if ctx2 == nil {
		t.Error("Expected non-nil QueryContext from pool")
	}

	// Verify the context was reset
	if ctx2.QueryID != "" || ctx2.SQL != "" || ctx2.StartTime != (time.Time{}) {
		t.Error("Expected QueryContext to be reset")
	}

	// Verify parameters slice is empty
	if len(ctx2.Parameters) != 0 {
		t.Error("Expected QueryContext parameters to be empty after reset")
	}
}

// TestConcurrentAccess tests concurrent access to context pools
func TestConcurrentAccess(t *testing.T) {
	// Initialize the pool
	Initialize()

	// Test concurrent access to RequestContextPool
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			ctx := GetRequestContext()
			// Simulate some work
			ctx.RequestID = "test"
			ctx.UserID = "user"
			PutRequestContext(ctx)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// BenchmarkRequestContextPool benchmarks the RequestContextPool performance
func BenchmarkRequestContextPool(b *testing.B) {
	// Initialize the pool
	Initialize()

	b.Run("GetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx := GetRequestContext()
			PutRequestContext(ctx)
		}
	})

	b.Run("GetResetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx := GetRequestContext()
			ctx.RequestID = "test"
			ctx.UserID = "user"
			PutRequestContext(ctx)
		}
	})
}

// BenchmarkTransactionContextPool benchmarks the TransactionContextPool performance
func BenchmarkTransactionContextPool(b *testing.B) {
	// Initialize the pool
	Initialize()

	b.Run("GetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx := GetTransactionContext()
			PutTransactionContext(ctx)
		}
	})

	b.Run("GetResetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx := GetTransactionContext()
			ctx.TxID = "test"
			ctx.Isolation = "serializable"
			PutTransactionContext(ctx)
		}
	})
}

// BenchmarkQueryContextPool benchmarks the QueryContextPool performance
func BenchmarkQueryContextPool(b *testing.B) {
	// Initialize the pool
	Initialize()

	b.Run("GetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx := GetQueryContext()
			PutQueryContext(ctx)
		}
	})

	b.Run("GetResetPut", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx := GetQueryContext()
			ctx.QueryID = "test"
			ctx.SQL = "SELECT * FROM users"
			ctx.Parameters = append(ctx.Parameters, "param1", "param2")
			PutQueryContext(ctx)
		}
	})
}