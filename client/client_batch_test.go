package client

import (
	"context"
	"testing"
)

// TestBatchInsertMethodSignature verifies that the BatchInsert method has the correct signature
func TestBatchInsertMethodSignature(t *testing.T) {
	// This test just verifies the method signature compiles correctly
	// We're not actually calling the method since it requires a real database connection
	
	// Create a dummy function that uses the BatchInsert method signature
	// This will cause a compile error if the signature is wrong
	_ = func(client *Client) {
		_, _ = client.BatchInsert(context.Background(), 1, "test_table", []map[string]interface{}{})
	}
	
	t.Log("BatchInsert method signature is correct")
}