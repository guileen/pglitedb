package main

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/client"
	"github.com/stretchr/testify/assert"
)

func TestClientCreation(t *testing.T) {
	// Test that we can create a client without errors
	db := client.NewClient("/tmp/test-compatibility-db")
	
	// Verify the client is not nil
	assert.NotNil(t, db)
	
	// Test that we can call methods on the client
	ctx := context.Background()
	result, err := db.Insert(ctx, 1, "test_table", map[string]interface{}{
		"name": "Test User",
	})
	
	// We're just testing that the API works, not the actual implementation
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.Count)
}