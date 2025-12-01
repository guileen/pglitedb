package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPebbleBatchOrdering tests that batch operations work correctly regardless of key order
func TestPebbleBatchOrdering(t *testing.T) {
	// Setup test database
	kv, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	batch := kv.NewBatch()

	// Add keys in reverse order to test sorting
	keys := [][]byte{
		[]byte("z_key"),
		[]byte("y_key"),
		[]byte("x_key"),
		[]byte("a_key"),
		[]byte("b_key"),
		[]byte("c_key"),
	}

	// Set keys in reverse order
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		value := []byte("value_" + string(key))
		err := batch.Set(key, value)
		assert.NoError(t, err)
	}

	// Commit the batch - this should not produce the "strictly increasing order" error
	err := kv.CommitBatch(ctx, batch)
	assert.NoError(t, err)

	// Verify all keys can be retrieved
	for _, key := range keys {
		value, err := kv.Get(ctx, key)
		assert.NoError(t, err)
		expectedValue := []byte("value_" + string(key))
		assert.Equal(t, expectedValue, value)
	}

	// Test with mixed set/delete operations
	batch2 := kv.NewBatch()
	
	// Delete some keys
	err = batch2.Delete([]byte("a_key"))
	assert.NoError(t, err)
	
	// Add them back out of order
	err = batch2.Set([]byte("a_key"), []byte("new_value_a"))
	assert.NoError(t, err)
	
	err = batch2.Set([]byte("0_key"), []byte("value_0"))
	assert.NoError(t, err)
	
	// Commit the batch
	err = kv.CommitBatch(ctx, batch2)
	assert.NoError(t, err)
	
	// Verify the keys
	value, err := kv.Get(ctx, []byte("a_key"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new_value_a"), value)
	
	value, err = kv.Get(ctx, []byte("0_key"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value_0"), value)
}

// setupTestDB creates a test PebbleKV instance
func setupTestDB(t *testing.T) (*PebbleKV, func()) {
	config := TestOptimizedPebbleConfig(t.TempDir())
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatal(err)
	}
	
	cleanup := func() {
		kv.Close()
	}
	
	return kv, cleanup
}