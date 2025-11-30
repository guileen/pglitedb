package storage

import (
	"os"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage/internal/kv"
	"github.com/guileen/pglitedb/storage/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMVCCStorage_CoreOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	
	// Setup test KV store with temporary directory
	tmpDir, err := os.MkdirTemp("", "mvcc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := kv.TestOptimizedPebbleConfig(tmpDir)
	kvStore, err := kv.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

	mvcc := NewMVCCStorage(kvStore)

	key := []byte("test-key")
	value := []byte("test-value")
	startTS := time.Now().UnixNano()

	t.Run("PutAndGet", func(t *testing.T) {
		// Test Put operation
		err := mvcc.Put(key, value, startTS)
		assert.NoError(t, err)

		// Test Get operation
		retrieved, err := mvcc.Get(key, startTS+1)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	t.Run("Delete", func(t *testing.T) {
		deleteTS := startTS + 10
		err := mvcc.Delete(key, deleteTS)
		assert.NoError(t, err)

		// Should not be visible after delete timestamp
		_, err = mvcc.Get(key, deleteTS+1)
		assert.Error(t, err)
		assert.True(t, shared.IsNotFound(err))

		// Should still be visible before delete timestamp
		retrieved, err := mvcc.Get(key, startTS+1)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})
}

func TestMVCCStorage_CommitAndAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	
	// Setup test KV store with temporary directory
	tmpDir, err := os.MkdirTemp("", "mvcc-commit-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := kv.TestOptimizedPebbleConfig(tmpDir)
	kvStore, err := kv.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

	mvcc := NewMVCCStorage(kvStore)

	key := []byte("commit-test-key")
	value := []byte("commit-test-value")
	startTS := time.Now().UnixNano()

	t.Run("Commit", func(t *testing.T) {
		// Put a value
		err := mvcc.Put(key, value, startTS)
		assert.NoError(t, err)

		// Commit the transaction
		commitTS := startTS + 5
		err = mvcc.Commit(key, startTS, commitTS)
		assert.NoError(t, err)

		// Should be visible after commit
		retrieved, err := mvcc.Get(key, commitTS+1)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)

		// Should not be visible before commit
		_, err = mvcc.Get(key, startTS+1)
		assert.Error(t, err)
		assert.True(t, shared.IsNotFound(err))
	})

	t.Run("Abort", func(t *testing.T) {
		abortKey := []byte("abort-test-key")
		abortTS := startTS + 100

		// Put a value
		err := mvcc.Put(abortKey, []byte("abort-value"), abortTS)
		assert.NoError(t, err)

		// Abort the transaction
		err = mvcc.Abort(abortKey, abortTS)
		assert.NoError(t, err)

		// Should not be visible even right after start timestamp
		_, err = mvcc.Get(abortKey, abortTS+1)
		assert.Error(t, err)
		assert.True(t, shared.IsNotFound(err))
	})
}

func TestMVCCStorage_VersionVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	
	// Setup test KV store with temporary directory
	tmpDir, err := os.MkdirTemp("", "mvcc-visibility-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := kv.TestOptimizedPebbleConfig(tmpDir)
	kvStore, err := kv.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

	mvcc := NewMVCCStorage(kvStore)

	key := []byte("visibility-test-key")
	baseTS := time.Now().UnixNano()

	// Create multiple versions
	versions := []struct {
		ts    int64
		value []byte
	}{
		{baseTS, []byte("version-1")},
		{baseTS + 10, []byte("version-2")},
		{baseTS + 20, []byte("version-3")},
	}

	// Put all versions
	for _, v := range versions {
		err := mvcc.Put(key, v.value, v.ts)
		require.NoError(t, err)
		
		// Commit each version
		err = mvcc.Commit(key, v.ts, v.ts+5)
		require.NoError(t, err)
	}

	t.Run("ReadAtDifferentTimestamps", func(t *testing.T) {
		tests := []struct {
			readTS   int64
			expected []byte
		}{
			{baseTS + 1, []byte("version-1")},     // Before any commit
			{baseTS + 7, []byte("version-1")},     // After first commit
			{baseTS + 17, []byte("version-2")},    // After second commit
			{baseTS + 27, []byte("version-3")},    // After third commit
		}

		for _, tt := range tests {
			retrieved, err := mvcc.Get(key, tt.readTS)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, retrieved)
		}
	})
}

func TestMVCCStorage_GetVisibleVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	
	// Setup test KV store with temporary directory
	tmpDir, err := os.MkdirTemp("", "mvcc-visible-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := kv.TestOptimizedPebbleConfig(tmpDir)
	kvStore, err := kv.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

	mvcc := NewMVCCStorage(kvStore)

	key := []byte("visible-versions-test-key")
	baseTS := time.Now().UnixNano()

	// Create multiple versions
	versions := []struct {
		ts    int64
		value []byte
	}{
		{baseTS, []byte("version-1")},
		{baseTS + 10, []byte("version-2")},
		{baseTS + 20, []byte("version-3")},
	}

	// Put all versions
	for _, v := range versions {
		err := mvcc.Put(key, v.value, v.ts)
		require.NoError(t, err)
		
		// Commit each version
		err = mvcc.Commit(key, v.ts, v.ts+5)
		require.NoError(t, err)
	}

	t.Run("GetVisibleVersions", func(t *testing.T) {
		visibleVersions, err := mvcc.GetVisibleVersions(key, baseTS+25)
		assert.NoError(t, err)
		assert.Len(t, visibleVersions, 3)
		
		// Check that all versions are present
		values := make(map[string]bool)
		for _, v := range visibleVersions {
			values[string(v.Value)] = true
		}
		
		assert.True(t, values["version-1"])
		assert.True(t, values["version-2"])
		assert.True(t, values["version-3"])
	})
}

func TestMVCCStorage_EdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	
	// Setup test KV store with temporary directory
	tmpDir, err := os.MkdirTemp("", "mvcc-edge-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := kv.TestOptimizedPebbleConfig(tmpDir)
	kvStore, err := kv.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

	mvcc := NewMVCCStorage(kvStore)

	t.Run("GetNonExistentKey", func(t *testing.T) {
		_, err := mvcc.Get([]byte("non-existent"), time.Now().UnixNano())
		assert.Error(t, err)
		assert.True(t, shared.IsNotFound(err))
	})

	t.Run("InvalidEncodedKey", func(t *testing.T) {
		_, _, err := mvcc.decodeKey([]byte("short"))
		assert.Error(t, err)
	})

	t.Run("InvalidEncodedValue", func(t *testing.T) {
		version := &MVCCVersion{}
		err := mvcc.decodeValue([]byte("too-short"), version)
		assert.Error(t, err)
	})
}