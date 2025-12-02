package buffer

import (
	"testing"

	"github.com/guileen/pglitedb/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferPoolManager_NewBufferPoolManager(t *testing.T) {
	manager := NewBufferPoolManager()
	assert.NotNil(t, manager)
}

func TestBufferPoolManager_InitializeBufferPool(t *testing.T) {
	manager := NewBufferPoolManager()
	
	// Test initializing buffer pool
	sizes := []int{64, 128, 256, 512, 1024}
	err := manager.InitializeBufferPool("test_pool", sizes)
	assert.NoError(t, err)
	
	// Verify pool was set
	bufferPool := manager.GetBufferPool()
	assert.NotNil(t, bufferPool)
}

func TestBufferPoolManager_GetPutBuffer(t *testing.T) {
	manager := NewBufferPoolManager()
	
	// Test with uninitialized pool (should create buffers directly)
	buf := manager.GetBuffer(100)
	assert.NotNil(t, buf)
	// Note: Pool might return a larger buffer than requested
	assert.GreaterOrEqual(t, len(buf), 100)
	
	// Return buffer (should not panic even with uninitialized pool)
	manager.PutBuffer(buf)
	
	// Test with initialized pool
	sizes := []int{64, 128, 256}
	err := manager.InitializeBufferPool("test_pool", sizes)
	require.NoError(t, err)
	
	// Get buffer from pool (request size 100, should get 128 from pool)
	buf1 := manager.GetBuffer(100)
	assert.NotNil(t, buf1)
	// Pool returns the smallest buffer that fits the request
	assert.GreaterOrEqual(t, len(buf1), 100)
	
	// Return buffer to pool
	manager.PutBuffer(buf1)
	
	// Get another buffer (might be the same one from pool)
	buf2 := manager.GetBuffer(100)
	assert.NotNil(t, buf2)
	assert.GreaterOrEqual(t, len(buf2), 100)
	manager.PutBuffer(buf2)
}

func TestBufferPoolManager_GetBufferPool(t *testing.T) {
	manager := NewBufferPoolManager()
	
	// Test with uninitialized pool
	bufferPool := manager.GetBufferPool()
	assert.Nil(t, bufferPool)
	
	// Test with initialized pool
	sizes := []int{64, 128}
	err := manager.InitializeBufferPool("test_pool", sizes)
	require.NoError(t, err)
	
	bufferPool = manager.GetBufferPool()
	assert.NotNil(t, bufferPool)
}

func TestBufferPoolManager_SetBufferPool(t *testing.T) {
	manager := NewBufferPoolManager()
	
	// Create a new buffer pool
	sizes := []int{32, 64}
	newPool := pool.NewMultiBufferPool("new_pool", sizes)
	
	// Set the buffer pool
	manager.SetBufferPool(newPool)
	
	// Verify it was set
	retrievedPool := manager.GetBufferPool()
	assert.Equal(t, newPool, retrievedPool)
}

func TestBufferPoolManager_GetPoolMetrics(t *testing.T) {
	manager := NewBufferPoolManager()
	
	// Test with uninitialized pool
	metrics := manager.GetPoolMetrics()
	// Should return empty metrics
	assert.Equal(t, pool.PoolMetrics{}, metrics)
	
	// Test with initialized pool
	sizes := []int{64, 128}
	err := manager.InitializeBufferPool("test_pool", sizes)
	require.NoError(t, err)
	
	metrics = manager.GetPoolMetrics()
	// Should return valid metrics (though initially empty)
	assert.NotNil(t, metrics)
}

func TestBufferPoolManager_HealthCheck(t *testing.T) {
	manager := NewBufferPoolManager()
	
	// Health check should not error
	err := manager.HealthCheck()
	assert.NoError(t, err)
	
	// Even after initialization
	sizes := []int{64, 128}
	err = manager.InitializeBufferPool("test_pool", sizes)
	require.NoError(t, err)
	
	err = manager.HealthCheck()
	assert.NoError(t, err)
}

func TestBufferPoolManager_ConcurrentAccess(t *testing.T) {
	manager := NewBufferPoolManager()
	sizes := []int{64, 128, 256}
	err := manager.InitializeBufferPool("test_pool", sizes)
	require.NoError(t, err)
	
	// Test concurrent access to buffer pool
	done := make(chan bool)
	
	// Goroutine 1: Get buffers
	go func() {
		for i := 0; i < 100; i++ {
			buf := manager.GetBuffer(100)
			manager.PutBuffer(buf)
		}
		done <- true
	}()
	
	// Goroutine 2: Get buffers
	go func() {
		for i := 0; i < 100; i++ {
			buf := manager.GetBuffer(200)
			manager.PutBuffer(buf)
		}
		done <- true
	}()
	
	// Wait for both goroutines
	<-done
	<-done
	
	// Verify no panics occurred and pool is still functional
	buf := manager.GetBuffer(50)
	assert.NotNil(t, buf)
	manager.PutBuffer(buf)
}