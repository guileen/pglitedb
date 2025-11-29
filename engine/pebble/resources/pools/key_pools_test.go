package pools

import (
	"testing"

	"github.com/guileen/pglitedb/engine/pebble/resources/leak"
	"github.com/stretchr/testify/assert"
)

func TestKeyBufferPool_SizeTiers(t *testing.T) {
	leakDetector := leak.NewDetector()
	poolManager := NewManager(leakDetector, nil)
	
	// Test different buffer sizes
	sizes := []int{64, 128, 256, 512, 1024, 2048}
	
	for _, size := range sizes {
		buf := poolManager.AcquireIndexKeyBuffer(size)
		assert.NotNil(t, buf)
		assert.GreaterOrEqual(t, cap(buf), size)
		
		// Fill buffer with some data
		for i := 0; i < size && i < cap(buf); i++ {
			buf = append(buf, byte(i%256))
		}
		
		// Release buffer back to pool
		poolManager.ReleaseIndexKeyBuffer(buf)
	}
}

func TestKeyBufferPool_MemoryEfficiency(t *testing.T) {
	leakDetector := leak.NewDetector()
	poolManager := NewManager(leakDetector, nil)
	
	const bufferSize = 256
	const numBuffers = 1000
	
	// Allocate many buffers
	buffers := make([][]byte, numBuffers)
	for i := 0; i < numBuffers; i++ {
		buffers[i] = poolManager.AcquireIndexKeyBuffer(bufferSize)
		assert.NotNil(t, buffers[i])
	}
	
	// Release all buffers
	for i := 0; i < numBuffers; i++ {
		poolManager.ReleaseIndexKeyBuffer(buffers[i])
	}
	
	// Acquire again - should reuse previously allocated buffers
	for i := 0; i < numBuffers; i++ {
		buf := poolManager.AcquireIndexKeyBuffer(bufferSize)
		assert.NotNil(t, buf)
	}
}