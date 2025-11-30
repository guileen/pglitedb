package pools

import (
	"fmt"
)

// Example of using pooled buffers in batch operations
func ExamplePooledBatchOperations() {
	bufferPool := NewBufferPool()
	
	// Acquire buffers from pool
	keyBuf := bufferPool.Acquire(128)
	valueBuf := bufferPool.Acquire(512)
	rowIDBuf := bufferPool.Acquire(64)
	
	// Use buffers for batch operations
	// ... (actual batch operation logic would go here)
	
	fmt.Printf("Acquired buffers - key: %d, value: %d, rowID: %d\n", 
		cap(keyBuf), cap(valueBuf), cap(rowIDBuf))
	
	// Release buffers back to pool
	bufferPool.Release(keyBuf)
	bufferPool.Release(valueBuf)
	bufferPool.Release(rowIDBuf)
	
	fmt.Println("Buffers released back to pool")
}