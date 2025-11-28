package main

import (
	"fmt"
	"os"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Test default configuration
	fmt.Println("Testing memory pool configuration...")
	
	config := types.DefaultMemoryPoolConfig()
	fmt.Printf("Default config: %+v\n", config)
	
	// Test loading from environment variables
	os.Setenv("MEMORY_POOL_RECORD_CAP", "32")
	os.Setenv("MEMORY_POOL_RESULT_SET_CAP", "32")
	os.Setenv("MEMORY_POOL_ENABLE_ADAPTIVE", "true")
	os.Setenv("MEMORY_POOL_RESIZE_THRESHOLD", "0.75")
	
	config = types.LoadMemoryPoolConfig()
	fmt.Printf("Loaded config: %+v\n", config)
	
	// Test reinitializing memory pool with new config
	types.ReinitializeMemoryPool(config)
	fmt.Println("Reinitialized memory pool with new configuration")
	
	// Test acquiring objects with new configuration
	record := types.AcquireRecord()
	fmt.Printf("Acquired record with new config: Data length=%d\n", len(record.Data))
	types.ReleaseRecord(record)
	
	fmt.Println("Configuration test completed successfully!")
}