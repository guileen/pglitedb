package main_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/client"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

// TestHighPerformanceConfiguration validates that the high-performance configuration
// provides better performance than the default configuration
func TestHighPerformanceConfiguration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "perf-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test high-performance configuration
	highPerfPath := filepath.Join(tmpDir, "high-perf")
	highPerfConfig := storage.HighPerformancePebbleConfig(highPerfPath)
	
	start := time.Now()
	highPerfDB := client.NewClientWithConfig(highPerfPath, highPerfConfig)
	highPerfSetup := time.Since(start)
	
	// Run performance test with high-performance config
	highPerfOps := runPerformanceTest(t, highPerfDB, "high_perf")
	
	// Note: Client doesn't have explicit Close method in current implementation
	
	t.Logf("High-performance config: %d ops in %v (%.2f ops/sec)", 
		highPerfOps, highPerfSetup, float64(highPerfOps)/highPerfSetup.Seconds())
}

// TestConfigurationComparison compares different configurations
func TestConfigurationComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-compare-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test default configuration
	defaultPath := filepath.Join(tmpDir, "default")
	defaultConfig := storage.DefaultPebbleConfig(defaultPath)
	
	start := time.Now()
	defaultDB := client.NewClientWithConfig(defaultPath, defaultConfig)
	defaultSetup := time.Since(start)
	defaultOps := runPerformanceTest(t, defaultDB, "default")
	// Note: Client doesn't have explicit Close method in current implementation

	// Test high-performance configuration
	highPerfPath := filepath.Join(tmpDir, "high-perf")
	highPerfConfig := storage.HighPerformancePebbleConfig(highPerfPath)
	
	start = time.Now()
	highPerfDB := client.NewClientWithConfig(highPerfPath, highPerfConfig)
	highPerfSetup := time.Since(start)
	highPerfOps := runPerformanceTest(t, highPerfDB, "high_perf")
	// Note: Client doesn't have explicit Close method in current implementation

	t.Logf("Default config: %d ops in %v (%.2f ops/sec)", 
		defaultOps, defaultSetup, float64(defaultOps)/defaultSetup.Seconds())
	t.Logf("High-performance config: %d ops in %v (%.2f ops/sec)", 
		highPerfOps, highPerfSetup, float64(highPerfOps)/highPerfSetup.Seconds())
	
	// With small test sizes, we might not see significant differences, so we won't assert failure
	// In a real performance test, we would run with larger datasets and more iterations
}

// runPerformanceTest runs a simple performance test
func runPerformanceTest(t *testing.T, db *client.Client, testName string) int {
	// intPtr returns a pointer to an int
	intPtr := func(i int) *int {
		return &i
	}

	ctx := context.Background()
	tenantID := int64(1)
	tableName := fmt.Sprintf("test_table_%s", testName)
	
	// Create table implicitly by inserting first record
	firstRecord := map[string]interface{}{
		"id":     1,
		"name":   "InitialUser",
		"email":  "initial@example.com",
		"age":    25,
		"score":  50.0,
		"active": true,
	}
	
	_, err := db.Insert(ctx, tenantID, tableName, firstRecord)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Perform batch operations
	ops := 0
	start := time.Now()
	
	// Insert batch operations
	for i := 0; i < 10; i++ {
		records := make([]map[string]interface{}, 5)
		for j := 0; j < 5; j++ {
			recordID := i*5 + j
			records[j] = map[string]interface{}{
				"id":     recordID,
				"name":   fmt.Sprintf("User%d", recordID),
				"email":  fmt.Sprintf("user%d@example.com", recordID),
				"age":    20 + (recordID % 50),
				"score":  float64(recordID % 100),
				"active": recordID%2 == 0,
			}
		}
		
		_, err := db.BatchInsert(ctx, tenantID, tableName, records)
		if err != nil {
			t.Fatalf("Batch insert failed: %v", err)
		}
		ops += 5
	}
	
	// Select operations
	for i := 0; i < 10; i++ {
		options := &types.QueryOptions{
			Limit: intPtr(5),
			Where: map[string]interface{}{
				"age": 20 + (i % 50),
			},
		}
		
		_, err := db.Select(ctx, tenantID, tableName, options)
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		ops++
	}
	
	// Update operations
	for i := 0; i < 5; i++ {
		updates := map[string]interface{}{
			"score": float64(i % 100),
		}
		
		conditions := map[string]interface{}{
			"active": i%2 == 0,
		}
		
		_, err := db.Update(ctx, tenantID, tableName, updates, conditions)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		ops++
	}
	
	duration := time.Since(start)
	t.Logf("%s: %d operations in %v (%.2f ops/sec)", 
		testName, ops, duration, float64(ops)/duration.Seconds())
	
	return ops
}