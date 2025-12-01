package debugtools

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

// TestOptimizedPerformance validates the performance improvements
func TestOptimizedPerformance(t *testing.T) {
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
	highPerfOps := runOptimizedPerformanceTest(t, highPerfDB, "high_perf")

	t.Logf("High-performance config: %d ops in %v (%.2f ops/sec)",
		highPerfOps, highPerfSetup, float64(highPerfOps)/highPerfSetup.Seconds())
}

// runOptimizedPerformanceTest runs a more comprehensive performance test
func runOptimizedPerformanceTest(t *testing.T, db *client.Client, testName string) int {
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

	// Test direct insert operations for better performance
	t.Run("DirectInsertOperations", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			recordID := i + 1000
			record := map[string]interface{}{
				"id":     recordID,
				"name":   fmt.Sprintf("User%d", recordID),
				"email":  fmt.Sprintf("user%d@example.com", recordID),
				"age":    20 + (recordID % 50),
				"score":  float64(recordID % 100),
				"active": recordID%2 == 0,
			}

			_, err := db.DirectInsert(ctx, tenantID, tableName, record)
			if err != nil {
				t.Fatalf("Direct insert failed: %v", err)
			}
			ops++
		}
	})

	// Test batch insert operations
	t.Run("BatchInsertOperations", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			records := make([]map[string]interface{}, 10)
			for j := 0; j < 10; j++ {
				recordID := i*10 + j + 2000
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
			ops += 10
		}
	})

	// Test direct batch insert operations
	t.Run("DirectBatchInsertOperations", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			records := make([]map[string]interface{}, 5)
			for j := 0; j < 5; j++ {
				recordID := i*5 + j + 3000
				records[j] = map[string]interface{}{
					"id":     recordID,
					"name":   fmt.Sprintf("User%d", recordID),
					"email":  fmt.Sprintf("user%d@example.com", recordID),
					"age":    20 + (recordID % 50),
					"score":  float64(recordID % 100),
					"active": recordID%2 == 0,
				}
			}

			_, err := db.DirectBatchInsert(ctx, tenantID, tableName, records)
			if err != nil {
				t.Fatalf("Direct batch insert failed: %v", err)
			}
			ops += 5
		}
	})

	// Select operations
	t.Run("SelectOperations", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			options := &types.QueryOptions{
				Limit: intPtr(10),
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
	})

	// Update operations
	t.Run("UpdateOperations", func(t *testing.T) {
		for i := 0; i < 25; i++ {
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
	})

	duration := time.Since(start)
	tps := float64(ops) / duration.Seconds()
	latency := duration.Seconds() / float64(ops) * 1000 // in milliseconds

	t.Logf("%s: %d operations in %v (%.2f ops/sec, %.3f ms/op)",
		testName, ops, duration, tps, latency)

	// Check if we're meeting performance targets
	if tps < 3245 {
		t.Logf("WARNING: TPS (%.2f) is below target (3245+)", tps)
	} else {
		t.Logf("SUCCESS: TPS (%.2f) meets target", tps)
	}

	if latency > 3.2 {
		t.Logf("WARNING: Latency (%.3f ms) is above target (3.2 ms)", latency)
	} else {
		t.Logf("SUCCESS: Latency (%.3f ms) meets target", latency)
	}

	return ops
}

// intPtr returns a pointer to an int
func intPtr(i int) *int {
	return &i
}