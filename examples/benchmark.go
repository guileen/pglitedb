package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/guileen/pglitedb/client"
	"github.com/guileen/pglitedb/types"
)

type BenchmarkResult struct {
	Operation    string
	Duration     time.Duration
	RecordsCount int
	OpsPerSecond float64
}

func (r BenchmarkResult) String() string {
	return fmt.Sprintf("%s: %d records in %v (%.2f ops/sec)",
		r.Operation, r.RecordsCount, r.Duration, r.OpsPerSecond)
}

func main() {
	dbPath := fmt.Sprintf("/tmp/pglitedb-benchmark-%d", time.Now().UnixNano())
	db := client.NewClient(dbPath)
	ctx := context.Background()
	tenantID := int64(1)
	tableName := "benchmark_table"

	log.Println("Starting performance benchmarks...")

	benchmarkInsert(ctx, db, tenantID, tableName, 1000)
	benchmarkInsert(ctx, db, tenantID, tableName, 10000)
	benchmarkSelect(ctx, db, tenantID, tableName)
	benchmarkUpdate(ctx, db, tenantID, tableName, 1000)
	benchmarkDelete(ctx, db, tenantID, tableName, 1000)
	
	benchmarkConcurrentInsert(ctx, db, tenantID, tableName, 5000, 10)
}

func benchmarkInsert(ctx context.Context, db *client.Client, tenantID int64, tableName string, count int) {
	start := time.Now()

	for i := 0; i < count; i++ {
		data := map[string]interface{}{
			"id":    i,
			"name":  fmt.Sprintf("User%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"age":   rand.Intn(60) + 20,
			"score": rand.Float64() * 100,
		}
		_, err := db.Insert(ctx, tenantID, tableName, data)
		if err != nil {
			log.Printf("Insert error: %v", err)
			return
		}
	}

	duration := time.Since(start)
	result := BenchmarkResult{
		Operation:    fmt.Sprintf("Insert (%d records)", count),
		Duration:     duration,
		RecordsCount: count,
		OpsPerSecond: float64(count) / duration.Seconds(),
	}
	log.Println(result)
}

func benchmarkSelect(ctx context.Context, db *client.Client, tenantID int64, tableName string) {
	queries := []struct {
		name    string
		options *types.QueryOptions
	}{
		{
			name:    "Select All",
			options: &types.QueryOptions{},
		},
		{
			name: "Select with WHERE",
			options: &types.QueryOptions{
				Where: map[string]interface{}{"age": 30},
			},
		},
		{
			name: "Select with ORDER BY",
			options: &types.QueryOptions{
				OrderBy: []string{"age"},
			},
		},
		{
			name: "Select with LIMIT",
			options: &types.QueryOptions{
				Limit: intPtr(100),
			},
		},
	}

	for _, query := range queries {
		start := time.Now()
		result, err := db.Select(ctx, tenantID, tableName, query.options)
		duration := time.Since(start)

		if err != nil {
			log.Printf("%s error: %v", query.name, err)
			continue
		}

		benchResult := BenchmarkResult{
			Operation:    query.name,
			Duration:     duration,
			RecordsCount: int(result.Count),
			OpsPerSecond: float64(result.Count) / duration.Seconds(),
		}
		log.Println(benchResult)
	}
}

func benchmarkUpdate(ctx context.Context, db *client.Client, tenantID int64, tableName string, count int) {
	start := time.Now()

	for i := 0; i < count; i++ {
		data := map[string]interface{}{
			"age": rand.Intn(60) + 20,
		}
		_, err := db.Update(ctx, tenantID, tableName, data, map[string]interface{}{
			"id": i,
		})
		if err != nil {
			log.Printf("Update error: %v", err)
			return
		}
	}

	duration := time.Since(start)
	result := BenchmarkResult{
		Operation:    fmt.Sprintf("Update (%d records)", count),
		Duration:     duration,
		RecordsCount: count,
		OpsPerSecond: float64(count) / duration.Seconds(),
	}
	log.Println(result)
}

func benchmarkDelete(ctx context.Context, db *client.Client, tenantID int64, tableName string, count int) {
	start := time.Now()

	for i := 0; i < count; i++ {
		_, err := db.Delete(ctx, tenantID, tableName, map[string]interface{}{
			"id": i,
		})
		if err != nil {
			log.Printf("Delete error: %v", err)
			return
		}
	}

	duration := time.Since(start)
	result := BenchmarkResult{
		Operation:    fmt.Sprintf("Delete (%d records)", count),
		Duration:     duration,
		RecordsCount: count,
		OpsPerSecond: float64(count) / duration.Seconds(),
	}
	log.Println(result)
}

func benchmarkConcurrentInsert(ctx context.Context, db *client.Client, tenantID int64, tableName string, totalCount, goroutines int) {
	perGoroutine := totalCount / goroutines
	start := time.Now()

	done := make(chan bool, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(offset int) {
			for i := 0; i < perGoroutine; i++ {
				id := offset*perGoroutine + i
				data := map[string]interface{}{
					"id":    id,
					"name":  fmt.Sprintf("ConcurrentUser%d", id),
					"email": fmt.Sprintf("user%d@concurrent.com", id),
					"age":   rand.Intn(60) + 20,
					"score": rand.Float64() * 100,
				}
				_, err := db.Insert(ctx, tenantID, tableName, data)
				if err != nil {
					log.Printf("Concurrent insert error: %v", err)
				}
			}
			done <- true
		}(g)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	duration := time.Since(start)
	result := BenchmarkResult{
		Operation:    fmt.Sprintf("Concurrent Insert (%d goroutines, %d records)", goroutines, totalCount),
		Duration:     duration,
		RecordsCount: totalCount,
		OpsPerSecond: float64(totalCount) / duration.Seconds(),
	}
	log.Println(result)
}

func intPtr(i int) *int {
	return &i
}
