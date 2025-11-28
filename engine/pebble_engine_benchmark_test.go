package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func BenchmarkStorageEngine_UpdateRows_Optimized(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "engine-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	engine := pebble.NewPebbleEngine(kvStore, c)

	ctx := context.Background()
	schema := createTestSchema()

	// Insert test data
	testData := make([]*types.Record, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: "User" + string(rune(i)), Type: types.ColumnTypeString},
				"email":  {Data: "user" + string(rune(i)) + "@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(20 + (i % 50)), Type: types.ColumnTypeNumber},
				"active": {Data: i%2 == 0, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	_, err = engine.InsertRowBatch(ctx, 1, 1, testData, schema)
	if err != nil {
		b.Fatalf("insert batch: %v", err)
	}

	updates := map[string]*types.Value{
		"age": {Data: int64(30), Type: types.ColumnTypeNumber},
	}

	conditions := map[string]interface{}{
		"active": true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.UpdateRows(ctx, 1, 1, updates, conditions, schema)
		if err != nil {
			b.Fatalf("update rows: %v", err)
		}
	}
}

func BenchmarkStorageEngine_DeleteRows_Optimized(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "engine-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	engine := pebble.NewPebbleEngine(kvStore, c)

	ctx := context.Background()
	schema := createTestSchema()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Insert test data
		testData := make([]*types.Record, 100)
		for j := 0; j < 100; j++ {
			testData[j] = &types.Record{
				Data: map[string]*types.Value{
					"name":   {Data: "User" + string(rune(j)), Type: types.ColumnTypeString},
					"email":  {Data: "user" + string(rune(j)) + "@example.com", Type: types.ColumnTypeString},
					"age":    {Data: int64(20 + (j % 50)), Type: types.ColumnTypeNumber},
					"active": {Data: j%3 == 0, Type: types.ColumnTypeBoolean},
				},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
		}

		_, err = engine.InsertRowBatch(ctx, 1, 1, testData, schema)
		if err != nil {
			b.Fatalf("insert batch: %v", err)
		}
		b.StartTimer()

		conditions := map[string]interface{}{
			"active": true,
		}

		_, err := engine.DeleteRows(ctx, 1, 1, conditions, schema)
		if err != nil {
			b.Fatalf("delete rows: %v", err)
		}
	}
}

func BenchmarkSortInt64Slice(b *testing.B) {
	// Create test data
	data := make([]int64, 1000)
	for i := range data {
		data[i] = int64(1000 - i) // Reverse sorted data for worst case
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Copy the data to avoid sorting already sorted data
		testData := make([]int64, len(data))
		copy(testData, data)
		
		// pebble.SortInt64Slice(testData)
	}
}

func BenchmarkIndexIterator_Next(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "engine-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	engine := pebble.NewPebbleEngine(kvStore, c)

	ctx := context.Background()
	schema := createTestSchema()

	// Insert test data
	testData := make([]*types.Record, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: "User" + string(rune(i)), Type: types.ColumnTypeString},
				"email":  {Data: "user" + string(rune(i)) + "@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(20 + (i % 50)), Type: types.ColumnTypeNumber},
				"active": {Data: i%2 == 0, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	_, err = engine.InsertRowBatch(ctx, 1, 1, testData, schema)
	if err != nil {
		b.Fatalf("insert batch: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create an index iterator and iterate through all records
		iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
		if err != nil {
			b.Fatalf("create iterator: %v", err)
		}
		
		count := 0
		for iter.Next() {
			_ = iter.Row()
			count++
		}
		
		if err := iter.Error(); err != nil {
			b.Fatalf("iterator error: %v", err)
		}
		
		iter.Close()
		
		if count != 1000 {
			b.Fatalf("expected 1000 records, got %d", count)
		}
	}
}