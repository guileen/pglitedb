package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "pglitedb_system_fix_test")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Initialize database components
	dbPath := filepath.Join(tempDir, "db")

	// Create a pebble KV store
	config := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		log.Fatalf("Failed to create pebble kv: %v", err)
	}
	defer kvStore.Close()

	// Create codec
	c := codec.NewMemComparableCodec()

	// Create engine and manager
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Create a test table to have some data
	ctx := context.Background()
	testTable := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false},
			{Name: "name", Type: types.ColumnTypeText, Nullable: true},
		},
	}

	if err := mgr.CreateTable(ctx, 1, testTable); err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}

	// Test querying system tables directly
	fmt.Println("=== Testing System Table Queries ===")

	// Test pg_class with filter
	fmt.Println("\n1. Testing pg_class query with filter...")
	filter := map[string]interface{}{
		"relname": "test_table",
	}
	result, err := mgr.QuerySystemTable(ctx, "pg_catalog.pg_class", filter)
	if err != nil {
		log.Printf("Warning: pg_class query with filter failed: %v", err)
	} else {
		fmt.Printf("pg_class result with filter: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			for i, col := range result.Columns {
				fmt.Printf("  Column %d: %s\n", i, col.Name)
			}
			fmt.Printf("  First row: %+v\n", result.Rows[0])
		}
	}

	// Test pg_class without filter
	fmt.Println("\n2. Testing pg_class query without filter...")
	result, err = mgr.QuerySystemTable(ctx, "pg_catalog.pg_class", nil)
	if err != nil {
		log.Printf("Warning: pg_class query without filter failed: %v", err)
	} else {
		fmt.Printf("pg_class result without filter: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("  First row: %+v\n", result.Rows[0])
		}
	}

	// Test pg_type
	fmt.Println("\n3. Testing pg_type query...")
	result, err = mgr.QuerySystemTable(ctx, "pg_catalog.pg_type", nil)
	if err != nil {
		log.Printf("Warning: pg_type query failed: %v", err)
	} else {
		fmt.Printf("pg_type result: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("  First row: %+v\n", result.Rows[0])
		}
	}

	// Test pg_namespace
	fmt.Println("\n4. Testing pg_namespace query...")
	result, err = mgr.QuerySystemTable(ctx, "pg_catalog.pg_namespace", nil)
	if err != nil {
		log.Printf("Warning: pg_namespace query failed: %v", err)
	} else {
		fmt.Printf("pg_namespace result: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("  First row: %+v\n", result.Rows[0])
		}
	}

	// Test information_schema.tables
	fmt.Println("\n5. Testing information_schema.tables query...")
	result, err = mgr.QuerySystemTable(ctx, "information_schema.tables", nil)
	if err != nil {
		log.Printf("Warning: information_schema.tables query failed: %v", err)
	} else {
		fmt.Printf("information_schema.tables result: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("  First row: %+v\n", result.Rows[0])
		}
	}

	fmt.Println("\n=== System table fix test completed! ===")
}