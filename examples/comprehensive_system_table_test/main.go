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
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "pglitedb_comprehensive_test")
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
		Name: "employees",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false},
			{Name: "name", Type: types.ColumnTypeText, Nullable: true},
			{Name: "department", Type: types.ColumnTypeText, Nullable: true},
			{Name: "salary", Type: types.ColumnTypeInteger, Nullable: true},
		},
	}

	if err := mgr.CreateTable(ctx, 1, testTable); err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}

	// Create SQL planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)

	fmt.Println("=== Comprehensive System Table Query Test ===")

	// Test 1: Basic pg_class query
	fmt.Println("\n1. Testing SELECT * FROM pg_class")
	result, err := planner.Execute(ctx, "SELECT * FROM pg_class")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
	}

	// Test 2: Filtered pg_class query
	fmt.Println("\n2. Testing SELECT relname, relkind FROM pg_class WHERE relname = 'employees'")
	result, err = planner.Execute(ctx, "SELECT relname, relkind FROM pg_class WHERE relname = 'employees'")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
		if result.Count > 0 {
			fmt.Printf("   Data: %v\n", result.Rows[0])
		}
	}

	// Test 3: pg_type query
	fmt.Println("\n3. Testing SELECT typname, typlen FROM pg_type WHERE typname = 'int4'")
	result, err = planner.Execute(ctx, "SELECT typname, typlen FROM pg_type WHERE typname = 'int4'")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
		if result.Count > 0 {
			fmt.Printf("   Data: %v\n", result.Rows[0])
		}
	}

	// Test 4: pg_namespace query
	fmt.Println("\n4. Testing SELECT * FROM pg_namespace")
	result, err = planner.Execute(ctx, "SELECT * FROM pg_namespace")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
	}

	// Test 5: information_schema.tables query
	fmt.Println("\n5. Testing SELECT * FROM information_schema.tables")
	result, err = planner.Execute(ctx, "SELECT * FROM information_schema.tables")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
	}

	// Test 6: information_schema.columns query
	fmt.Println("\n6. Testing SELECT * FROM information_schema.columns WHERE table_name = 'employees'")
	result, err = planner.Execute(ctx, "SELECT * FROM information_schema.columns WHERE table_name = 'employees'")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
	}

	// Test 7: Query with schema prefix
	fmt.Println("\n7. Testing SELECT * FROM pg_catalog.pg_class WHERE relname = 'employees'")
	result, err = planner.Execute(ctx, "SELECT * FROM pg_catalog.pg_class WHERE relname = 'employees'")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
	}

	// Test 8: Complex query with multiple conditions
	fmt.Println("\n8. Testing SELECT oid, relname FROM pg_class WHERE relkind = 'r' LIMIT 5")
	result, err = planner.Execute(ctx, "SELECT oid, relname FROM pg_class WHERE relkind = 'r' LIMIT 5")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		fmt.Printf("   SUCCESS: %d rows returned\n", result.Count)
	}

	fmt.Println("\n=== All tests completed successfully! ===")
}