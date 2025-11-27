package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "pglitedb_sql_system_test")
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
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Create a test table to have some data
	ctx := context.Background()
	testTable := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false},
			{Name: "name", Type: types.ColumnTypeText, Nullable: true},
			{Name: "email", Type: types.ColumnTypeText, Nullable: true},
		},
	}

	if err := mgr.CreateTable(ctx, 1, testTable); err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}

	// Create SQL planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)

	// Test SQL queries on system tables
	fmt.Println("=== Testing SQL Queries on System Tables ===")

	// Test basic pg_class query
	fmt.Println("\n1. Testing SELECT * FROM pg_class...")
	result, err := planner.Execute(ctx, "SELECT * FROM pg_class")
	if err != nil {
		log.Printf("Warning: SELECT * FROM pg_class failed: %v", err)
	} else {
		fmt.Printf("SELECT * FROM pg_class: %d rows, %d columns\n", result.Count, len(result.Columns))
		if len(result.Rows) > 0 {
			fmt.Printf("  First row sample: [")
			for i, val := range result.Rows[0] {
				if i > 2 { // Limit output for readability
					fmt.Printf("...")
					break
				}
				fmt.Printf("%v ", val)
			}
			fmt.Printf("]\n")
		}
	}

	// Test filtered pg_class query
	fmt.Println("\n2. Testing SELECT relname, relkind FROM pg_class WHERE relname = 'users'...")
	result, err = planner.Execute(ctx, "SELECT relname, relkind FROM pg_class WHERE relname = 'users'")
	if err != nil {
		log.Printf("Warning: SELECT with filter failed: %v", err)
	} else {
		fmt.Printf("SELECT with filter: %d rows, %d columns\n", result.Count, len(result.Columns))
		if len(result.Rows) > 0 {
			fmt.Printf("  Columns: %v\n", result.Columns)
			fmt.Printf("  Rows: %v\n", result.Rows)
		}
	}

	// Test pg_type query
	fmt.Println("\n3. Testing SELECT typname, typlen FROM pg_type WHERE typname = 'int4'...")
	result, err = planner.Execute(ctx, "SELECT typname, typlen FROM pg_type WHERE typname = 'int4'")
	if err != nil {
		log.Printf("Warning: SELECT pg_type failed: %v", err)
	} else {
		fmt.Printf("SELECT pg_type: %d rows, %d columns\n", result.Count, len(result.Columns))
		if len(result.Rows) > 0 {
			fmt.Printf("  Columns: %v\n", result.Columns)
			fmt.Printf("  Rows: %v\n", result.Rows)
		}
	}

	// Test pg_namespace query
	fmt.Println("\n4. Testing SELECT nspname FROM pg_namespace...")
	result, err = planner.Execute(ctx, "SELECT nspname FROM pg_namespace")
	if err != nil {
		log.Printf("Warning: SELECT pg_namespace failed: %v", err)
	} else {
		fmt.Printf("SELECT pg_namespace: %d rows, %d columns\n", result.Count, len(result.Columns))
		if len(result.Rows) > 0 {
			fmt.Printf("  Columns: %v\n", result.Columns)
			fmt.Printf("  Rows: %v\n", result.Rows)
		}
	}

	// Test information_schema query
	fmt.Println("\n5. Testing SELECT table_name, table_type FROM information_schema.tables...")
	result, err = planner.Execute(ctx, "SELECT table_name, table_type FROM information_schema.tables")
	if err != nil {
		log.Printf("Warning: SELECT information_schema.tables failed: %v", err)
	} else {
		fmt.Printf("SELECT information_schema.tables: %d rows, %d columns\n", result.Count, len(result.Columns))
		if len(result.Rows) > 0 {
			fmt.Printf("  Columns: %v\n", result.Columns)
			fmt.Printf("  Rows: %v\n", result.Rows)
		}
	}

	// Test query planning
	fmt.Println("\n6. Testing query planning for system table...")
	plan, err := planner.CreatePlan("SELECT oid, relname FROM pg_class WHERE relname = 'users'")
	if err != nil {
		log.Printf("Warning: Plan creation failed: %v", err)
	} else {
		fmt.Printf("Query plan created successfully:\n")
		fmt.Printf("  Type: %v\n", plan.Type)
		fmt.Printf("  Operation: %s\n", plan.Operation)
		fmt.Printf("  Table: %s\n", plan.Table)
		fmt.Printf("  Fields: %v\n", plan.Fields)
		fmt.Printf("  Conditions: %v\n", plan.Conditions)
	}

	fmt.Println("\n=== SQL system table test completed! ===")
}