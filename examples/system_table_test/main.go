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
	"github.com/guileen/pglitedb/protocol/executor"
	"github.com/guileen/pglitedb/storage"
)

func main() {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "pglitedb_system_test")
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

	// Create executor
	exec := executor.NewExecutor(mgr, eng)

	// Test querying system tables
	ctx := context.Background()

	// Test pg_class
	fmt.Println("Testing pg_class query...")
	result, err := exec.Execute(ctx, &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: "pg_catalog.pg_class",
		TenantID:  1,
		Select: &executor.SelectQuery{
			Columns: []string{"relname", "relkind"},
		},
	})
	if err != nil {
		log.Printf("Warning: pg_class query failed: %v", err)
	} else {
		fmt.Printf("pg_class result: %+v\n", result)
	}

	// Test pg_type
	fmt.Println("Testing pg_type query...")
	result, err = exec.Execute(ctx, &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: "pg_catalog.pg_type",
		TenantID:  1,
		Select: &executor.SelectQuery{
			Columns: []string{"typname", "typoid"},
		},
	})
	if err != nil {
		log.Printf("Warning: pg_type query failed: %v", err)
	} else {
		fmt.Printf("pg_type result: %+v\n", result)
	}

	// Test pg_namespace
	fmt.Println("Testing pg_namespace query...")
	result, err = exec.Execute(ctx, &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: "pg_catalog.pg_namespace",
		TenantID:  1,
		Select: &executor.SelectQuery{
			Columns: []string{"nspname", "nspoid"},
		},
	})
	if err != nil {
		log.Printf("Warning: pg_namespace query failed: %v", err)
	} else {
		fmt.Printf("pg_namespace result: %+v\n", result)
	}

	fmt.Println("System table test completed!")
}