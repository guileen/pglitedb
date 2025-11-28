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
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Test querying system tables directly
	ctx := context.Background()

	// Test pg_class
	fmt.Println("Testing pg_class query...")
	result, err := mgr.QuerySystemTable(ctx, "pg_catalog.pg_class", nil)
	if err != nil {
		log.Printf("Warning: pg_class query failed: %v", err)
	} else {
		fmt.Printf("pg_class result: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("First row: %+v\n", result.Rows[0])
		}
	}

	// Test pg_type
	fmt.Println("Testing pg_type query...")
	result, err = mgr.QuerySystemTable(ctx, "pg_catalog.pg_type", nil)
	if err != nil {
		log.Printf("Warning: pg_type query failed: %v", err)
	} else {
		fmt.Printf("pg_type result: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("First row: %+v\n", result.Rows[0])
		}
	}

	// Test pg_namespace
	fmt.Println("Testing pg_namespace query...")
	result, err = mgr.QuerySystemTable(ctx, "pg_catalog.pg_namespace", nil)
	if err != nil {
		log.Printf("Warning: pg_namespace query failed: %v", err)
	} else {
		fmt.Printf("pg_namespace result: %d rows\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("First row: %+v\n", result.Rows[0])
		}
	}

	fmt.Println("Direct system table test completed!")
}