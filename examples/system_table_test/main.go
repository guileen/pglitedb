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

	// Create SQL parser and planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()

	// Test querying system tables
	// ctx := context.Background()

	// Test pg_class
	fmt.Println("Testing pg_class query...")
	result, err := exec.Execute(context.Background(), "SELECT relname, relkind FROM pg_catalog.pg_class")
	if err != nil {
		log.Printf("Warning: pg_class query failed: %v", err)
	} else {
		fmt.Printf("pg_class result: %+v\n", result)
	}

	// Test pg_type
	fmt.Println("Testing pg_type query...")
	result, err = exec.Execute(context.Background(), "SELECT typname, typoid FROM pg_catalog.pg_type")
	if err != nil {
		log.Printf("Warning: pg_type query failed: %v", err)
	} else {
		fmt.Printf("pg_type result: %+v\n", result)
	}

	// Test pg_namespace
	fmt.Println("Testing pg_namespace query...")
	result, err = exec.Execute(context.Background(), "SELECT nspname, nspoid FROM pg_catalog.pg_namespace")
	if err != nil {
		log.Printf("Warning: pg_namespace query failed: %v", err)
	} else {
		fmt.Printf("pg_namespace result: %+v\n", result)
	}

	fmt.Println("System table test completed!")
}