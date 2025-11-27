package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "pglitedb_debug_test")
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

	// Test direct system table query
	fmt.Println("=== Testing Direct System Table Query ===")
	
	result, err := mgr.QuerySystemTable(ctx, "pg_catalog.pg_class", nil)
	if err != nil {
		log.Printf("Direct pg_class query failed: %v", err)
	} else {
		fmt.Printf("Direct pg_class query succeeded: %d rows\n", len(result.Rows))
	}
	
	// Test through QueryManager interface
	fmt.Println("\n=== Testing Through QueryManager Interface ===")
	
	opts := &types.QueryOptions{}
	result2, err := mgr.Query(ctx, 1, "pg_catalog.pg_class", opts)
	if err != nil {
		log.Printf("QueryManager pg_class query failed: %v", err)
	} else {
		fmt.Printf("QueryManager pg_class query succeeded: %d rows\n", len(result2.Rows))
	}
	
	// Test isSystemTable function
	fmt.Println("\n=== Testing isSystemTable Function ===")
	
	// We need to access the queryManager's isSystemTable method
	// Let's create a simple test
	testNames := []string{
		"pg_class",
		"pg_catalog.pg_class",
		"information_schema.tables",
		"users",
		"pg_type",
		"pg_catalog.pg_type",
	}
	
	for _, name := range testNames {
		// Manual check
		isSys := isSystemTableManual(name)
		fmt.Printf("isSystemTable('%s'): %v\n", name, isSys)
	}
}

func isSystemTableManual(tableName string) bool {
	// Normalize table name by removing any extra whitespace and converting to lowercase
	normalized := strings.TrimSpace(strings.ToLower(tableName))
	
	// Check for system table prefixes
	return strings.HasPrefix(normalized, "information_schema.") || 
		   strings.HasPrefix(normalized, "pg_catalog.")
}