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
	tempDir, err := os.MkdirTemp("", "pglitedb_test")
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

	// Load existing schemas
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		log.Printf("Warning: failed to load schemas: %v", err)
	}

	// Create a test table
	ctx := context.Background()
	tableDef := &types.TableDefinition{
		Name: "test_users",
		Columns: []types.ColumnDefinition{
			{
				Name:       "id",
				Type:       types.ColumnTypeInteger,
				PrimaryKey: true,
				Nullable:   false,
			},
			{
				Name:     "name",
				Type:     types.ColumnTypeString,
				Nullable: false,
			},
			{
				Name:     "email",
				Type:     types.ColumnTypeString,
				Nullable: true,
			},
		},
	}

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Test inserting data
	data := map[string]interface{}{
		"id":    1,
		"name":  "Alice",
		"email": "alice@example.com",
	}

	record, err := mgr.Insert(ctx, 1, "test_users", data)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	fmt.Printf("Inserted record: %+v\n", record)

	// Test querying data
	result, err := mgr.Query(ctx, 1, "test_users", &types.QueryOptions{})
	if err != nil {
		log.Fatalf("Failed to query data: %v", err)
	}

	fmt.Printf("Query result: %+v\n", result)

	// Verify data
	if result.Count != 1 {
		log.Fatalf("Expected 1 record, got %d", result.Count)
	}

	fmt.Println("Test completed successfully!")
}