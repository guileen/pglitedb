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
	tempDir, err := os.MkdirTemp("", "pglitedb_table_test")
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

	fmt.Println("Creating test table...")
	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("Table created successfully!")

	// Test querying pg_class to see if our table appears
	fmt.Println("Testing pg_class query...")
	result, err := mgr.QuerySystemTable(ctx, "pg_catalog.pg_class", nil)
	if err != nil {
		log.Printf("Warning: pg_class query failed: %v", err)
	} else {
		fmt.Printf("pg_class result: %d rows\n", len(result.Rows))
		for i, row := range result.Rows {
			fmt.Printf("Row %d: %v\n", i, row[0]) // Print table name (first column)
		}
	}

	// Test querying pg_attribute to see column information
	fmt.Println("Testing pg_attribute query...")
	result, err = mgr.QuerySystemTable(ctx, "pg_catalog.pg_attribute", nil)
	if err != nil {
		log.Printf("Warning: pg_attribute query failed: %v", err)
	} else {
		fmt.Printf("pg_attribute result: %d rows\n", len(result.Rows))
		for i, row := range result.Rows {
			fmt.Printf("Row %d: table=%v, column=%v, attnum=%v\n", i, row[0], row[1], row[4])
		}
	}

	fmt.Println("Table creation and system table test completed!")
}