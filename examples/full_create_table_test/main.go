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
)

func main() {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "pglitedb_full_test")
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

	// Create parser and planner
	parser := sql.NewPGParser()
	planner := sql.NewPlanner(parser)
	planner.SetCatalog(mgr)

	// Create executor
	executor := sql.NewExecutorWithCatalog(planner, mgr)

	// Test CREATE TABLE statement
	createTableSQL := `CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(255),
		age INTEGER,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	fmt.Println("Executing CREATE TABLE statement...")
	result, err := executor.Execute(context.Background(), createTableSQL)
	if err != nil {
		log.Fatalf("CREATE TABLE execution failed: %v", err)
	}

	fmt.Printf("CREATE TABLE executed successfully: %+v\n", result)

	// Check if table was created by querying pg_class
	fmt.Println("Checking if table was created...")
	tableResult, err := mgr.QuerySystemTable(context.Background(), "pg_catalog.pg_class", map[string]interface{}{
		"relname": "users",
	})
	if err != nil {
		log.Printf("Warning: pg_class query failed: %v", err)
	} else {
		fmt.Printf("Table 'users' found in pg_class: %d rows\n", len(tableResult.Rows))
		if len(tableResult.Rows) > 0 {
			fmt.Printf("Table details: name=%v, relkind=%v\n", tableResult.Rows[0][0], tableResult.Rows[0][14])
		}
	}

	// Check columns by querying pg_attribute
	fmt.Println("Checking table columns...")
	attrResult, err := mgr.QuerySystemTable(context.Background(), "pg_catalog.pg_attribute", map[string]interface{}{
		"relname": "users",
	})
	if err != nil {
		log.Printf("Warning: pg_attribute query failed: %v", err)
	} else {
		fmt.Printf("Columns found in pg_attribute: %d rows\n", len(attrResult.Rows))
		for i, row := range attrResult.Rows {
			fmt.Printf("Column %d: name=%v, attnum=%v\n", i, row[1], row[4])
		}
	}

	fmt.Println("Full CREATE TABLE test completed!")
}