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
	tempDir, err := os.MkdirTemp("", "pglitedb_ddl_test")
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

	// Load schemas (should be empty initially)
	ctx := context.Background()
	if err := mgr.LoadSchemas(ctx); err != nil {
		log.Printf("Warning: failed to load schemas: %v", err)
	}

	// Create SQL parser and planner
	parser := sql.NewPGParser()
	planner := sql.NewPlanner(parser)
	planner.SetCatalog(mgr)

	// Test CREATE TABLE statement
	createTableSQL := `CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(255),
		age INTEGER,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	fmt.Println("Parsing CREATE TABLE statement...")
	parsed, err := parser.Parse(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to parse CREATE TABLE statement: %v", err)
	}

	fmt.Printf("Parsed statement type: %v\n", parsed.Type)
	fmt.Printf("Parsed table name: %s\n", parsed.Table)

	// Try to execute the CREATE TABLE statement
	fmt.Println("Executing CREATE TABLE statement...")
	result, err := planner.Execute(ctx, createTableSQL)
	if err != nil {
		log.Printf("Warning: CREATE TABLE execution failed: %v", err)
	} else {
		fmt.Printf("CREATE TABLE executed successfully: %+v\n", result)
	}

	// Check if table was created by querying pg_class
	fmt.Println("Checking if table was created...")
	tableResult, err := mgr.QuerySystemTable(ctx, "pg_catalog.pg_class", map[string]interface{}{
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
	attrResult, err := mgr.QuerySystemTable(ctx, "pg_catalog.pg_attribute", map[string]interface{}{
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

	fmt.Println("DDL parsing and execution test completed!")
}