package main

import (
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
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Create parser and planner
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
	fmt.Printf("Parsed table: %s\n", parsed.Table)
	fmt.Printf("Parsed statement content: %+v\n", parsed.Statement)

	fmt.Println("Creating execution plan...")
	plan, err := planner.CreatePlan(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create execution plan: %v", err)
	}

	fmt.Printf("Plan operation: %s\n", plan.Operation)
	fmt.Printf("Plan type: %v\n", plan.Type)

	fmt.Println("Full debug test completed!")
}