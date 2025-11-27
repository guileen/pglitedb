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
	tempDir, err := os.MkdirTemp("", "pglitedb_is_system_test")
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

	// Test isSystemTable function directly
	fmt.Println("=== Testing isSystemTable Function ===")
	
	// Test different table names
	testCases := []string{
		"pg_class",
		"pg_catalog.pg_class",
		"information_schema.tables",
		"users",
		"pg_type",
		"pg_catalog.pg_type",
		"tables",
		"columns",
	}
	
	for _, testCase := range testCases {
		isSys := sql.IsSystemTable(testCase) // Assuming we export this function
		fmt.Printf("isSystemTable('%s'): %v\n", testCase, isSys)
	}
	
	// Test query planning
	fmt.Println("\n=== Testing Query Planning ===")
	
	plan, err := planner.CreatePlan("SELECT * FROM pg_class")
	if err != nil {
		log.Printf("Plan creation failed: %v", err)
	} else {
		fmt.Printf("Plan created:\n")
		fmt.Printf("  Table: '%s'\n", plan.Table)
		fmt.Printf("  Is system table: %v\n", sql.IsSystemTable(plan.Table))
	}
}