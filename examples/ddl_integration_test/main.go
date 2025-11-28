package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
)

func main() {
	// Clean up any existing test database
	os.RemoveAll("test.db")

	// Create a new database instance
	config := storage.DefaultPebbleConfig("test.db")
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		log.Fatal(err)
	}
	defer kvStore.Close()

	// Create engine and catalog
	codec := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, codec)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Create parser, planner and executor
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)

	ctx := context.Background()

	// Test CREATE TABLE
	fmt.Println("Testing CREATE TABLE...")
	createQuery := "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE)"
	result, err := planner.Execute(ctx, createQuery)
	if err != nil {
		log.Fatal("CREATE TABLE failed:", err)
	}
	fmt.Printf("CREATE TABLE result: %+v\n", result)

	// Test DROP TABLE
	fmt.Println("Testing DROP TABLE...")
	dropQuery := "DROP TABLE users"
	result, err = planner.Execute(ctx, dropQuery)
	if err != nil {
		log.Fatal("DROP TABLE failed:", err)
	}
	fmt.Printf("DROP TABLE result: %+v\n", result)

	// Test DROP TABLE IF EXISTS (should not fail)
	fmt.Println("Testing DROP TABLE IF EXISTS...")
	dropIfExistsQuery := "DROP TABLE IF EXISTS users"
	result, err = planner.Execute(ctx, dropIfExistsQuery)
	if err != nil {
		log.Fatal("DROP TABLE IF EXISTS failed:", err)
	}
	fmt.Printf("DROP TABLE IF EXISTS result: %+v\n", result)

	fmt.Println("All DDL tests passed!")
}