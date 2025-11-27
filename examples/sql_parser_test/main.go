package main

import (
	"fmt"
	"log"

	"github.com/guileen/pglitedb/protocol/sql"
)

func main() {
	// Test CREATE TABLE statement
	createTableSQL := `CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(255),
		age INTEGER,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	// Create SQL parser (not DDL parser)
	parser := sql.NewPGParser()

	fmt.Println("Parsing CREATE TABLE statement with SQL parser...")
	parsed, err := parser.Parse(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to parse CREATE TABLE statement: %v", err)
	}

	fmt.Printf("Parsed statement type: %v\n", parsed.Type)
	fmt.Printf("Parsed statement: %+v\n", parsed.Statement)
	fmt.Printf("Parsed table name: %s\n", parsed.Table)

	fmt.Println("SQL parser test completed!")
}