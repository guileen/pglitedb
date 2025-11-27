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

	// Create DDL parser
	ddlParser := sql.NewDDLParser()

	fmt.Println("Parsing CREATE TABLE statement with DDL parser...")
	ddlStmt, err := ddlParser.Parse(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to parse CREATE TABLE statement: %v", err)
	}

	fmt.Printf("DDL statement type: %v\n", ddlStmt.Type)
	fmt.Printf("DDL table name: %s\n", ddlStmt.TableName)
	fmt.Printf("DDL columns: %d\n", len(ddlStmt.Columns))

	// Print column details
	for i, col := range ddlStmt.Columns {
		fmt.Printf("Column %d: name=%s, type=%s, not_null=%v, primary_key=%v\n", 
			i, col.Name, col.Type, col.NotNull, col.PrimaryKey)
	}

	fmt.Println("DDL parsing test completed!")
}