// Package main provides a simple connection test utility for PGLiteDB
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

// connectToPGLiteDB tests connectivity to a PGLiteDB instance
func connectToPGLiteDB() {
	// Connect to the PGLiteDB server
	connStr := "host=localhost port=5666 dbname=pglitedb sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping database:", err)
	}
	fmt.Println("Successfully connected to PGLiteDB!")
}

// func main() {
// 	connectToPGLiteDB()
// }