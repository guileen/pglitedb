package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Connect to the database
	connStr := "host=localhost port=5432 user=postgres dbname=pgbench_test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to database successfully!")

	// Check if pgbench tables exist
	tables := []string{"pgbench_branches", "pgbench_accounts", "pgbench_tellers", "pgbench_history"}
	for _, table := range tables {
		var exists bool
		query := `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		);`
		err = db.QueryRow(query, table).Scan(&exists)
		if err != nil {
			log.Fatal(err)
		}
		if exists {
			fmt.Printf("Table %s exists\n", table)
		} else {
			fmt.Printf("Table %s does not exist\n", table)
		}
	}

	// Run a simple test query
	var version string
	err = db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Database version: %s\n", version)

	// Test a simple pgbench table query
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM pgbench_branches").Scan(&count)
	if err != nil {
		fmt.Printf("Error querying pgbench_branches: %v\n", err)
	} else {
		fmt.Printf("Number of rows in pgbench_branches: %d\n", count)
	}

	fmt.Println("pgbench test completed at:", time.Now().Format("2006-01-02 15:04:05"))
}