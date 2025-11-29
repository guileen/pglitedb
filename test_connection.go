package main_test

import (
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConnection() {
	// Connect to the database
	connString := "postgresql://postgres@localhost:5666/pgbench_test"
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatal(err)
	}

	// Set connection pool configuration
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = time.Minute * 30
	config.HealthCheckPeriod = time.Minute

	db, err := pgxpool.NewWithConfig(nil, config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test simple query
	var result int
	err = db.QueryRow(nil, "SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Connection successful, result: %d\n", result)
}