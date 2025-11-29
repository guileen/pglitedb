package main_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkSimple() {
	// Connect to the database
	connString := "postgresql://postgres@localhost:5666/postgres"
	db, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test simple insert performance
	fmt.Println("Testing simple insert performance...")
	
	// Create test table
	_, err = db.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name VARCHAR(100), value INTEGER)")
	if err != nil {
		log.Fatal(err)
	}

	// Clear test table
	_, err = db.Exec(context.Background(), "TRUNCATE test_table")
	if err != nil {
		log.Fatal(err)
	}

	// Perform benchmark
	const numRecords = 1000
	startTime := time.Now()

	for i := 0; i < numRecords; i++ {
		_, err = db.Exec(context.Background(), "INSERT INTO test_table (name, value) VALUES ($1, $2)", 
			fmt.Sprintf("test_name_%d", i), i)
		if err != nil {
			log.Fatal(err)
		}
	}

	duration := time.Since(startTime)
	tps := float64(numRecords) / duration.Seconds()

	fmt.Printf("Inserted %d records in %v\n", numRecords, duration)
	fmt.Printf("TPS: %.2f\n", tps)

	// Test concurrent performance
	fmt.Println("\nTesting concurrent insert performance...")
	testConcurrentInserts(db)
}

func testConcurrentInserts(db *pgxpool.Pool) {
	const numWorkers = 10
	const recordsPerWorker = 100
	
	var wg sync.WaitGroup
	startTime := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for i := 0; i < recordsPerWorker; i++ {
				recordID := workerID*recordsPerWorker + i
				_, err := db.Exec(context.Background(), 
					"INSERT INTO test_table (name, value) VALUES ($1, $2)", 
					fmt.Sprintf("worker_%d_record_%d", workerID, recordID), recordID)
				if err != nil {
					log.Printf("Error inserting record: %v", err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	
	duration := time.Since(startTime)
	totalRecords := numWorkers * recordsPerWorker
	tps := float64(totalRecords) / duration.Seconds()

	fmt.Printf("Inserted %d records concurrently in %v\n", totalRecords, duration)
	fmt.Printf("Concurrent TPS: %.2f\n", tps)
}