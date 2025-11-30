package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/guileen/pglitedb/client"
)

func main() {
	// Test concurrent access to identify the issue
	dbPath := "/tmp/pglitedb-concurrent-test"
	
	// Clean up any existing database
	//os.RemoveAll(dbPath)
	
	db := client.NewClient(dbPath)
	ctx := context.Background()
	tenantID := int64(1)
	tableName := "test_table"
	
	// Create table with initial record
	firstRecord := map[string]interface{}{
		"id":    1,
		"name":  "TestUser",
		"email": "test@example.com",
	}
	
	_, err := db.Insert(ctx, tenantID, tableName, firstRecord)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
		return
	}
	
	log.Printf("Created table with initial record")
	
	// Test concurrent access
	var wg sync.WaitGroup
	numWorkers := 10
	
	start := time.Now()
	
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < 100; j++ {
				// Try different operations
				switch j % 3 {
				case 0:
					// Insert operation
					record := map[string]interface{}{
						"id":    workerID*1000 + j,
						"name":  fmt.Sprintf("User_%d_%d", workerID, j),
						"email": fmt.Sprintf("user_%d_%d@example.com", workerID, j),
					}
					_, err := db.Insert(ctx, tenantID, tableName, record)
					if err != nil {
						log.Printf("Worker %d insert error: %v", workerID, err)
					}
					
				case 1:
					// Select operation
					_, err := db.Select(ctx, tenantID, tableName, nil)
					if err != nil {
						log.Printf("Worker %d select error: %v", workerID, err)
					}
					
				case 2:
					// Update operation
					updates := map[string]interface{}{
						"name": fmt.Sprintf("Updated_User_%d_%d", workerID, j),
					}
					conditions := map[string]interface{}{
						"id": workerID*1000 + j,
					}
					_, err := db.Update(ctx, tenantID, tableName, updates, conditions)
					if err != nil {
						log.Printf("Worker %d update error: %v", workerID, err)
					}
				}
				
				// Small delay to increase chance of detecting issues
				time.Sleep(time.Millisecond * 1)
			}
		}(i)
	}
	
	wg.Wait()
	
	log.Printf("Completed concurrent test in %v", time.Since(start))
	log.Printf("Test finished successfully")
}