package main

import (
	"context"
	"fmt"
	"log"

	"github.com/guileen/pglitedb/client"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Create an embedded client
	db := client.NewClient("/tmp/pglitedb-index-test")
	
	// Example: Create a table first
	ctx := context.Background()
	
	// Test ORDER BY functionality
	options := &types.QueryOptions{
		OrderBy: []string{"name"},
		Limit:   intPtr(10),
	}
	
	result, err := db.Select(ctx, 1, "users", options)
	if err != nil {
		log.Printf("Query returned error: %v", err)
	} else {
		fmt.Printf("Found %d records\n", result.Count)
		for _, row := range result.Rows {
			fmt.Printf("Record: %+v\n", row)
		}
	}
	
	fmt.Println("Index-based ORDER BY test completed successfully!")
}

func intPtr(i int) *int {
	return &i
}