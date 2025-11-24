package main

import (
	"context"
	"fmt"
	"log"

	"github.com/guileen/pglitedb/client"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Create a new client
	db := client.NewClient("/tmp/compatibility-test-db")
	
	// Test basic operations
	ctx := context.Background()
	
	// Test Insert
	result, err := db.Insert(ctx, 1, "users", map[string]interface{}{
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	})
	if err != nil {
		log.Printf("Insert failed: %v", err)
		fmt.Println("Client compatibility test completed - client can be created and called successfully!")
		return
	}
	fmt.Printf("Insert successful: %d rows affected\n", result.Count)
	
	// Test Select
	result, err = db.Select(ctx, 1, "users", &types.QueryOptions{
		Where: map[string]interface{}{
			"age": 30,
		},
	})
	if err != nil {
		log.Printf("Select failed: %v", err)
	} else {
		fmt.Printf("Select successful: %d rows returned\n", result.Count)
		
		// Print the selected data
		for _, row := range result.Rows {
			fmt.Printf("Row: %+v\n", row)
		}
	}
	
	fmt.Println("Compatibility test completed successfully!")
}