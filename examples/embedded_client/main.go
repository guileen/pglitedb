package main

import (
	"context"
	"fmt"
	"log"

	"github.com/guileen/pglitedb/client"
	"github.com/guileen/pglitedb/table"
)

func main() {
	// Create an embedded client
	db := client.NewClient("/tmp/pglitedb-example")

	// Example: Insert a record
	ctx := context.Background()
	data := map[string]interface{}{
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}

	result, err := db.Insert(ctx, 1, "users", data)
	if err != nil {
		log.Fatalf("Failed to insert record: %v", err)
	}

	fmt.Printf("Inserted record with ID: %d\n", result.Count)

	// Example: Query records
	options := &table.QueryOptions{
		Where: map[string]interface{}{
			"age": 30,
		},
		Limit: intPtr(10),
	}

	result, err = db.Select(ctx, 1, "users", options)
	if err != nil {
		log.Fatalf("Failed to query records: %v", err)
	}

	fmt.Printf("Found %d records\n", result.Count)
	for _, record := range result.Rows {
		fmt.Printf("Record: %+v\n", record.Data)
	}

	// Example: Update a record
	updateData := map[string]interface{}{
		"age": 31,
	}

	where := map[string]interface{}{
		"name": "John Doe",
	}

	result, err = db.Update(ctx, 1, "users", where, updateData)
	if err != nil {
		log.Fatalf("Failed to update record: %v", err)
	}

	fmt.Printf("Updated %d records\n", result.Count)

	// Example: Delete a record
	result, err = db.Delete(ctx, 1, "users", where)
	if err != nil {
		log.Fatalf("Failed to delete record: %v", err)
	}

	fmt.Printf("Deleted %d records\n", result.Count)
}

func intPtr(i int) *int {
	return &i
}
