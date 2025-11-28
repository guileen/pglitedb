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
    db := client.NewClient("/tmp/pglitedb-interactive-basic")
    ctx := context.Background()
    
    // Create a table
    fmt.Println("Creating users table...")
    _, err := db.Query(ctx, `
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert records
    fmt.Println("Inserting records...")
    users := []map[string]interface{}{
        {"name": "Alice Johnson", "email": "alice@example.com", "age": 30},
        {"name": "Bob Smith", "email": "bob@example.com", "age": 25},
        {"name": "Charlie Brown", "email": "charlie@example.com", "age": 35},
    }
    
    for _, userData := range users {
        result, err := db.Insert(ctx, 1, "users", userData)
        if err != nil {
            log.Printf("Error inserting user: %v", err)
            continue
        }
        fmt.Printf("Inserted user with ID: %d\n", result.LastInsertID)
    }
    
    // Query all users
    fmt.Println("\nQuerying all users...")
    result, err := db.Select(ctx, 1, "users", &types.QueryOptions{})
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Found %d users:\n", result.Count)
    for _, row := range result.Rows {
        fmt.Printf("  ID: %v, Name: %v, Email: %v, Age: %v\n", 
            row[0], row[1], row[2], row[3])
    }
    
    // Query with conditions
    fmt.Println("\nQuerying users over 25...")
    result, err = db.Select(ctx, 1, "users", &types.QueryOptions{
        Where: map[string]interface{}{
            "age": map[string]interface{}{"$gt": 25},
        },
        OrderBy: []string{"age DESC"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Found %d users over 25:\n", result.Count)
    for _, row := range result.Rows {
        fmt.Printf("  %v (age %v)\n", row[1], row[3])
    }
    
    // Update records
    fmt.Println("\nUpdating Bob's email...")
    updateData := map[string]interface{}{
        "email": "bob.smith@newdomain.com",
    }
    where := map[string]interface{}{
        "name": "Bob Smith",
    }
    
    result, err = db.Update(ctx, 1, "users", updateData, where)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Updated %d records\n", result.Count)
    
    // Delete records
    fmt.Println("\nDeleting Charlie...")
    where = map[string]interface{}{
        "name": "Charlie Brown",
    }
    
    result, err = db.Delete(ctx, 1, "users", where)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Deleted %d records\n", result.Count)
    
    // Final query
    fmt.Println("\nFinal user list:")
    result, err = db.Select(ctx, 1, "users", &types.QueryOptions{
        OrderBy: []string{"name ASC"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v (%v) - %v\n", row[1], row[3], row[2])
    }
}