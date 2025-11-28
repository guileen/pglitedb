package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/guileen/pglitedb/client"
)

func main() {
    // Create an embedded client
    db := client.NewClient("/tmp/pglitedb-interactive-transactions")
    ctx := context.Background()
    
    // Create accounts table
    fmt.Println("Creating accounts table...")
    _, err := db.Query(ctx, `
        CREATE TABLE accounts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            balance NUMERIC(12,2) NOT NULL DEFAULT 0
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert initial accounts
    fmt.Println("Creating initial accounts...")
    accounts := []map[string]interface{}{
        {"name": "Alice", "balance": 1000.00},
        {"name": "Bob", "balance": 500.00},
        {"name": "Charlie", "balance": 750.00},
    }
    
    for _, accountData := range accounts {
        _, err := db.Insert(ctx, 1, "accounts", accountData)
        if err != nil {
            log.Fatal(err)
        }
    }
    
    // Show initial balances
    fmt.Println("\nInitial account balances:")
    showBalances(db, ctx)
    
    // Basic transaction using SQL statements
    fmt.Println("\n--- Basic Transaction ---")
    fmt.Println("Transferring $100 from Alice to Bob...")
    
    // Begin transaction
    _, err = db.Query(ctx, "BEGIN")
    if err != nil {
        log.Fatal(err)
    }
    
    // Debit Alice
    _, err = db.Query(ctx, "UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'")
    if err != nil {
        db.Query(ctx, "ROLLBACK")
        log.Fatal(err)
    }
    
    // Credit Bob
    _, err = db.Query(ctx, "UPDATE accounts SET balance = balance + 100 WHERE name = 'Bob'")
    if err != nil {
        db.Query(ctx, "ROLLBACK")
        log.Fatal(err)
    }
    
    // Commit transaction
    _, err = db.Query(ctx, "COMMIT")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Transfer completed successfully!")
    showBalances(db, ctx)
}

func showBalances(db *client.Client, ctx context.Context) {
    result, err := db.Query(ctx, "SELECT name, balance FROM accounts ORDER BY name")
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v: $%.2f\n", row[0], row[1])
    }
}