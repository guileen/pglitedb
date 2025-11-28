# PGLiteDB Interactive Documentation Examples

This guide provides interactive examples that demonstrate PGLiteDB features in action. These examples can be run directly to see how PGLiteDB works in practice.

## Table of Contents

1. [Basic Operations](#basic-operations)
2. [Advanced Querying](#advanced-querying)
3. [Transactions](#transactions)
4. [Multi-tenancy](#multi-tenancy)
5. [Performance Testing](#performance-testing)

## Basic Operations

This example demonstrates basic CRUD operations with PGLiteDB.

```go
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
```

To run this example:
```bash
go run basic_operations.go
```

## Advanced Querying

This example demonstrates advanced querying features including joins, aggregations, and complex conditions.

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/guileen/pglitedb/client"
)

func main() {
    // Create an embedded client
    db := client.NewClient("/tmp/pglitedb-interactive-advanced")
    ctx := context.Background()
    
    // Create tables
    fmt.Println("Creating tables...")
    _, err := db.Query(ctx, `
        CREATE TABLE categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    _, err = db.Query(ctx, `
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            category_id INTEGER REFERENCES categories(id),
            price NUMERIC(10,2),
            stock INTEGER
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert categories
    fmt.Println("Inserting categories...")
    categories := []string{"Electronics", "Books", "Clothing"}
    for _, catName := range categories {
        _, err := db.Insert(ctx, 1, "categories", map[string]interface{}{
            "name": catName,
        })
        if err != nil {
            log.Printf("Error inserting category: %v", err)
        }
    }
    
    // Insert products
    fmt.Println("Inserting products...")
    products := []map[string]interface{}{
        {"name": "Laptop", "category_id": 1, "price": 999.99, "stock": 10},
        {"name": "Smartphone", "category_id": 1, "price": 599.99, "stock": 25},
        {"name": "Novel", "category_id": 2, "price": 19.99, "stock": 100},
        {"name": "Textbook", "category_id": 2, "price": 79.99, "stock": 50},
        {"name": "T-Shirt", "category_id": 3, "price": 29.99, "stock": 200},
        {"name": "Jeans", "category_id": 3, "price": 49.99, "stock": 75},
    }
    
    for _, productData := range products {
        _, err := db.Insert(ctx, 1, "products", productData)
        if err != nil {
            log.Printf("Error inserting product: %v", err)
        }
    }
    
    // Complex query with join (simulated with subqueries)
    fmt.Println("\nProducts with category names:")
    result, err := db.Query(ctx, `
        SELECT 
            p.name as product_name,
            c.name as category_name,
            p.price,
            p.stock
        FROM products p, categories c
        WHERE p.category_id = c.id
        ORDER BY c.name, p.name
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v (%v) - $%.2f (Stock: %v)\n", row[0], row[1], row[2], row[3])
    }
    
    // Aggregation query
    fmt.Println("\nCategory summaries:")
    result, err = db.Query(ctx, `
        SELECT 
            c.name as category_name,
            COUNT(p.id) as product_count,
            AVG(p.price) as avg_price,
            SUM(p.stock) as total_stock
        FROM categories c
        LEFT JOIN products p ON c.id = p.category_id
        GROUP BY c.id, c.name
        ORDER BY c.name
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v: %v products, avg price $%.2f, total stock %v\n", 
            row[0], row[1], row[2], row[3])
    }
    
    // Subquery example
    fmt.Println("\nExpensive products (above average price):")
    result, err = db.Query(ctx, `
        SELECT name, price
        FROM products
        WHERE price > (SELECT AVG(price) FROM products)
        ORDER BY price DESC
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v - $%.2f\n", row[0], row[1])
    }
}
```

To run this example:
```bash
go run advanced_querying.go
```

## Transactions

This example demonstrates transaction management with PGLiteDB, including isolation levels and savepoints.

```go
package main

import (
    "context"
    "database/sql"
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
    
    // Basic transaction
    fmt.Println("\n--- Basic Transaction ---")
    fmt.Println("Transferring $100 from Alice to Bob...")
    
    tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
    if err != nil {
        log.Fatal(err)
    }
    
    // Debit Alice
    _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'")
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    // Credit Bob
    _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 100 WHERE name = 'Bob'")
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    err = tx.Commit()
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Transfer completed successfully!")
    showBalances(db, ctx)
    
    // Transaction with savepoint
    fmt.Println("\n--- Transaction with Savepoint ---")
    fmt.Println("Attempting transfer with validation...")
    
    tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
    if err != nil {
        log.Fatal(err)
    }
    
    // Create savepoint
    savepoint, err := tx.Savepoint()
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    // Transfer $200 from Bob to Charlie
    _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 200 WHERE name = 'Bob'")
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 200 WHERE name = 'Charlie'")
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    // Check if Bob has sufficient funds (should fail)
    var bobBalance float64
    err = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE name = 'Bob'").Scan(&bobBalance)
    if err != nil {
        tx.RollbackToSavepoint(savepoint)
        fmt.Println("Error checking balance, rolled back to savepoint")
    } else if bobBalance < 0 {
        tx.RollbackToSavepoint(savepoint)
        fmt.Println("Insufficient funds for Bob, rolled back to savepoint")
    } else {
        tx.ReleaseSavepoint(savepoint)
        err = tx.Commit()
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("Transfer completed successfully!")
    }
    
    showBalances(db, ctx)
    
    // Demonstrate isolation levels
    fmt.Println("\n--- Isolation Levels ---")
    demonstrateIsolationLevels(db, ctx)
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

func demonstrateIsolationLevels(db *client.Client, ctx context.Context) {
    fmt.Println("Demonstrating different isolation levels...")
    
    // Serializable isolation
    tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Started serializable transaction")
    tx.Commit()
    
    // Repeatable read isolation
    tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Started repeatable read transaction")
    tx.Commit()
    
    // Read committed isolation
    tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Started read committed transaction")
    tx.Commit()
}
```

To run this example:
```bash
go run transactions.go
```

## Multi-tenancy

This example demonstrates PGLiteDB's multi-tenancy features.

```go
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
    db := client.NewClient("/tmp/pglitedb-interactive-multitenancy")
    ctx := context.Background()
    
    // Create a shared table
    fmt.Println("Creating shared projects table...")
    _, err := db.Query(ctx, `
        CREATE TABLE projects (
            id SERIAL PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Create indexes for tenant isolation
    _, err = db.Query(ctx, `
        CREATE INDEX idx_projects_tenant ON projects(tenant_id)
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert data for different tenants
    fmt.Println("Inserting data for different tenants...")
    
    // Tenant 1: Tech Corp
    techProjects := []map[string]interface{}{
        {"tenant_id": 1, "name": "Website Redesign", "description": "Complete website overhaul"},
        {"tenant_id": 1, "name": "Mobile App", "description": "iOS and Android application"},
        {"tenant_id": 1, "name": "API Integration", "description": "Third-party service integration"},
    }
    
    for _, projectData := range techProjects {
        _, err := db.Insert(ctx, 1, "projects", projectData)
        if err != nil {
            log.Printf("Error inserting project: %v", err)
        }
    }
    
    // Tenant 2: Marketing Inc
    marketingProjects := []map[string]interface{}{
        {"tenant_id": 2, "name": "Social Media Campaign", "description": "Q4 social media strategy"},
        {"tenant_id": 2, "name": "Email Marketing", "description": "Newsletter and promotional emails"},
        {"tenant_id": 2, "name": "Content Creation", "description": "Blog posts and articles"},
    }
    
    for _, projectData := range marketingProjects {
        _, err := db.Insert(ctx, 2, "projects", projectData)
        if err != nil {
            log.Printf("Error inserting project: %v", err)
        }
    }
    
    // Query data for each tenant
    fmt.Println("\nTech Corp projects (Tenant 1):")
    result, err := db.Select(ctx, 1, "projects", &types.QueryOptions{
        Where: map[string]interface{}{
            "tenant_id": 1,
        },
        OrderBy: []string{"name ASC"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v: %v\n", row[2], row[3])
    }
    
    fmt.Println("\nMarketing Inc projects (Tenant 2):")
    result, err = db.Select(ctx, 2, "projects", &types.QueryOptions{
        Where: map[string]interface{}{
            "tenant_id": 2,
        },
        OrderBy: []string{"name ASC"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("  %v: %v\n", row[2], row[3])
    }
    
    // Demonstrate tenant isolation
    fmt.Println("\n--- Tenant Isolation ---")
    fmt.Println("Attempting to access Tech Corp projects as Marketing Inc...")
    
    // This should only return Marketing Inc's projects
    result, err = db.Select(ctx, 2, "projects", &types.QueryOptions{
        OrderBy: []string{"name ASC"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Found %d projects (should only be Marketing Inc's):\n", result.Count)
    for _, row := range result.Rows {
        tenantID := row[1]
        projectName := row[2]
        fmt.Printf("  Tenant %v: %v\n", tenantID, projectName)
    }
    
    // Cross-tenant operations (demonstrating proper tenant handling)
    fmt.Println("\n--- Cross-tenant Operations ---")
    fmt.Println("Performing operations for multiple tenants...")
    
    tenants := []int64{1, 2}
    tenantNames := map[int64]string{1: "Tech Corp", 2: "Marketing Inc"}
    
    for _, tenantID := range tenants {
        fmt.Printf("\nOperations for %s (Tenant %d):\n", tenantNames[tenantID], tenantID)
        
        // Count projects
        result, err := db.Select(ctx, tenantID, "projects", &types.QueryOptions{
            Count: true,
        })
        if err != nil {
            log.Printf("Error counting projects: %v", err)
            continue
        }
        fmt.Printf("  Total projects: %d\n", result.Count)
        
        // Add a new project
        newProject := map[string]interface{}{
            "tenant_id":   tenantID,
            "name":        fmt.Sprintf("New Project for %s", tenantNames[tenantID]),
            "description": "Automatically generated project",
        }
        
        insertResult, err := db.Insert(ctx, tenantID, "projects", newProject)
        if err != nil {
            log.Printf("Error inserting project: %v", err)
        } else {
            fmt.Printf("  Added new project with ID: %d\n", insertResult.LastInsertID)
        }
    }
    
    // Final verification
    fmt.Println("\n--- Final Verification ---")
    for _, tenantID := range tenants {
        result, err := db.Select(ctx, tenantID, "projects", &types.QueryOptions{
            Where: map[string]interface{}{
                "tenant_id": tenantID,
            },
            OrderBy: []string{"created_at DESC"},
            Limit:   intPtr(1),
        })
        if err != nil {
            log.Printf("Error querying tenant %d: %v", tenantID, err)
            continue
        }
        
        if len(result.Rows) > 0 {
            latestProject := result.Rows[0][2] // name column
            fmt.Printf("%s latest project: %v\n", tenantNames[tenantID], latestProject)
        }
    }
}

func intPtr(i int) *int {
    return &i
}
```

To run this example:
```bash
go run multitenancy.go
```

## Performance Testing

This example demonstrates performance testing and benchmarking with PGLiteDB.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "runtime"
    "sync"
    "time"
    
    "github.com/guileen/pglitedb/client"
    "github.com/guileen/pglitedb/types"
)

func main() {
    // Create an embedded client
    db := client.NewClient("/tmp/pglitedb-interactive-performance")
    ctx := context.Background()
    
    // Create test table
    fmt.Println("Creating test table...")
    _, err := db.Query(ctx, `
        CREATE TABLE performance_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            value INTEGER NOT NULL,
            category VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Create indexes for performance
    _, err = db.Query(ctx, `
        CREATE INDEX idx_performance_test_category ON performance_test(category)
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Run performance tests
    fmt.Println("\n=== Performance Tests ===")
    
    // Test 1: Bulk insert performance
    fmt.Println("\n1. Bulk Insert Performance Test")
    bulkInsertTest(db, ctx)
    
    // Test 2: Concurrent read performance
    fmt.Println("\n2. Concurrent Read Performance Test")
    concurrentReadTest(db, ctx)
    
    // Test 3: Mixed read/write performance
    fmt.Println("\n3. Mixed Read/Write Performance Test")
    mixedWorkloadTest(db, ctx)
    
    // Test 4: Query performance with different conditions
    fmt.Println("\n4. Query Performance Test")
    queryPerformanceTest(db, ctx)
}

func bulkInsertTest(db *client.Client, ctx context.Context) {
    const recordCount = 10000
    
    fmt.Printf("Inserting %d records...\n", recordCount)
    
    start := time.Now()
    
    // Prepare batch data
    batch := make([]map[string]interface{}, recordCount)
    categories := []string{"A", "B", "C", "D", "E"}
    
    for i := 0; i < recordCount; i++ {
        batch[i] = map[string]interface{}{
            "name":     fmt.Sprintf("Record %d", i),
            "value":    rand.Intn(1000),
            "category": categories[rand.Intn(len(categories))],
        }
    }
    
    // Insert in batches of 1000 for better performance
    const batchSize = 1000
    inserted := 0
    
    for i := 0; i < recordCount; i += batchSize {
        end := i + batchSize
        if end > recordCount {
            end = recordCount
        }
        
        for j := i; j < end; j++ {
            _, err := db.Insert(ctx, 1, "performance_test", batch[j])
            if err != nil {
                log.Printf("Error inserting record %d: %v", j, err)
                continue
            }
            inserted++
        }
        
        if i%5000 == 0 && i > 0 {
            fmt.Printf("  Inserted %d records...\n", inserted)
        }
    }
    
    duration := time.Since(start)
    fmt.Printf("Inserted %d records in %v\n", inserted, duration)
    fmt.Printf("Rate: %.0f records/second\n", float64(inserted)/duration.Seconds())
}

func concurrentReadTest(db *client.Client, ctx context.Context) {
    const concurrentReaders = 50
    const readsPerReader = 100
    
    fmt.Printf("Running %d concurrent readers, %d reads each...\n", concurrentReaders, readsPerReader)
    
    start := time.Now()
    var wg sync.WaitGroup
    var totalErrors int64
    
    for i := 0; i < concurrentReaders; i++ {
        wg.Add(1)
        go func(readerID int) {
            defer wg.Done()
            
            for j := 0; j < readsPerReader; j++ {
                // Random query
                options := &types.QueryOptions{
                    Where: map[string]interface{}{
                        "category": []string{"A", "B", "C", "D", "E"}[rand.Intn(5)],
                    },
                    Limit: intPtr(10),
                }
                
                _, err := db.Select(ctx, 1, "performance_test", options)
                if err != nil {
                    // Atomic increment of error counter
                    totalErrors++
                }
                
                // Small delay to simulate real-world usage
                time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
            }
        }(i)
    }
    
    wg.Wait()
    duration := time.Since(start)
    
    totalReads := concurrentReaders * readsPerReader
    fmt.Printf("Completed %d reads in %v\n", totalReads, duration)
    fmt.Printf("Rate: %.0f reads/second\n", float64(totalReads)/duration.Seconds())
    fmt.Printf("Errors: %d\n", totalErrors)
    
    // Print memory stats
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    fmt.Printf("Memory usage: Alloc = %v MB, Sys = %v MB\n", 
        m.Alloc/1024/1024, m.Sys/1024/1024)
}

func mixedWorkloadTest(db *client.Client, ctx context.Context) {
    const duration = 10 * time.Second
    const writers = 5
    const readers = 20
    
    fmt.Printf("Running mixed workload for %v (%d writers, %d readers)...\n", 
        duration, writers, readers)
    
    start := time.Now()
    var wg sync.WaitGroup
    var totalWrites, totalReads, totalErrors int64
    
    // Writer goroutines
    for i := 0; i < writers; i++ {
        wg.Add(1)
        go func(writerID int) {
            defer wg.Done()
            
            for time.Since(start) < duration {
                data := map[string]interface{}{
                    "name":     fmt.Sprintf("Writer-%d-Time-%d", writerID, time.Now().UnixNano()),
                    "value":    rand.Intn(10000),
                    "category": []string{"A", "B", "C"}[rand.Intn(3)],
                }
                
                _, err := db.Insert(ctx, 1, "performance_test", data)
                if err != nil {
                    totalErrors++
                } else {
                    totalWrites++
                }
                
                time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
            }
        }(i)
    }
    
    // Reader goroutines
    for i := 0; i < readers; i++ {
        wg.Add(1)
        go func(readerID int) {
            defer wg.Done()
            
            for time.Since(start) < duration {
                options := &types.QueryOptions{
                    Where: map[string]interface{}{
                        "value": map[string]interface{}{
                            "$gt": rand.Intn(5000),
                        },
                    },
                    Limit: intPtr(5),
                }
                
                result, err := db.Select(ctx, 1, "performance_test", options)
                if err != nil {
                    totalErrors++
                } else {
                    totalReads++
                    // Process results to simulate real work
                    _ = len(result.Rows)
                }
                
                time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
            }
        }(i)
    }
    
    wg.Wait()
    
    finalDuration := time.Since(start)
    fmt.Printf("Mixed workload completed in %v\n", finalDuration)
    fmt.Printf("Writes: %d (%.0f/second)\n", totalWrites, float64(totalWrites)/finalDuration.Seconds())
    fmt.Printf("Reads: %d (%.0f/second)\n", totalReads, float64(totalReads)/finalDuration.Seconds())
    fmt.Printf("Errors: %d\n", totalErrors)
}

func queryPerformanceTest(db *client.Client, ctx context.Context) {
    queries := []struct {
        name    string
        query   string
        options *types.QueryOptions
    }{
        {
            name:  "Simple select all",
            query: "SELECT * FROM performance_test LIMIT 100",
        },
        {
            name: "Select with category filter",
            options: &types.QueryOptions{
                Where: map[string]interface{}{
                    "category": "A",
                },
                Limit: intPtr(100),
            },
        },
        {
            name: "Select with value range",
            options: &types.QueryOptions{
                Where: map[string]interface{}{
                    "value": map[string]interface{}{
                        "$gt": 500,
                        "$lt": 800,
                    },
                },
                Limit: intPtr(100),
            },
        },
        {
            name: "Select with ordering",
            options: &types.QueryOptions{
                OrderBy: []string{"value DESC"},
                Limit:   intPtr(100),
            },
        },
    }
    
    fmt.Println("Running query performance tests...")
    
    for _, test := range queries {
        fmt.Printf("\nTesting: %s\n", test.name)
        
        const iterations = 100
        start := time.Now()
        
        for i := 0; i < iterations; i++ {
            var err error
            if test.query != "" {
                _, err = db.Query(ctx, test.query)
            } else {
                _, err = db.Select(ctx, 1, "performance_test", test.options)
            }
            
            if err != nil {
                log.Printf("Error in query %s: %v", test.name, err)
                break
            }
        }
        
        duration := time.Since(start)
        avgDuration := duration / time.Duration(iterations)
        fmt.Printf("  Average query time: %v\n", avgDuration)
        fmt.Printf("  Queries per second: %.0f\n", float64(iterations)/duration.Seconds())
    }
}
```

To run this example:
```bash
go run performance_testing.go
```

## Running the Examples

To run any of these interactive examples:

1. Save the code to a `.go` file
2. Run with `go run filename.go`

You can also create a main program that runs all examples:

```go
package main

import (
    "fmt"
    "log"
)

func main() {
    fmt.Println("PGLiteDB Interactive Examples")
    fmt.Println("============================")
    fmt.Println("Choose an example to run:")
    fmt.Println("1. Basic Operations")
    fmt.Println("2. Advanced Querying")
    fmt.Println("3. Transactions")
    fmt.Println("4. Multi-tenancy")
    fmt.Println("5. Performance Testing")
    fmt.Println("")
    fmt.Println("Run individual examples with:")
    fmt.Println("  go run basic_operations.go")
    fmt.Println("  go run advanced_querying.go")
    fmt.Println("  go run transactions.go")
    fmt.Println("  go run multitenancy.go")
    fmt.Println("  go run performance_testing.go")
}
```

These interactive examples provide hands-on experience with PGLiteDB's features and help you understand how to use them effectively in your applications.