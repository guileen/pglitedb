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