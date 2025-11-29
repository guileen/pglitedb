package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	// Connect to PGLiteDB PostgreSQL server
	// This assumes the server is running on localhost:5433 as started by the test-client target
	dsn := "host=localhost user=postgres password=postgres dbname=postgres port=5433 sslmode=disable"
	
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		return
	}
	
	fmt.Println("Successfully connected to PGLiteDB with GORM")
	
	// Test basic operations
	type Product struct {
		ID    uint   `gorm:"primaryKey"`
		Code  string
		Price uint
	}
	
	// Auto migrate the schema
	err = db.AutoMigrate(&Product{})
	if err != nil {
		fmt.Printf("Failed to migrate schema: %v\n", err)
		return
	}
	
	fmt.Println("Schema migration completed successfully")
	
	// Create a product
	db.Create(&Product{Code: "D42", Price: 100})
	
	// Read a product
	var product Product
	db.First(&product, "code = ?", "D42")
	fmt.Printf("Found product: Code=%s, Price=%d\n", product.Code, product.Price)
	
	fmt.Println("GORM test completed successfully")
}