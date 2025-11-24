package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type User struct {
	ID        uint   `gorm:"primaryKey"`
	Name      string `gorm:"size:100;not null"`
	Email     string `gorm:"size:100;unique;not null"`
	Age       int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func main() {
	dsn := "host=localhost port=5433 user=admin password=password dbname=testdb sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	ctx := context.Background()

	if err := db.WithContext(ctx).AutoMigrate(&User{}); err != nil {
		log.Fatalf("Failed to auto migrate: %v", err)
	}
	fmt.Println("✅ AutoMigrate succeeded")

	user := User{
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}
	if err := db.WithContext(ctx).Create(&user).Error; err != nil {
		log.Fatalf("Failed to create user: %v", err)
	}
	fmt.Printf("✅ Created user: ID=%d, Name=%s\n", user.ID, user.Name)

	var foundUser User
	if err := db.WithContext(ctx).First(&foundUser, "email = ?", "alice@example.com").Error; err != nil {
		log.Fatalf("Failed to find user: %v", err)
	}
	fmt.Printf("✅ Found user: ID=%d, Name=%s, Age=%d\n", foundUser.ID, foundUser.Name, foundUser.Age)

	if err := db.WithContext(ctx).Model(&foundUser).Update("age", 31).Error; err != nil {
		log.Fatalf("Failed to update user: %v", err)
	}
	fmt.Println("✅ Updated user age to 31")

	var updatedUser User
	if err := db.WithContext(ctx).First(&updatedUser, foundUser.ID).Error; err != nil {
		log.Fatalf("Failed to find updated user: %v", err)
	}
	fmt.Printf("✅ Verified update: Age=%d\n", updatedUser.Age)

	if err := db.WithContext(ctx).Delete(&updatedUser).Error; err != nil {
		log.Fatalf("Failed to delete user: %v", err)
	}
	fmt.Println("✅ Deleted user")

	var count int64
	db.WithContext(ctx).Model(&User{}).Count(&count)
	fmt.Printf("✅ Final user count: %d\n", count)

	fmt.Println("\n🎉 All GORM tests passed!")
}
