package main

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"time"

	"github.com/guileen/pglitedb/client"
)

func main() {
	fmt.Println("Testing pgbench table initialization...")

	// Start the PGLiteDB server
	dbPath := fmt.Sprintf("/tmp/pglitedb-test-%d", time.Now().UnixNano())
	cmd := exec.Command("go", "run", "../../cmd/server", dbPath, "pg")
	err := cmd.Start()
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer cmd.Process.Kill()

	// Wait for server to start
	time.Sleep(2 * time.Second)

	// Connect to the database
	db := client.NewClient(dbPath)
	ctx := context.Background()

	// Test basic connection
	_, err = db.ListTables(ctx, 1)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	fmt.Println("Successfully connected to database")

	// Note: Actual pgbench initialization would be done via the pgbench command
	// This test just verifies that the database is working properly
	fmt.Println("Test completed successfully")
}