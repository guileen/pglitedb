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
}