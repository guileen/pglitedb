# PGLiteDB Quick Start Guides

This section provides quick start guides for different use cases of PGLiteDB, helping you get up and running quickly with the database in various scenarios.

## Table of Contents

1. [Web Application Backend](#web-application-backend)
2. [Desktop Application](#desktop-application)
3. [Mobile Application Backend](#mobile-application-backend)
4. [IoT Edge Computing](#iot-edge-computing)
5. [Data Analytics Pipeline](#data-analytics-pipeline)

## Web Application Backend

This guide shows how to use PGLiteDB as a backend database for web applications.

### Setup

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    
    "github.com/guileen/pglitedb/client"
    "github.com/guileen/pglitedb/types"
)

var db *client.Client

func init() {
    // Initialize database
    db = client.NewClient("./data/webapp")
    ctx := context.Background()
    
    // Create tables if they don't exist
    db.Query(ctx, `
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
}

func main() {
    http.HandleFunc("/users", handleUsers)
    http.HandleFunc("/users/", handleUserByID)
    
    log.Println("Server starting on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleUsers(w http.ResponseWriter, r *http.Request) {
    ctx := context.Background()
    
    switch r.Method {
    case http.MethodGet:
        // List users
        options := &types.QueryOptions{
            OrderBy: []string{"created_at DESC"},
            Limit:   intPtr(50),
        }
        
        result, err := db.Select(ctx, 1, "users", options)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(result.Rows)
        
    case http.MethodPost:
        // Create user
        var userData map[string]interface{}
        if err := json.NewDecoder(r.Body).Decode(&userData); err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }
        
        result, err := db.Insert(ctx, 1, "users", userData)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusCreated)
        json.NewEncoder(w).Encode(map[string]interface{}{
            "id": result.LastInsertID,
        })
    }
}

func handleUserByID(w http.ResponseWriter, r *http.Request) {
    ctx := context.Background()
    
    // Extract user ID from URL (simplified)
    userID := r.URL.Path[len("/users/"):]
    
    switch r.Method {
    case http.MethodGet:
        // Get user by ID
        options := &types.QueryOptions{
            Where: map[string]interface{}{
                "id": userID,
            },
        }
        
        result, err := db.Select(ctx, 1, "users", options)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        if len(result.Rows) == 0 {
            http.Error(w, "User not found", http.StatusNotFound)
            return
        }
        
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(result.Rows[0])
        
    case http.MethodPut:
        // Update user
        var userData map[string]interface{}
        if err := json.NewDecoder(r.Body).Decode(&userData); err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }
        
        where := map[string]interface{}{
            "id": userID,
        }
        
        result, err := db.Update(ctx, 1, "users", userData, where)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "updated": result.Count,
        })
        
    case http.MethodDelete:
        // Delete user
        where := map[string]interface{}{
            "id": userID,
        }
        
        result, err := db.Delete(ctx, 1, "users", where)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "deleted": result.Count,
        })
    }
}

func intPtr(i int) *int {
    return &i
}
```

### Running the Application

```bash
go run main.go
```

Your web application backend is now running and ready to handle user management operations.

## Desktop Application

This guide shows how to integrate PGLiteDB into desktop applications for local data storage.

### Setup

```go
package main

import (
    "context"
    "log"
    "os"
    "os/user"
    "path/filepath"
    
    "github.com/guileen/pglitedb/client"
)

type App struct {
    db *client.Client
}

func NewApp() *App {
    // Get user's home directory for data storage
    usr, err := user.Current()
    if err != nil {
        log.Fatal(err)
    }
    
    dataDir := filepath.Join(usr.HomeDir, ".myapp", "data")
    
    // Create data directory if it doesn't exist
    if err := os.MkdirAll(dataDir, 0755); err != nil {
        log.Fatal(err)
    }
    
    // Initialize database
    db := client.NewClient(dataDir)
    
    return &App{
        db: db,
    }
}

func (a *App) InitializeDatabase() error {
    ctx := context.Background()
    
    // Create tables
    _, err := a.db.Query(ctx, `
        CREATE TABLE IF NOT EXISTS documents (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        return err
    }
    
    // Create indexes
    _, err = a.db.Query(ctx, `
        CREATE INDEX IF NOT EXISTS idx_documents_title ON documents(title)
    `)
    return err
}

func (a *App) CreateDocument(title, content string) (int64, error) {
    ctx := context.Background()
    
    data := map[string]interface{}{
        "title":   title,
        "content": content,
    }
    
    result, err := a.db.Insert(ctx, 1, "documents", data)
    if err != nil {
        return 0, err
    }
    
    return result.LastInsertID, nil
}

func (a *App) ListDocuments() ([]map[string]interface{}, error) {
    ctx := context.Background()
    
    options := &types.QueryOptions{
        OrderBy: []string{"created_at DESC"},
    }
    
    result, err := a.db.Select(ctx, 1, "documents", options)
    if err != nil {
        return nil, err
    }
    
    // Convert to slice of maps
    documents := make([]map[string]interface{}, len(result.Rows))
    for i, row := range result.Rows {
        doc := make(map[string]interface{})
        for j, col := range result.Columns {
            doc[col.Name] = row[j]
        }
        documents[i] = doc
    }
    
    return documents, nil
}

func main() {
    app := NewApp()
    
    if err := app.InitializeDatabase(); err != nil {
        log.Fatal(err)
    }
    
    // Create a sample document
    docID, err := app.CreateDocument("Welcome", "This is your first document!")
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Created document with ID: %d", docID)
    
    // List all documents
    documents, err := app.ListDocuments()
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Found %d documents", len(documents))
    for _, doc := range documents {
        log.Printf("Document: %+v", doc)
    }
}
```

## Mobile Application Backend

This guide shows how to use PGLiteDB as a backend for mobile applications, particularly useful for offline-first apps.

### Setup

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "sync"
    "time"
    
    "github.com/guileen/pglitedb/client"
    "github.com/guileen/pglitedb/types"
)

type MobileBackend struct {
    db       *client.Client
    syncMu   sync.Mutex
    lastSync time.Time
}

func NewMobileBackend(dbPath string) *MobileBackend {
    db := client.NewClient(dbPath)
    
    // Initialize required tables
    ctx := context.Background()
    db.Query(ctx, `
        CREATE TABLE IF NOT EXISTS sync_queue (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            operation VARCHAR(20) NOT NULL,
            record_id INTEGER,
            data JSONB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            synced BOOLEAN DEFAULT FALSE
        )
    `)
    
    return &MobileBackend{
        db:       db,
        lastSync: time.Now(),
    }
}

// SyncLocalChanges syncs local changes to remote server
func (mb *MobileBackend) SyncLocalChanges(remoteURL string) error {
    mb.syncMu.Lock()
    defer mb.syncMu.Unlock()
    
    ctx := context.Background()
    
    // Get unsynced changes
    options := &types.QueryOptions{
        Where: map[string]interface{}{
            "synced": false,
        },
        OrderBy: []string{"timestamp ASC"},
    }
    
    result, err := mb.db.Select(ctx, 1, "sync_queue", options)
    if err != nil {
        return err
    }
    
    // Process each change
    for _, row := range result.Rows {
        // Send to remote server (implementation depends on your API)
        // ...
        
        // Mark as synced
        where := map[string]interface{}{
            "id": row[0], // Assuming id is first column
        }
        data := map[string]interface{}{
            "synced": true,
        }
        
        mb.db.Update(ctx, 1, "sync_queue", data, where)
    }
    
    mb.lastSync = time.Now()
    return nil
}

// QueueChange adds a change to the sync queue
func (mb *MobileBackend) QueueChange(tableName, operation string, recordID int64, data map[string]interface{}) error {
    ctx := context.Background()
    
    queueData := map[string]interface{}{
        "table_name": tableName,
        "operation":  operation,
        "record_id":  recordID,
        "data":       data,
    }
    
    _, err := mb.db.Insert(ctx, 1, "sync_queue", queueData)
    return err
}

func main() {
    backend := NewMobileBackend("./data/mobile")
    
    // Start sync worker
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        defer ticker.Stop()
        
        for range ticker.C {
            if err := backend.SyncLocalChanges("https://api.example.com/sync"); err != nil {
                log.Printf("Sync error: %v", err)
            }
        }
    }()
    
    // HTTP handlers for mobile app communication
    http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
        if err := backend.SyncLocalChanges("https://api.example.com/sync"); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Sync completed"))
    })
    
    log.Println("Mobile backend starting on :8081")
    log.Fatal(http.ListenAndServe(":8081", nil))
}
```

## IoT Edge Computing

This guide shows how to use PGLiteDB in IoT edge computing scenarios for local data processing and storage.

### Setup

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"
    
    "github.com/guileen/pglitedb/client"
    "github.com/guileen/pglitedb/types"
)

type IoTDevice struct {
    db     *client.Client
    deviceID string
}

func NewIoTDevice(deviceID, dbPath string) *IoTDevice {
    db := client.NewClient(dbPath)
    
    // Initialize telemetry table
    ctx := context.Background()
    db.Query(ctx, `
        CREATE TABLE IF NOT EXISTS telemetry (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) NOT NULL,
            sensor_type VARCHAR(50) NOT NULL,
            value NUMERIC NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed BOOLEAN DEFAULT FALSE
        )
    `)
    
    // Create indexes for performance
    db.Query(ctx, `
        CREATE INDEX IF NOT EXISTS idx_telemetry_device_timestamp ON telemetry(device_id, timestamp)
    `)
    
    return &IoTDevice{
        db:       db,
        deviceID: deviceID,
    }
}

func (d *IoTDevice) RecordTelemetry(sensorType string, value float64) error {
    ctx := context.Background()
    
    data := map[string]interface{}{
        "device_id":    d.deviceID,
        "sensor_type":  sensorType,
        "value":        value,
    }
    
    _, err := d.db.Insert(ctx, 1, "telemetry", data)
    return err
}

func (d *IoTDevice) GetRecentTelemetry(hours int) ([]map[string]interface{}, error) {
    ctx := context.Background()
    
    // Calculate time threshold
    threshold := time.Now().Add(-time.Duration(hours) * time.Hour)
    
    options := &types.QueryOptions{
        Where: map[string]interface{}{
            "device_id": d.deviceID,
            "timestamp": map[string]interface{}{
                "$gt": threshold.Format("2006-01-02 15:04:05"),
            },
        },
        OrderBy: []string{"timestamp DESC"},
    }
    
    result, err := d.db.Select(ctx, 1, "telemetry", options)
    if err != nil {
        return nil, err
    }
    
    // Convert to slice of maps
    telemetry := make([]map[string]interface{}, len(result.Rows))
    for i, row := range result.Rows {
        t := make(map[string]interface{})
        for j, col := range result.Columns {
            t[col.Name] = row[j]
        }
        telemetry[i] = t
    }
    
    return telemetry, nil
}

func (d *IoTDevice) ProcessData() error {
    ctx := context.Background()
    
    // Get unprocessed telemetry
    options := &types.QueryOptions{
        Where: map[string]interface{}{
            "device_id":  d.deviceID,
            "processed":  false,
        },
    }
    
    result, err := d.db.Select(ctx, 1, "telemetry", options)
    if err != nil {
        return err
    }
    
    // Process each record (simple aggregation example)
    for _, row := range result.Rows {
        // Perform local analytics
        // ...
        
        // Mark as processed
        where := map[string]interface{}{
            "id": row[0], // Assuming id is first column
        }
        data := map[string]interface{}{
            "processed": true,
        }
        
        d.db.Update(ctx, 1, "telemetry", data, where)
    }
    
    return nil
}

func main() {
    device := NewIoTDevice("device-001", "./data/iot")
    
    // Simulate data collection
    go func() {
        ticker := time.NewTicker(5 * time.Second)
        defer ticker.Stop()
        
        sensors := []string{"temperature", "humidity", "pressure"}
        
        for range ticker.C {
            for _, sensor := range sensors {
                value := float64(time.Now().UnixNano()%1000) / 10.0
                if err := device.RecordTelemetry(sensor, value); err != nil {
                    log.Printf("Error recording telemetry: %v", err)
                }
            }
        }
    }()
    
    // Process data periodically
    go func() {
        ticker := time.NewTicker(1 * time.Minute)
        defer ticker.Stop()
        
        for range ticker.C {
            if err := device.ProcessData(); err != nil {
                log.Printf("Error processing data: %v", err)
            }
        }
    }()
    
    // Keep application running
    select {}
}
```

## Data Analytics Pipeline

This guide shows how to use PGLiteDB in data analytics pipelines for ETL (Extract, Transform, Load) operations.

### Setup

```go
package main

import (
    "context"
    "encoding/csv"
    "log"
    "os"
    "strconv"
    "time"
    
    "github.com/guileen/pglitedb/client"
    "github.com/guileen/pglitedb/types"
)

type AnalyticsPipeline struct {
    db *client.Client
}

func NewAnalyticsPipeline(dbPath string) *AnalyticsPipeline {
    db := client.NewClient(dbPath)
    
    ctx := context.Background()
    
    // Raw data table
    db.Query(ctx, `
        CREATE TABLE IF NOT EXISTS raw_events (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            user_id INTEGER,
            value NUMERIC,
            metadata JSONB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    
    // Aggregated data table
    db.Query(ctx, `
        CREATE TABLE IF NOT EXISTS daily_metrics (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            count INTEGER NOT NULL,
            sum_value NUMERIC,
            avg_value NUMERIC,
            UNIQUE(date, event_type)
        )
    `)
    
    return &AnalyticsPipeline{
        db: db,
    }
}

func (ap *AnalyticsPipeline) LoadCSVData(filePath string) error {
    ctx := context.Background()
    
    file, err := os.Open(filePath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    reader := csv.NewReader(file)
    records, err := reader.ReadAll()
    if err != nil {
        return err
    }
    
    // Skip header row
    for i, record := range records {
        if i == 0 {
            continue
        }
        
        // Parse CSV data
        userID, _ := strconv.Atoi(record[1])
        value, _ := strconv.ParseFloat(record[2], 64)
        
        data := map[string]interface{}{
            "event_type": record[0],
            "user_id":    userID,
            "value":      value,
            "metadata":   "{}", // Simplified
        }
        
        _, err := ap.db.Insert(ctx, 1, "raw_events", data)
        if err != nil {
            log.Printf("Error inserting record: %v", err)
        }
    }
    
    return nil
}

func (ap *AnalyticsPipeline) GenerateDailyMetrics(targetDate time.Time) error {
    ctx := context.Background()
    
    // Format date for SQL
    dateStr := targetDate.Format("2006-01-02")
    
    // Aggregate data using SQL
    query := `
        INSERT INTO daily_metrics (date, event_type, count, sum_value, avg_value)
        SELECT 
            DATE(timestamp) as date,
            event_type,
            COUNT(*) as count,
            SUM(value) as sum_value,
            AVG(value) as avg_value
        FROM raw_events 
        WHERE DATE(timestamp) = $1
        GROUP BY DATE(timestamp), event_type
        ON CONFLICT (date, event_type) DO UPDATE SET
            count = EXCLUDED.count,
            sum_value = EXCLUDED.sum_value,
            avg_value = EXCLUDED.avg_value
    `
    
    _, err := ap.db.Query(ctx, query, dateStr)
    return err
}

func (ap *AnalyticsPipeline) GetMetrics(startDate, endDate time.Time) ([]map[string]interface{}, error) {
    ctx := context.Background()
    
    options := &types.QueryOptions{
        Where: map[string]interface{}{
            "date": map[string]interface{}{
                "$gte": startDate.Format("2006-01-02"),
                "$lte": endDate.Format("2006-01-02"),
            },
        },
        OrderBy: []string{"date ASC", "event_type ASC"},
    }
    
    result, err := ap.db.Select(ctx, 1, "daily_metrics", options)
    if err != nil {
        return nil, err
    }
    
    // Convert to slice of maps
    metrics := make([]map[string]interface{}, len(result.Rows))
    for i, row := range result.Rows {
        m := make(map[string]interface{})
        for j, col := range result.Columns {
            m[col.Name] = row[j]
        }
        metrics[i] = m
    }
    
    return metrics, nil
}

func main() {
    pipeline := NewAnalyticsPipeline("./data/analytics")
    
    // Load sample data
    if err := pipeline.LoadCSVData("./sample_data.csv"); err != nil {
        log.Printf("Error loading data: %v", err)
    }
    
    // Generate metrics for yesterday
    yesterday := time.Now().AddDate(0, 0, -1)
    if err := pipeline.GenerateDailyMetrics(yesterday); err != nil {
        log.Printf("Error generating metrics: %v", err)
    }
    
    // Get metrics for the last week
    endDate := time.Now()
    startDate := endDate.AddDate(0, 0, -7)
    
    metrics, err := pipeline.GetMetrics(startDate, endDate)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Generated %d metrics records", len(metrics))
    for _, metric := range metrics {
        log.Printf("Metric: %+v", metric)
    }
}
```

## Conclusion

These quick start guides provide practical examples for using PGLiteDB in various scenarios. Each guide demonstrates core concepts and best practices specific to that use case. You can adapt these examples to fit your specific requirements and extend them with additional functionality as needed.