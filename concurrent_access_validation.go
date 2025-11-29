package main_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/guileen/pglitedb/client"
	"github.com/guileen/pglitedb/profiling"
)

type ConcurrencyLevel struct {
	Workers int
	Name    string
}

type ValidationStats struct {
	ConcurrencyLevel string
	Operations       int64
	Errors           int64
	StartTime        time.Time
	
	// GC statistics
	LastGCStats *runtime.MemStats
	CurrentGCStats *runtime.MemStats
	
	// Contention metrics
	BlockedGoroutines int64
	MutexWaitTime     time.Duration
}

func (s *ValidationStats) OpsPerSecond() float64 {
	if s.Operations == 0 {
		return 0
	}
	duration := time.Since(s.StartTime)
	return float64(s.Operations) / duration.Seconds()
}

func (s *ValidationStats) ErrorRate() float64 {
	if s.Operations == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Operations) * 100
}

func (s *ValidationStats) UpdateGCStats() {
	s.LastGCStats = s.CurrentGCStats
	s.CurrentGCStats = &runtime.MemStats{}
	runtime.ReadMemStats(s.CurrentGCStats)
}

func (s *ValidationStats) PrintDetailedStats() {
	if s.LastGCStats == nil || s.CurrentGCStats == nil {
		return
	}
	
	fmt.Printf("=== DETAILED STATISTICS FOR %s ===\n", s.ConcurrencyLevel)
	fmt.Printf("Operations per second: %.2f\n", s.OpsPerSecond())
	fmt.Printf("Error rate: %.2f%%\n", s.ErrorRate())
	fmt.Printf("Alloc: %d KB -> %d KB (%+d KB)\n", 
		s.LastGCStats.Alloc/1024, s.CurrentGCStats.Alloc/1024, 
		int64(s.CurrentGCStats.Alloc-s.LastGCStats.Alloc)/1024)
	fmt.Printf("TotalAlloc: %d MB -> %d MB (%+d MB)\n",
		s.LastGCStats.TotalAlloc/1024/1024, s.CurrentGCStats.TotalAlloc/1024/1024,
		int64(s.CurrentGCStats.TotalAlloc-s.LastGCStats.TotalAlloc)/1024/1024)
	fmt.Printf("NumGC: %d -> %d (+%d)\n",
		s.LastGCStats.NumGC, s.CurrentGCStats.NumGC,
		s.CurrentGCStats.NumGC-s.LastGCStats.NumGC)
	fmt.Printf("GCCPUFraction: %.2f%% -> %.2f%%\n",
		s.LastGCStats.GCCPUFraction*100, s.CurrentGCStats.GCCPUFraction*100)
	fmt.Printf("=========================\n")
}

func concurrentMain() {
	// Define local flags for this command
	cmd := flag.NewFlagSet("concurrent-validation", flag.ExitOnError)
	dbPath := cmd.String("db-path", "/tmp/pglitedb-concurrent-validation", "Database path")
	profileDir := cmd.String("profile-dir", "./concurrent_profiles", "Directory to store profiling data")
	duration := cmd.Duration("duration", 2*time.Minute, "Duration to run each concurrency level")
	
	cmd.Parse(os.Args[2:])

	log.Printf("Starting concurrent access validation")
	log.Printf("Database path: %s", *dbPath)
	log.Printf("Profile directory: %s", *profileDir)

	// Create profile directory if it doesn't exist
	if err := os.MkdirAll(*profileDir, 0755); err != nil {
		log.Fatalf("Failed to create profile directory: %v", err)
	}

	// Define concurrency levels to test
	concurrencyLevels := []ConcurrencyLevel{
		{Workers: 5, Name: "Low_Concurrency"},
		{Workers: 10, Name: "Medium_Concurrency"},
		{Workers: 20, Name: "High_Concurrency"},
	}

	// Run validation for each concurrency level
	for _, level := range concurrencyLevels {
		log.Printf("Running validation for %s (%d workers)", level.Name, level.Workers)
		runConcurrentValidation(level, dbPath, profileDir, duration)
	}

	log.Printf("Concurrent access validation completed")
}

func runConcurrentValidation(level ConcurrencyLevel, dbPath, profileDir *string, duration *time.Duration) {
	// Remove existing database if it exists
	os.RemoveAll(*dbPath)

	// Initialize profiler
	prof := profiling.NewProfiler()

	// Create profile directory for this level
	levelProfileDir := fmt.Sprintf("%s/%s", *profileDir, level.Name)
	if err := os.MkdirAll(levelProfileDir, 0755); err != nil {
		log.Printf("Warning: Failed to create level profile directory: %v", err)
		levelProfileDir = *profileDir
	}

	// Start all profiling types
	concurrentStartAllProfiles(prof, levelProfileDir, level.Name)

	db := client.NewClient(*dbPath)
	ctx := context.Background()
	tenantID := int64(1)
	tableName := "validation_table"

	// Create table implicitly by inserting first record
	firstRecord := map[string]interface{}{
		"id":     1,
		"name":   "InitialUser",
		"email":  "initial@example.com",
		"age":    25,
		"score":  50.0,
		"active": true,
	}

	_, err := db.Insert(ctx, tenantID, tableName, firstRecord)
	if err != nil {
		log.Fatalf("Failed to create table implicitly: %v", err)
	}

	stats := &ValidationStats{
		ConcurrencyLevel: level.Name,
		StartTime:       time.Now(),
	}
	stats.UpdateGCStats() // Initialize GC stats

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	var wg sync.WaitGroup
	doneChan := make(chan struct{})
	
	for i := 0; i < level.Workers; i++ {
		wg.Add(1)
		go concurrentAccessWorker(ctx, db, tenantID, tableName, stats, &wg, doneChan)
	}

	// Wait for duration or interrupt signal
	timer := time.NewTimer(*duration)
	select {
	case <-timer.C:
		log.Printf("Validation duration completed for %s", level.Name)
	case <-sigChan:
		log.Printf("Received interrupt signal, stopping validation for %s", level.Name)
	}

	// Signal workers to stop
	close(doneChan)

	// Wait for all workers to finish
	wg.Wait()

	// Print final stats
	log.Printf("=== RESULTS FOR %s ===", level.Name)
	log.Printf("Workers: %d", level.Workers)
	log.Printf("Total Operations: %d", stats.Operations)
	log.Printf("Total Errors: %d", stats.Errors)
	log.Printf("Operations per second: %.2f", stats.OpsPerSecond())
	log.Printf("Error rate: %.2f%%", stats.ErrorRate())
	log.Printf("Validation completed in %v", time.Since(stats.StartTime))

	// Final GC stats
	stats.UpdateGCStats()
	stats.PrintDetailedStats()

	// Stop all profiling
	concurrentStopAllProfiles(prof, levelProfileDir, level.Name)

	log.Printf("Profiles for %s saved to: %s", level.Name, levelProfileDir)
	log.Printf("To analyze profiles, use:")
	log.Printf("  go tool pprof %s/cpu_%s.prof", levelProfileDir, level.Name)
	log.Printf("  go tool pprof -http=:8080 %s/mem_%s.prof", levelProfileDir, level.Name)
}

func concurrentStartAllProfiles(prof *profiling.Profiler, profileDir, levelName string) {
	// Start CPU profiling
	cpuProfilePath := fmt.Sprintf("%s/cpu_%s.prof", profileDir, levelName)
	if err := prof.StartCPUProfile(cpuProfilePath); err != nil {
		log.Printf("Warning: Could not start CPU profiling for %s: %v", levelName, err)
	} else {
		log.Printf("CPU profiling started for %s: %s", levelName, cpuProfilePath)
	}

	// Enable allocation profiling
	if err := prof.StartAllocProfile(""); err != nil {
		log.Printf("Warning: Could not start allocation profiling for %s: %v", levelName, err)
	}

	// Enable block profiling
	if err := prof.StartBlockProfile(1); err != nil {
		log.Printf("Warning: Could not start block profiling for %s: %v", levelName, err)
	}

	// Enable mutex profiling
	if err := prof.StartMutexProfile(1); err != nil {
		log.Printf("Warning: Could not start mutex profiling for %s: %v", levelName, err)
	}
}

func concurrentStopAllProfiles(prof *profiling.Profiler, profileDir, levelName string) {
	// Stop CPU profiling
	if prof.IsProfileActive(profiling.CPUProfile) {
		if err := prof.StopCPUProfile(); err != nil {
			log.Printf("Warning: Could not stop CPU profiling for %s: %v", levelName, err)
		} else {
			log.Printf("CPU profiling stopped for %s", levelName)
		}
	}

	// Save memory profile
	memProfilePath := fmt.Sprintf("%s/mem_%s.prof", profileDir, levelName)
	if err := prof.StartMemProfile(memProfilePath); err != nil {
		log.Printf("Warning: Could not save memory profile for %s: %v", levelName, err)
	} else {
		log.Printf("Memory profile saved for %s: %s", levelName, memProfilePath)
	}

	// Save allocation profile
	allocProfilePath := fmt.Sprintf("%s/allocs_%s.prof", profileDir, levelName)
	if err := prof.StopAllocProfile(allocProfilePath); err != nil {
		log.Printf("Warning: Could not save allocation profile for %s: %v", levelName, err)
	} else {
		log.Printf("Allocation profile saved for %s: %s", levelName, allocProfilePath)
	}

	// Save block profile
	blockProfilePath := fmt.Sprintf("%s/block_%s.prof", profileDir, levelName)
	if err := prof.StopBlockProfile(blockProfilePath); err != nil {
		log.Printf("Warning: Could not save block profile for %s: %v", levelName, err)
	} else {
		log.Printf("Block profile saved for %s: %s", levelName, blockProfilePath)
	}

	// Save mutex profile
	mutexProfilePath := fmt.Sprintf("%s/mutex_%s.prof", profileDir, levelName)
	if err := prof.StopMutexProfile(mutexProfilePath); err != nil {
		log.Printf("Warning: Could not save mutex profile for %s: %v", levelName, err)
	} else {
		log.Printf("Mutex profile saved for %s: %s", levelName, mutexProfilePath)
	}

	// Save goroutine profile
	goroutineProfilePath := fmt.Sprintf("%s/goroutine_%s.prof", profileDir, levelName)
	if err := prof.WriteGoroutineProfile(goroutineProfilePath); err != nil {
		log.Printf("Warning: Could not save goroutine profile for %s: %v", levelName, err)
	} else {
		log.Printf("Goroutine profile saved for %s: %s", levelName, goroutineProfilePath)
	}
}

func concurrentAccessWorker(ctx context.Context, db *client.Client, tenantID int64, tableName string, stats *ValidationStats, wg *sync.WaitGroup, doneChan chan struct{}) {
	defer wg.Done()

	for {
		select {
		case <-doneChan:
			return
		case <-ctx.Done():
			return
		default:
			// Perform concurrent operations
			err := concurrentPerformConcurrentOperations(ctx, db, tenantID, tableName)
			if err != nil {
				log.Printf("Worker error in %s: %v", stats.ConcurrencyLevel, err)
				stats.Errors++
			}
			stats.Operations++
		}
	}
}

func concurrentPerformConcurrentOperations(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Perform a mix of operations that might cause contention
	switch rand.Intn(5) {
	case 0: // Heavy insert operation
		return concurrentPerformHeavyInsert(ctx, db, tenantID, tableName)
	case 1: // Concurrent select operations
		return concurrentPerformConcurrentSelects(ctx, db, tenantID, tableName)
	case 2: // Update operations
		return concurrentPerformConcurrentUpdates(ctx, db, tenantID, tableName)
	case 3: // Mixed operations
		return concurrentPerformMixedOperations(ctx, db, tenantID, tableName)
	case 4: // High contention operations
		return concurrentPerformHighContentionOps(ctx, db, tenantID, tableName)
	default:
		return nil
	}
}

func concurrentPerformHeavyInsert(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Insert multiple records to create load
	for i := 0; i < 10; i++ {
		record := map[string]interface{}{
			"id":     rand.Int63(),
			"name":   fmt.Sprintf("User%d_%d", rand.Int(), time.Now().UnixNano()),
			"email":  fmt.Sprintf("user%d_%d@example.com", rand.Int(), time.Now().UnixNano()),
			"age":    rand.Intn(60) + 20,
			"score":  rand.Float64() * 100,
			"active": rand.Intn(2) == 0,
		}
		
		_, err := db.Insert(ctx, tenantID, tableName, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func concurrentPerformConcurrentSelects(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Perform multiple selects that might contend for shared resources
	for i := 0; i < 5; i++ {
		options := &client.QueryOptions{
			Limit: concurrentIntPtr(rand.Intn(50) + 10),
			Where: map[string]interface{}{
				"age": rand.Int63n(60) + 20,
			},
		}
		
		_, err := db.Select(ctx, tenantID, tableName, options)
		if err != nil {
			return err
		}
		
		// Small delay to increase chance of contention
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))
	}
	return nil
}

func concurrentPerformConcurrentUpdates(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Perform updates that might cause contention
	updates := map[string]interface{}{
		"score": rand.Float64() * 100,
		"age":   rand.Intn(60) + 20,
	}

	conditions := map[string]interface{}{
		"active": rand.Intn(2) == 0,
	}

	// Multiple updates
	for i := 0; i < 3; i++ {
		_, err := db.Update(ctx, tenantID, tableName, updates, conditions)
		if err != nil {
			return err
		}
	}

	return nil
}

func concurrentPerformMixedOperations(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Mix of different operations
	operations := []func(context.Context, *client.Client, int64, string) error{
		concurrentPerformHeavyInsert,
		concurrentPerformConcurrentSelects,
		concurrentPerformConcurrentUpdates,
	}

	// Execute random operation
	op := operations[rand.Intn(len(operations))]
	return op(ctx, db, tenantID, tableName)
}

func concurrentPerformHighContentionOps(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Operations designed to create high contention
	// Update a specific record that all workers might try to access
	updates := map[string]interface{}{
		"last_accessed": time.Now().UnixNano(),
	}

	conditions := map[string]interface{}{
		"id": 1, // The initial record that all workers will try to update
	}

	_, err := db.Update(ctx, tenantID, tableName, updates, conditions)
	return err
}

func concurrentIntPtr(i int) *int {
	return &i
}