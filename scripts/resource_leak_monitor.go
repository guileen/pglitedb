package main

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
	"github.com/guileen/pglitedb/types"
)

var (
	duration   = flag.Duration("duration", 10*time.Minute, "Duration to run the resource leak monitoring")
	workers    = flag.Int("workers", 15, "Number of concurrent workers")
	batchSize  = flag.Int("batch-size", 100, "Batch size for operations")
	dbPath     = flag.String("db-path", "/tmp/pglitedb-resource-leak-monitor", "Database path")
	profileDir = flag.String("profile-dir", "./resource_profiles", "Directory to store profiling data")
)

type ResourceMonitorStats struct {
	Operations int64
	Errors     int64
	StartTime  time.Time
	
	// Resource tracking
	MemoryStats    []runtime.MemStats
	GoroutineCount []int
	Timestamps     []time.Time
	
	// Leak detection thresholds
	MemoryGrowthThreshold   uint64
	GoroutineGrowthThreshold int
}

func (s *ResourceMonitorStats) OpsPerSecond() float64 {
	if s.Operations == 0 {
		return 0
	}
	duration := time.Since(s.StartTime)
	return float64(s.Operations) / duration.Seconds()
}

func (s *ResourceMonitorStats) ErrorRate() float64 {
	if s.Operations == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Operations) * 100
}

func (s *ResourceMonitorStats) SampleResources() {
	// Memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	s.MemoryStats = append(s.MemoryStats, memStats)
	
	// Goroutines
	goroutines := runtime.NumGoroutine()
	s.GoroutineCount = append(s.GoroutineCount, goroutines)
	
	// Timestamps
	s.Timestamps = append(s.Timestamps, time.Now())
}

func (s *ResourceMonitorStats) PrintResourceStats() {
	if len(s.MemoryStats) < 2 {
		return
	}
	
	current := s.MemoryStats[len(s.MemoryStats)-1]
	previous := s.MemoryStats[len(s.MemoryStats)-2]
	
	fmt.Printf("=== RESOURCE USAGE ===\n")
	fmt.Printf("Alloc: %d KB -> %d KB (%+d KB)\n", 
		previous.Alloc/1024, current.Alloc/1024, 
		int64(current.Alloc-previous.Alloc)/1024)
	fmt.Printf("TotalAlloc: %d MB -> %d MB (%+d MB)\n",
		previous.TotalAlloc/1024/1024, current.TotalAlloc/1024/1024,
		int64(current.TotalAlloc-previous.TotalAlloc)/1024/1024)
	fmt.Printf("Sys: %d MB -> %d MB (%+d MB)\n",
		previous.Sys/1024/1024, current.Sys/1024/1024,
		int64(current.Sys-previous.Sys)/1024/1024)
	fmt.Printf("NumGC: %d -> %d (+%d)\n",
		previous.NumGC, current.NumGC,
		current.NumGC-previous.NumGC)
	fmt.Printf("Goroutines: %d -> %d (%+d)\n",
		s.GoroutineCount[len(s.GoroutineCount)-2], s.GoroutineCount[len(s.GoroutineCount)-1],
		s.GoroutineCount[len(s.GoroutineCount)-1]-s.GoroutineCount[len(s.GoroutineCount)-2])
	fmt.Printf("=====================\n")
}

func (s *ResourceMonitorStats) DetectLeaks() []string {
	var leaks []string
	
	if len(s.MemoryStats) < 3 {
		return leaks // Need at least 3 samples for trend analysis
	}
	
	// Analyze memory growth trend
	initialAlloc := s.MemoryStats[0].Alloc
	finalAlloc := s.MemoryStats[len(s.MemoryStats)-1].Alloc
	
	// Check if memory is consistently growing
	memoryGrowth := finalAlloc - initialAlloc
	if memoryGrowth > s.MemoryGrowthThreshold {
		leaks = append(leaks, fmt.Sprintf("Potential memory leak: +%d KB growth", memoryGrowth/1024))
	}
	
	// Analyze goroutine growth trend
	initialGoroutines := s.GoroutineCount[0]
	finalGoroutines := s.GoroutineCount[len(s.GoroutineCount)-1]
	
	goroutineGrowth := finalGoroutines - initialGoroutines
	if goroutineGrowth > s.GoroutineGrowthThreshold {
		leaks = append(leaks, fmt.Sprintf("Potential goroutine leak: +%d goroutines", goroutineGrowth))
	}
	
	// Check for GC pressure - too many GC cycles relative to operations
	initialGC := s.MemoryStats[0].NumGC
	finalGC := s.MemoryStats[len(s.MemoryStats)-1].NumGC
	gcCount := finalGC - initialGC
	
	// If we have a lot of GC cycles for relatively few operations, it might indicate memory pressure
	if s.Operations > 0 && gcCount > 0 {
		gcPerOperation := float64(gcCount) / float64(s.Operations)
		if gcPerOperation > 0.1 { // More than 1 GC per 10 operations
			leaks = append(leaks, fmt.Sprintf("High GC frequency: %.2f GCs per operation", gcPerOperation))
		}
	}
	
	return leaks
}

func (s *ResourceMonitorStats) GenerateReport() {
	fmt.Printf("\n=== RESOURCE LEAK DETECTION REPORT ===\n")
	
	if len(s.MemoryStats) == 0 {
		fmt.Printf("No resource data collected.\n")
		return
	}
	
	initial := s.MemoryStats[0]
	final := s.MemoryStats[len(s.MemoryStats)-1]
	
	fmt.Printf("Test Duration: %v\n", time.Since(s.StartTime))
	fmt.Printf("Total Operations: %d\n", s.Operations)
	fmt.Printf("Operations per second: %.2f\n", s.OpsPerSecond())
	fmt.Printf("Total Errors: %d\n", s.Errors)
	fmt.Printf("Error rate: %.2f%%\n", s.ErrorRate())
	
	fmt.Printf("\n--- Memory Usage ---\n")
	fmt.Printf("Initial Alloc: %d KB\n", initial.Alloc/1024)
	fmt.Printf("Final Alloc: %d KB\n", final.Alloc/1024)
	fmt.Printf("Memory Growth: %+d KB\n", int64(final.Alloc-initial.Alloc)/1024)
	fmt.Printf("Initial Sys: %d MB\n", initial.Sys/1024/1024)
	fmt.Printf("Final Sys: %d MB\n", final.Sys/1024/1024)
	fmt.Printf("System Memory Growth: %+d MB\n", int64(final.Sys-initial.Sys)/1024/1024)
	fmt.Printf("Total GC Runs: %d\n", final.NumGC-initial.NumGC)
	fmt.Printf("Total Alloc: %d MB\n", final.TotalAlloc/1024/1024)
	
	fmt.Printf("\n--- Goroutine Count ---\n")
	if len(s.GoroutineCount) > 0 {
		fmt.Printf("Initial Goroutines: %d\n", s.GoroutineCount[0])
		fmt.Printf("Final Goroutines: %d\n", s.GoroutineCount[len(s.GoroutineCount)-1])
		fmt.Printf("Goroutine Growth: %+d\n", s.GoroutineCount[len(s.GoroutineCount)-1]-s.GoroutineCount[0])
	}
	
	// Leak detection summary
	leaks := s.DetectLeaks()
	if len(leaks) > 0 {
		fmt.Printf("\n⚠️  WARNING: Potential resource leaks detected!\n")
		for _, leak := range leaks {
			fmt.Printf("  - %s\n", leak)
		}
		fmt.Printf("  - Please review profiles for detailed analysis\n")
	} else {
		fmt.Printf("\n✅ No significant resource leaks detected during monitoring period.\n")
	}
	
	fmt.Printf("\n=== END REPORT ===\n")
}

func main() {
	flag.Parse()

	log.Printf("Starting resource leak monitoring for %v with %d workers", *duration, *workers)
	log.Printf("Database path: %s", *dbPath)
	log.Printf("Profile directory: %s", *profileDir)

	// Create profile directory if it doesn't exist
	if err := os.MkdirAll(*profileDir, 0755); err != nil {
		log.Fatalf("Failed to create profile directory: %v", err)
	}

	// Remove existing database if it exists
	os.RemoveAll(*dbPath)

	// Initialize profiler
	prof := profiling.NewProfiler()

	// Start CPU profiling
	cpuProfilePath := fmt.Sprintf("%s/cpu.prof", *profileDir)
	if err := prof.StartCPUProfile(cpuProfilePath); err != nil {
		log.Printf("Warning: Could not start CPU profiling: %v", err)
	} else {
		log.Printf("CPU profiling started: %s", cpuProfilePath)
	}

	// Enable allocation profiling
	if err := prof.StartAllocProfile(""); err != nil {
		log.Printf("Warning: Could not start allocation profiling: %v", err)
	}

	// Enable block profiling
	if err := prof.StartBlockProfile(1); err != nil {
		log.Printf("Warning: Could not start block profiling: %v", err)
	}

	// Enable mutex profiling
	if err := prof.StartMutexProfile(1); err != nil {
		log.Printf("Warning: Could not start mutex profiling: %v", err)
	}

	db := client.NewClient(*dbPath)
	ctx := context.Background()
	tenantID := int64(1)
	tableName := "monitor_table"

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

	stats := &ResourceMonitorStats{
		StartTime: time.Now(),
		MemoryGrowthThreshold:   50 * 1024 * 1024, // 50MB
		GoroutineGrowthThreshold: 50,               // 50 goroutines
	}
	stats.SampleResources() // Initial resource sample

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, db, tenantID, tableName, stats, &wg)
	}

	// Start stats reporter
	go reportStats(stats)

	// Start resource monitoring
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-doneChan:
				return
			case <-ticker.C:
				stats.SampleResources()
				stats.PrintResourceStats()
				
	// Check for potential leaks
				if leaks := stats.DetectLeaks(); len(leaks) > 0 {
					log.Printf("⚠️  Potential resource leaks detected:")
					for _, leak := range leaks {
						log.Printf("  - %s", leak)
					}
				}
			}
		}
	}()

	// Wait for duration or interrupt signal
	timer := time.NewTimer(*duration)
	select {
	case <-timer.C:
		log.Println("Monitoring duration completed")
	case <-sigChan:
		log.Println("Received interrupt signal, stopping monitoring")
	}

	// Signal workers to stop
	close(doneChan)

	// Wait for all workers to finish
	wg.Wait()

	// Final resource sampling
	stats.SampleResources()

	// Print final report
	log.Printf("=== MONITORING COMPLETE ===")
	stats.GenerateReport()

	// Stop profiling
	if prof.IsProfileActive(profiling.CPUProfile) {
		if err := prof.StopCPUProfile(); err != nil {
			log.Printf("Warning: Could not stop CPU profiling: %v", err)
		} else {
			log.Printf("CPU profiling stopped: %s", cpuProfilePath)
		}
	}

	// Save memory profile
	memProfilePath := fmt.Sprintf("%s/mem.prof", *profileDir)
	if err := prof.StartMemProfile(memProfilePath); err != nil {
		log.Printf("Warning: Could not save memory profile: %v", err)
	} else {
		log.Printf("Memory profile saved: %s", memProfilePath)
	}

	// Save allocation profile
	allocProfilePath := fmt.Sprintf("%s/allocs.prof", *profileDir)
	if err := prof.StopAllocProfile(allocProfilePath); err != nil {
		log.Printf("Warning: Could not save allocation profile: %v", err)
	} else {
		log.Printf("Allocation profile saved: %s", allocProfilePath)
	}

	// Save block profile
	blockProfilePath := fmt.Sprintf("%s/block.prof", *profileDir)
	if err := prof.StopBlockProfile(blockProfilePath); err != nil {
		log.Printf("Warning: Could not save block profile: %v", err)
	} else {
		log.Printf("Block profile saved: %s", blockProfilePath)
	}

	// Save mutex profile
	mutexProfilePath := fmt.Sprintf("%s/mutex.prof", *profileDir)
	if err := prof.StopMutexProfile(mutexProfilePath); err != nil {
		log.Printf("Warning: Could not save mutex profile: %v", err)
	} else {
		log.Printf("Mutex profile saved: %s", mutexProfilePath)
	}

	// Save goroutine profile
	goroutineProfilePath := fmt.Sprintf("%s/goroutine.prof", *profileDir)
	if err := prof.WriteGoroutineProfile(goroutineProfilePath); err != nil {
		log.Printf("Warning: Could not save goroutine profile: %v", err)
	} else {
		log.Printf("Goroutine profile saved: %s", goroutineProfilePath)
	}

	log.Printf("=== PROFILING SUMMARY ===")
	log.Printf("Profiles saved to: %s", *profileDir)
	log.Printf("To analyze profiles, use:")
	log.Printf("  go tool pprof %s", cpuProfilePath)
	log.Printf("  go tool pprof -http=:8080 %s", memProfilePath)
}

var doneChan = make(chan struct{})

func worker(ctx context.Context, db *client.Client, tenantID int64, tableName string, stats *ResourceMonitorStats, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-doneChan:
			return
		case <-ctx.Done():
			return
		default:
			// Perform a batch operation
			err := performBatchOperation(ctx, db, tenantID, tableName)
			if err != nil {
				log.Printf("Worker error: %v", err)
				stats.Errors++
			}
			stats.Operations++
		}
	}
}

func performBatchOperation(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Randomly choose operation type
	switch rand.Intn(4) {
	case 0: // Insert
		return performInsert(ctx, db, tenantID, tableName)
	case 1: // Select
		return performSelect(ctx, db, tenantID, tableName)
	case 2: // Update
		return performUpdate(ctx, db, tenantID, tableName)
	case 3: // Delete
		return performDelete(ctx, db, tenantID, tableName)
	default:
		return nil
	}
}

func performInsert(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Insert batch of records
	records := make([]map[string]interface{}, *batchSize)
	for i := 0; i < *batchSize; i++ {
		records[i] = map[string]interface{}{
			"id":     rand.Int(),
			"name":   fmt.Sprintf("User%d", rand.Int()),
			"email":  fmt.Sprintf("user%d@example.com", rand.Int()),
			"age":    rand.Intn(60) + 20,
			"score":  rand.Float64() * 100,
			"active": rand.Intn(2) == 0,
		}
	}

	for _, record := range records {
		_, err := db.Insert(ctx, tenantID, tableName, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func performSelect(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Select with various conditions
	options := &types.QueryOptions{
		Limit: intPtr(rand.Intn(100) + 10),
	}

	// Randomly add conditions
	if rand.Intn(2) == 0 {
		options.Where = map[string]interface{}{
			"age": rand.Int63n(60) + 20,
		}
	}

	if rand.Intn(2) == 0 {
		options.OrderBy = []string{"age", "score"}
	}

	_, err := db.Select(ctx, tenantID, tableName, options)
	return err
}

func performUpdate(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Update records with random conditions
	updates := map[string]interface{}{
		"age":   rand.Intn(60) + 20,
		"score": rand.Float64() * 100,
	}

	conditions := map[string]interface{}{
		"active": rand.Intn(2) == 0,
	}

	_, err := db.Update(ctx, tenantID, tableName, updates, conditions)
	return err
}

func performDelete(ctx context.Context, db *client.Client, tenantID int64, tableName string) error {
	// Delete records with random conditions
	conditions := map[string]interface{}{
		"active": rand.Intn(2) == 0,
	}

	_, err := db.Delete(ctx, tenantID, tableName, conditions)
	return err
}

func reportStats(stats *ResourceMonitorStats) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-doneChan:
			return
		case <-ticker.C:
			log.Printf("Stats - Ops: %d, Errors: %d, Ops/sec: %.2f, Error Rate: %.2f%%",
				stats.Operations, stats.Errors, stats.OpsPerSecond(), stats.ErrorRate())
		}
	}
}

func intPtr(i int) *int {
	return &i
}