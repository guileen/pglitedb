package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guileen/pglitedb/catalog"
)

func main() {
	// Create a stats collector
	statsCollector := catalog.NewStatsCollector(nil)
	
	// Example usage of the stats collector
	ctx := context.Background()
	tableID := uint64(1)
	
	// Collect table statistics
	fmt.Println("Collecting table statistics...")
	tableStats, err := statsCollector.CollectTableStats(ctx, tableID)
	if err != nil {
		log.Printf("Error collecting table stats: %v", err)
		return
	}
	fmt.Printf("Collected table stats for table %d\n", tableStats.RelID)
	fmt.Printf("Table name: %s\n", tableStats.RelName)
	fmt.Printf("Last updated: %v\n", tableStats.LastUpdated)
	
	// Collect column statistics
	fmt.Println("\nCollecting column statistics...")
	columnID := 1
	columnStats, err := statsCollector.CollectColumnStats(ctx, tableID, columnID)
	if err != nil {
		log.Printf("Error collecting column stats: %v", err)
		return
	}
	fmt.Printf("Collected column stats for table %d, column %d\n", columnStats.RelID, columnStats.AttNum)
	fmt.Printf("Column name: %s\n", columnStats.AttName)
	
	// Update table statistics with more detailed information
	fmt.Println("\nUpdating table statistics...")
	updatedStats := &catalog.TableStatistics{
		RelID:        tableID,
		RelName:      "users",
		SeqScan:      150,
		SeqTupRead:   1500,
		IdxScan:      75,
		IdxTupFetch:  750,
		NTupIns:      300,
		NTupUpd:      75,
		NTupDel:      15,
		NTupHotUpd:   8,
		NLiveTup:     15000,
		NDeadTup:     150,
		HeapBlksRead: 1500,
		HeapBlksHit:  13500,
		IdxBlksRead:  750,
		IdxBlksHit:   6750,
		LastUpdated:  time.Now(),
	}
	
	err = statsCollector.UpdateStats(ctx, tableID, updatedStats)
	if err != nil {
		log.Printf("Error updating stats: %v", err)
		return
	}
	fmt.Println("Updated table statistics successfully")
	
	// Retrieve table statistics
	fmt.Println("\nRetrieving table statistics...")
	retrievedStats, err := statsCollector.GetTableStats(ctx, tableID)
	if err != nil {
		log.Printf("Error getting table stats: %v", err)
		return
	}
	fmt.Printf("Retrieved table stats: RelID=%d, RelName=%s, SeqScan=%d, NTupIns=%d\n", 
		retrievedStats.RelID, retrievedStats.RelName, retrievedStats.SeqScan, retrievedStats.NTupIns)
	
	// Retrieve column statistics
	fmt.Println("\nRetrieving column statistics...")
	retrievedColumnStats, err := statsCollector.GetColumnStats(ctx, tableID, columnID)
	if err != nil {
		log.Printf("Error getting column stats: %v", err)
		return
	}
	fmt.Printf("Retrieved column stats: RelID=%d, AttNum=%d, AttName=%s, NDistinct=%d\n", 
		retrievedColumnStats.RelID, retrievedColumnStats.AttNum, retrievedColumnStats.AttName, retrievedColumnStats.NDistinct)
	
	// Delete statistics
	fmt.Println("\nDeleting statistics...")
	err = statsCollector.DeleteStats(ctx, tableID)
	if err != nil {
		log.Printf("Error deleting stats: %v", err)
		return
	}
	fmt.Println("Deleted statistics for table", tableID)
	
	// Try to retrieve deleted statistics (should fail)
	fmt.Println("\nTrying to retrieve deleted statistics...")
	_, err = statsCollector.GetTableStats(ctx, tableID)
	if err != nil {
		fmt.Printf("Expected error when retrieving deleted stats: %v\n", err)
	}
	
	_, err = statsCollector.GetColumnStats(ctx, tableID, columnID)
	if err != nil {
		fmt.Printf("Expected error when retrieving deleted column stats: %v\n", err)
	}
	
	fmt.Println("\nExample completed successfully!")
}