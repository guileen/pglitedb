package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestStatsCollectorIntegration tests the integration of the stats collector with table statistics
func TestStatsCollectorIntegration(t *testing.T) {
	// Create stats collector
	collector := NewStatsCollector(nil)
	
	// Test collecting and retrieving table stats
	ctx := context.Background()
	tableID := uint64(1)
	
	// Collect table statistics
	tableStats, err := collector.CollectTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, tableID, tableStats.RelID)
	assert.NotEmpty(t, tableStats.RelName)
	assert.WithinDuration(t, time.Now(), tableStats.LastUpdated, time.Second)
	
	// Collect column statistics
	columnID := 1
	columnStats, err := collector.CollectColumnStats(ctx, tableID, columnID)
	assert.NoError(t, err)
	assert.Equal(t, tableID, columnStats.RelID)
	assert.Equal(t, columnID, columnStats.AttNum)
	assert.NotEmpty(t, columnStats.AttName)
	
	// Retrieve table stats
	retrievedTableStats, err := collector.GetTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, tableStats, retrievedTableStats)
	
	// Retrieve column stats
	retrievedColumnStats, err := collector.GetColumnStats(ctx, tableID, columnID)
	assert.NoError(t, err)
	assert.Equal(t, columnStats, retrievedColumnStats)
	
	// Update stats
	updatedTableStats := &TableStatistics{
		RelID:        tableID,
		RelName:      "test_users",
		SeqScan:      50,
		SeqTupRead:   500,
		IdxScan:      25,
		IdxTupFetch:  250,
		NTupIns:      100,
		NTupUpd:      25,
		NTupDel:      5,
		NTupHotUpd:   2,
		NLiveTup:     5000,
		NDeadTup:     50,
		HeapBlksRead: 500,
		HeapBlksHit:  4500,
		IdxBlksRead:  250,
		IdxBlksHit:   2250,
		LastUpdated:  time.Now(),
	}
	
	err = collector.UpdateStats(ctx, tableID, updatedTableStats)
	assert.NoError(t, err)
	
	// Verify updated stats
	retrievedUpdatedStats, err := collector.GetTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, updatedTableStats, retrievedUpdatedStats)
	
	// Delete stats
	err = collector.DeleteStats(ctx, tableID)
	assert.NoError(t, err)
	
	// Verify deletion
	_, err = collector.GetTableStats(ctx, tableID)
	assert.Error(t, err)
	
	_, err = collector.GetColumnStats(ctx, tableID, columnID)
	assert.Error(t, err)
}

// TestTableStatisticsStructure verifies the structure of TableStatistics
func TestTableStatisticsStructure(t *testing.T) {
	stats := &TableStatistics{
		RelID:        1,
		RelName:      "users",
		SeqScan:      100,
		SeqTupRead:   1000,
		IdxScan:      50,
		IdxTupFetch:  500,
		NTupIns:      200,
		NTupUpd:      50,
		NTupDel:      10,
		NTupHotUpd:   5,
		NLiveTup:     10000,
		NDeadTup:     100,
		HeapBlksRead: 1000,
		HeapBlksHit:  9000,
		IdxBlksRead:  500,
		IdxBlksHit:   4500,
		LastUpdated:  time.Now(),
	}
	
	// Verify all fields are present and correctly typed
	assert.Equal(t, uint64(1), stats.RelID)
	assert.Equal(t, "users", stats.RelName)
	assert.Equal(t, int64(100), stats.SeqScan)
	assert.Equal(t, int64(1000), stats.SeqTupRead)
	assert.Equal(t, int64(50), stats.IdxScan)
	assert.Equal(t, int64(500), stats.IdxTupFetch)
	assert.Equal(t, int64(200), stats.NTupIns)
	assert.Equal(t, int64(50), stats.NTupUpd)
	assert.Equal(t, int64(10), stats.NTupDel)
	assert.Equal(t, int64(5), stats.NTupHotUpd)
	assert.Equal(t, int64(10000), stats.NLiveTup)
	assert.Equal(t, int64(100), stats.NDeadTup)
	assert.Equal(t, int64(1000), stats.HeapBlksRead)
	assert.Equal(t, int64(9000), stats.HeapBlksHit)
	assert.Equal(t, int64(500), stats.IdxBlksRead)
	assert.Equal(t, int64(4500), stats.IdxBlksHit)
}

// TestColumnStatisticsStructure verifies the structure of ColumnStatistics
func TestColumnStatisticsStructure(t *testing.T) {
	stats := &ColumnStatistics{
		RelID:           1,
		AttNum:          1,
		AttName:         "username",
		NDistinct:       5000,
		NullFrac:        0.001,
		AvgWidth:        15,
		MostCommonVals:  []string{"john", "jane", "bob", "alice"},
		MostCommonFreqs: []float64{0.01, 0.008, 0.005, 0.003},
		HistogramBounds: []string{"a", "f", "k", "p", "u", "z"},
	}
	
	// Verify all fields are present and correctly typed
	assert.Equal(t, uint64(1), stats.RelID)
	assert.Equal(t, 1, stats.AttNum)
	assert.Equal(t, "username", stats.AttName)
	assert.Equal(t, int64(5000), stats.NDistinct)
	assert.Equal(t, 0.001, stats.NullFrac)
	assert.Equal(t, 15, stats.AvgWidth)
	assert.Equal(t, []string{"john", "jane", "bob", "alice"}, stats.MostCommonVals)
	assert.Equal(t, []float64{0.01, 0.008, 0.005, 0.003}, stats.MostCommonFreqs)
	assert.Equal(t, []string{"a", "f", "k", "p", "u", "z"}, stats.HistogramBounds)
}

// TestConcurrentAccess tests concurrent access to the stats collector
func TestConcurrentAccess(t *testing.T) {
	collector := NewStatsCollector(nil)
	ctx := context.Background()
	
	// Run multiple goroutines to test concurrent access
	done := make(chan bool)
	
	for i := 0; i < 10; i++ {
		go func(id int) {
			tableID := uint64(id)
			
			// Collect stats
			_, err := collector.CollectTableStats(ctx, tableID)
			assert.NoError(t, err)
			
			// Get stats
			_, err = collector.GetTableStats(ctx, tableID)
			assert.NoError(t, err)
			
			done <- true
		}(i)
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}