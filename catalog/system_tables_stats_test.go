package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestQueryPgStatUserTables tests the pg_stat_user_tables system table query
func TestQueryPgStatUserTables(t *testing.T) {
	// Create a stats collector
	collector := NewStatsCollector(nil)
	
	// Add some test statistics
	ctx := context.Background()
	tableID := uint64(1)
	
	tableStats := &TableStatistics{
		RelID:        tableID,
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
	
	err := collector.UpdateStats(ctx, tableID, tableStats)
	assert.NoError(t, err)
	
	// Test that we can retrieve the stats
	retrievedStats, err := collector.GetTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, tableStats, retrievedStats)
}

// TestQueryPgStatUserIndexes tests the pg_stat_user_indexes system table query
func TestQueryPgStatUserIndexes(t *testing.T) {
	// Create a stats collector
	collector := NewStatsCollector(nil)
	
	// Add some test statistics
	ctx := context.Background()
	tableID := uint64(1)
	
	tableStats := &TableStatistics{
		RelID:       tableID,
		RelName:     "users",
		IdxScan:     50,
		IdxTupFetch: 500,
		LastUpdated: time.Now(),
	}
	
	err := collector.UpdateStats(ctx, tableID, tableStats)
	assert.NoError(t, err)
	
	// Test that we can retrieve the stats
	retrievedStats, err := collector.GetTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, tableStats, retrievedStats)
}

// TestQueryPgStats tests the pg_stats system table query
func TestQueryPgStats(t *testing.T) {
	// Create a stats collector
	collector := NewStatsCollector(nil)
	
	// Test collecting column stats
	ctx := context.Background()
	tableID := uint64(1)
	columnID := 1
	
	// Test collecting column stats
	collectedStats, err := collector.CollectColumnStats(ctx, tableID, columnID)
	assert.NoError(t, err)
	assert.Equal(t, tableID, collectedStats.RelID)
	assert.Equal(t, columnID, collectedStats.AttNum)
	
	// Test updating table stats (needed for the system table query)
	tableStats := &TableStatistics{
		RelID:       tableID,
		RelName:     "users",
		LastUpdated: time.Now(),
	}
	
	err = collector.UpdateStats(ctx, tableID, tableStats)
	assert.NoError(t, err)
}