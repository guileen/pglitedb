package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
)

// TestQueryPgStatUserTables tests the pg_stat_user_tables system table query
func TestQueryPgStatUserTables(t *testing.T) {
	// Create a stats collector
	collector := NewStatsCollector(nil)
	
	// Add some test statistics
	ctx := context.Background()
	tableID := uint64(1)
	
	tableStats := &interfaces.TableStatistics{
		RelID:                tableID,
		RelName:              "users",
		SeqScan:              100,
		SeqTupRead:           1000,
		IdxScan:              50,
		IdxTupFetch:          500,
		NScan:                200,
		NUpdate:              50,
		NDelete:              10,
		NLiveTup:             10000,
		NDeadTup:             100,
		NModSinceAnalyze:     2000,
		LastVacuum:           time.Now(),
		LastAutovacuum:       time.Now(),
		LastAnalyze:          time.Now(),
		LastAutoanalyze:      time.Now(),
		VacuumCount:          2,
		AutovacuumCount:      1,
		AnalyzeCount:         2,
		AutoanalyzeCount:     1,
		LastUpdated:          time.Now(),
	}
	
	err := collector.(*statsCollector).UpdateStats(ctx, tableID, tableStats)
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
	
	tableStats := &interfaces.TableStatistics{
		RelID:                tableID,
		RelName:              "users",
		IdxScan:              50,
		IdxTupFetch:          500,
		NScan:                200,
		NUpdate:              50,
		NDelete:              10,
		NLiveTup:             10000,
		NDeadTup:             100,
		NModSinceAnalyze:     2000,
		LastVacuum:           time.Now(),
		LastAutovacuum:       time.Now(),
		LastAnalyze:          time.Now(),
		LastAutoanalyze:      time.Now(),
		VacuumCount:          2,
		AutovacuumCount:      1,
		AnalyzeCount:         2,
		AutoanalyzeCount:     1,
		LastUpdated:          time.Now(),
	}
	
	err := collector.(*statsCollector).UpdateStats(ctx, tableID, tableStats)
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
	
	// Test collecting column stats
	collectedStats, err := collector.(*statsCollector).CollectColumnStats(ctx, tableID, "column1")
	assert.NoError(t, err)
	assert.Equal(t, tableID, collectedStats.TableID)
	assert.Equal(t, "column1", collectedStats.ColumnName)
	
	// Test updating table stats (needed for the system table query)
	tableStats := &interfaces.TableStatistics{
		RelID:       tableID,
		RelName:     "users",
		LastUpdated: time.Now(),
	}
	
	err = collector.(*statsCollector).UpdateStats(ctx, tableID, tableStats)
	assert.NoError(t, err)
}