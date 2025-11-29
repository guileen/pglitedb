package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
)

// TestStatsCollectorIntegration tests the integration of the stats collector with table statistics
func TestStatsCollectorIntegration(t *testing.T) {
	// Create stats collector
	collector := NewStatsCollector(nil)
	
	// Test collecting and retrieving table stats
	ctx := context.Background()
	tableID := uint64(1)
	
	// Collect table statistics
	tableStats, err := collector.(*statsCollector).CollectTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, tableID, tableStats.RelID)
	assert.NotEmpty(t, tableStats.RelName)
	assert.WithinDuration(t, time.Now(), tableStats.LastUpdated, time.Second)
	
	// Collect column statistics
	columnStats, err := collector.(*statsCollector).CollectColumnStats(ctx, tableID, "column1")
	assert.NoError(t, err)
	assert.Equal(t, tableID, columnStats.TableID)
	assert.Equal(t, "column1", columnStats.ColumnName)
	assert.NotEmpty(t, columnStats.ColumnName)
	
	// Retrieve table stats
	retrievedTableStats, err := collector.GetTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, tableStats, retrievedTableStats)
	
	// Retrieve column stats
	retrievedColumnStats, err := collector.GetColumnStats(ctx, tableID, "column1")
	assert.NoError(t, err)
	assert.Equal(t, columnStats, retrievedColumnStats)
	
	// Update stats
	updatedTableStats := &interfaces.TableStatistics{
		RelID:        tableID,
		RelName:      "test_users",
		SeqScan:      50,
		SeqTupRead:   500,
		IdxScan:      25,
		IdxTupFetch:  250,
		NScan:        100,
		NUpdate:      25,
		NDelete:      5,
		NLiveTup:     5000,
		NDeadTup:     50,
		NModSinceAnalyze: 1000,
		LastVacuum:   time.Now(),
		LastAutovacuum: time.Now(),
		LastAnalyze:  time.Now(),
		LastAutoanalyze: time.Now(),
		VacuumCount:  1,
		AutovacuumCount: 0,
		AnalyzeCount: 1,
		AutoanalyzeCount: 0,

		LastUpdated:  time.Now(),
	}
	
	err = collector.(*statsCollector).UpdateStats(ctx, tableID, updatedTableStats)
	assert.NoError(t, err)
	
	// Verify updated stats
	retrievedUpdatedStats, err := collector.GetTableStats(ctx, tableID)
	assert.NoError(t, err)
	assert.Equal(t, updatedTableStats, retrievedUpdatedStats)
	
	// Delete stats
	err = collector.(*statsCollector).DeleteStats(ctx, tableID)
	assert.NoError(t, err)
	
	// Verify deletion
	_, err = collector.GetTableStats(ctx, tableID)
	assert.Error(t, err)
	
	_, err = collector.GetColumnStats(ctx, tableID, "column1")
	assert.Error(t, err)
}

// TestTableStatisticsStructure verifies the structure of TableStatistics
func TestTableStatisticsStructure(t *testing.T) {
	stats := &interfaces.TableStatistics{
		RelID:        1,
		RelName:      "users",
		SeqScan:      100,
		SeqTupRead:   1000,
		IdxScan:      50,
		IdxTupFetch:  500,
		NScan:        200,
		NUpdate:      50,
		NDelete:      10,
		NLiveTup:     10000,
		NDeadTup:     100,
		NModSinceAnalyze: 2000,
		LastVacuum:   time.Now(),
		LastAutovacuum: time.Now(),
		LastAnalyze:  time.Now(),
		LastAutoanalyze: time.Now(),
		VacuumCount:  2,
		AutovacuumCount: 1,
		AnalyzeCount: 2,
		AutoanalyzeCount: 1,

		LastUpdated:  time.Now(),
	}
	
	// Verify all fields are present and correctly typed
	assert.Equal(t, uint64(1), stats.RelID)
	assert.Equal(t, "users", stats.RelName)
	assert.Equal(t, int64(100), stats.SeqScan)
	assert.Equal(t, int64(1000), stats.SeqTupRead)
	assert.Equal(t, int64(50), stats.IdxScan)
	assert.Equal(t, int64(500), stats.IdxTupFetch)
	assert.Equal(t, int64(200), stats.NScan)
	assert.Equal(t, int64(50), stats.NUpdate)
	assert.Equal(t, int64(10), stats.NDelete)
	assert.Equal(t, int64(10000), stats.NLiveTup)
	assert.Equal(t, int64(100), stats.NDeadTup)
	assert.Equal(t, int64(2000), stats.NModSinceAnalyze)

}

// TestColumnStatisticsStructure verifies the structure of ColumnStatistics
func TestColumnStatisticsStructure(t *testing.T) {
	stats := &interfaces.ColumnStatistics{
		TableID:         1,
		ColumnName:      "username",
		NDistinct:       5000,
		NullFrac:        0.001,
		AvgWidth:        15,
		Correlation:     0.5,
		MostCommonVals:  []string{"john", "jane", "bob", "alice"},
		MostCommonFreqs: []float64{0.01, 0.008, 0.005, 0.003},
		HistogramBounds: []string{"a", "f", "k", "p", "u", "z"},
		LastUpdated:     time.Now(),
	}
	
	// Verify all fields are present and correctly typed
	assert.Equal(t, uint64(1), stats.TableID)
	assert.Equal(t, "username", stats.ColumnName)
	assert.Equal(t, float64(5000), stats.NDistinct)
	assert.Equal(t, 0.001, stats.NullFrac)
	assert.Equal(t, int32(15), stats.AvgWidth)
	assert.Equal(t, 0.5, stats.Correlation)
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
			_, err := collector.(*statsCollector).CollectTableStats(ctx, tableID)
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

// BenchmarkCollectTableStats benchmarks the table stats collection performance
func BenchmarkCollectTableStats(b *testing.B) {
	collector := NewStatsCollector(nil)
	ctx := context.Background()
	tableID := uint64(1)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collector.(*statsCollector).CollectTableStats(ctx, tableID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCollectColumnStats benchmarks the column stats collection performance
func BenchmarkCollectColumnStats(b *testing.B) {
	collector := NewStatsCollector(nil)
	ctx := context.Background()
	tableID := uint64(1)
	columnName := "test_column"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collector.(*statsCollector).CollectColumnStats(ctx, tableID, columnName)
		if err != nil {
			b.Fatal(err)
		}
	}
}