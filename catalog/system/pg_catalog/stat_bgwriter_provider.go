package pgcatalog

import (
	"context"
	"fmt"
	"time"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

// QueryPgStatBgWriter implements the pg_stat_bgwriter query
func (p *Provider) QueryPgStatBgWriter(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// Get stats collector
	statsCollector := p.manager.GetStatsCollector()
	if statsCollector == nil {
		return nil, fmt.Errorf("stats collector not available")
	}
	// Get database statistics to extract background writer info
	// In a full implementation, we would have separate bgwriter statistics
	dbStats, err := statsCollector.GetDatabaseStats(ctx)
	if err != nil {
		// If stats not found, create empty stats
		dbStats = &interfaces.DatabaseStatistics{
			DBName:    "pglitedb",
			StatsReset: time.Now(),
		}
	}
	rows := make([][]interface{}, 0)
	// Add a single entry with zero values (placeholder)
	row := []interface{}{
		int64(0),   // checkpoints_timed
		int64(0),   // checkpoints_req
		float64(0), // checkpoint_write_time
		float64(0), // checkpoint_sync_time
		int64(0),   // buffers_checkpoint
		int64(0),   // buffers_clean
		int64(0),   // maxwritten_clean
		int64(0),   // buffers_backend
		int64(0),   // buffers_backend_fsync
		int64(0),   // buffers_alloc
		dbStats.StatsReset, // stats_reset
	}
	rows = append(rows, row)
	columns := []types.ColumnInfo{
		{Name: "checkpoints_timed", Type: types.ColumnTypeBigInt},
		{Name: "checkpoints_req", Type: types.ColumnTypeBigInt},
		{Name: "checkpoint_write_time", Type: types.ColumnTypeDouble},
		{Name: "checkpoint_sync_time", Type: types.ColumnTypeDouble},
		{Name: "buffers_checkpoint", Type: types.ColumnTypeBigInt},
		{Name: "buffers_clean", Type: types.ColumnTypeBigInt},
		{Name: "maxwritten_clean", Type: types.ColumnTypeBigInt},
		{Name: "buffers_backend", Type: types.ColumnTypeBigInt},
		{Name: "buffers_backend_fsync", Type: types.ColumnTypeBigInt},
		{Name: "buffers_alloc", Type: types.ColumnTypeBigInt},
		{Name: "stats_reset", Type: types.ColumnTypeTimestamp},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}