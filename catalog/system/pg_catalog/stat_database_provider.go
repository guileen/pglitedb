package pgcatalog

import (
	"context"
	"fmt"
	"time"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

// QueryPgStatDatabase implements the pg_stat_database query
func (p *Provider) QueryPgStatDatabase(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// Get stats collector
	statsCollector := p.manager.GetStatsCollector()
	if statsCollector == nil {
		return nil, fmt.Errorf("stats collector not available")
	}
	// Get database statistics
	dbStats, err := statsCollector.GetDatabaseStats(ctx)
	if err != nil {
		// If stats not found, create empty stats
		dbStats = &interfaces.DatabaseStatistics{
			DBName:    "pglitedb",
			StatsReset: time.Now(),
		}
	}
	rows := make([][]interface{}, 0)
	// Apply database name filter
	if filterDatName, ok := filter["datname"].(string); ok {
		if dbStats.DBName != filterDatName {
			// Return empty result if filter doesn't match
			columns := []types.ColumnInfo{
				{Name: "datid", Type: types.ColumnTypeBigInt},
				{Name: "datname", Type: types.ColumnTypeText},
				{Name: "numbackends", Type: types.ColumnTypeBigInt},
				{Name: "xact_commit", Type: types.ColumnTypeBigInt},
				{Name: "xact_rollback", Type: types.ColumnTypeBigInt},
				{Name: "blks_read", Type: types.ColumnTypeBigInt},
				{Name: "blks_hit", Type: types.ColumnTypeBigInt},
				{Name: "tup_returned", Type: types.ColumnTypeBigInt},
				{Name: "tup_fetched", Type: types.ColumnTypeBigInt},
				{Name: "tup_inserted", Type: types.ColumnTypeBigInt},
				{Name: "tup_updated", Type: types.ColumnTypeBigInt},
				{Name: "tup_deleted", Type: types.ColumnTypeBigInt},
				{Name: "stats_reset", Type: types.ColumnTypeTimestamp},
			}
			return &types.QueryResult{
				Columns: columns,
				Rows:    rows,
			}, nil
		}
	}
	// Add database entry
	row := []interface{}{
		int64(1),             // datid (placeholder)
		dbStats.DBName,        // datname
		dbStats.NumBackends,   // numbackends
		dbStats.XactCommit,    // xact_commit
		dbStats.XactRollback,  // xact_rollback
		dbStats.BlksRead,      // blks_read
		dbStats.BlksHit,       // blks_hit
		dbStats.TupReturned,   // tup_returned
		dbStats.TupFetched,    // tup_fetched
		dbStats.TupInserted,   // tup_inserted
		dbStats.TupUpdated,    // tup_updated
		dbStats.TupDeleted,    // tup_deleted
		dbStats.StatsReset,    // stats_reset
	}
	rows = append(rows, row)
	columns := []types.ColumnInfo{
		{Name: "datid", Type: types.ColumnTypeBigInt},
		{Name: "datname", Type: types.ColumnTypeText},
		{Name: "numbackends", Type: types.ColumnTypeBigInt},
		{Name: "xact_commit", Type: types.ColumnTypeBigInt},
		{Name: "xact_rollback", Type: types.ColumnTypeBigInt},
		{Name: "blks_read", Type: types.ColumnTypeBigInt},
		{Name: "blks_hit", Type: types.ColumnTypeBigInt},
		{Name: "tup_returned", Type: types.ColumnTypeBigInt},
		{Name: "tup_fetched", Type: types.ColumnTypeBigInt},
		{Name: "tup_inserted", Type: types.ColumnTypeBigInt},
		{Name: "tup_updated", Type: types.ColumnTypeBigInt},
		{Name: "tup_deleted", Type: types.ColumnTypeBigInt},
		{Name: "stats_reset", Type: types.ColumnTypeTimestamp},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}