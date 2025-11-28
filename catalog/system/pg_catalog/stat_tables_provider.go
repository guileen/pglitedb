package pgcatalog

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

// QueryPgStatUserTables implements the pg_stat_user_tables query
func (p *Provider) QueryPgStatUserTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	// Get stats collector
	statsCollector := p.manager.GetStatsCollector()
	if statsCollector == nil {
		return nil, fmt.Errorf("stats collector not available")
	}
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Apply table name filter
		if filterTableName, ok := filter["relname"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		// Convert table ID to uint64 for stats collector
		tableID, err := strconv.ParseUint(table.ID, 10, 64)
		if err != nil {
			// Skip tables with invalid IDs
			continue
		}
		// Get table statistics
		tableStats, err := statsCollector.GetTableStats(ctx, tableID)
		if err != nil {
			// If stats not found, create empty stats
			tableStats = &interfaces.TableStatistics{
				RelID:       tableID,
				RelName:     table.Name,
				LastUpdated: time.Now(),
			}
		}
		row := []interface{}{
			tableStats.RelID,              // relid
			"public",                      // schemaname
			tableStats.RelName,            // relname
			tableStats.SeqScan,            // seq_scan
			tableStats.SeqTupRead,         // seq_tup_read
			tableStats.IdxScan,            // idx_scan
			tableStats.IdxTupFetch,        // idx_tup_fetch
			tableStats.NScan,              // n_tup_ins
			tableStats.NUpdate,            // n_tup_upd
			tableStats.NDelete,            // n_tup_del
			int64(0),                      // n_tup_hot_upd (not tracked)
			tableStats.NLiveTup,           // n_live_tup
			tableStats.NDeadTup,           // n_dead_tup
			int64(0),                      // heap_blks_read (not tracked)
			int64(0),                      // heap_blks_hit (not tracked)
			int64(0),                      // idx_blks_read (not tracked)
			int64(0),                      // idx_blks_hit (not tracked)
			tableStats.LastUpdated,        // last_updated
		}
		rows = append(rows, row)
	}
	columns := []types.ColumnInfo{
		{Name: "relid", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "relname", Type: types.ColumnTypeText},
		{Name: "seq_scan", Type: types.ColumnTypeBigInt},
		{Name: "seq_tup_read", Type: types.ColumnTypeBigInt},
		{Name: "idx_scan", Type: types.ColumnTypeBigInt},
		{Name: "idx_tup_fetch", Type: types.ColumnTypeBigInt},
		{Name: "n_tup_ins", Type: types.ColumnTypeBigInt},
		{Name: "n_tup_upd", Type: types.ColumnTypeBigInt},
		{Name: "n_tup_del", Type: types.ColumnTypeBigInt},
		{Name: "n_tup_hot_upd", Type: types.ColumnTypeBigInt},
		{Name: "n_live_tup", Type: types.ColumnTypeBigInt},
		{Name: "n_dead_tup", Type: types.ColumnTypeBigInt},
		{Name: "heap_blks_read", Type: types.ColumnTypeBigInt},
		{Name: "heap_blks_hit", Type: types.ColumnTypeBigInt},
		{Name: "idx_blks_read", Type: types.ColumnTypeBigInt},
		{Name: "idx_blks_hit", Type: types.ColumnTypeBigInt},
		{Name: "last_updated", Type: types.ColumnTypeTimestamp},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}