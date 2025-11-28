package pgcatalog

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

// QueryPgStatUserIndexes implements the pg_stat_user_indexes query
func (p *Provider) QueryPgStatUserIndexes(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
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
		tableSchema, err := p.manager.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
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
		// Add row for each index
		for _, index := range tableSchema.Indexes {
			// Apply index name filter
			if filterIndexName, ok := filter["indexname"].(string); ok {
				if index.Name != filterIndexName {
					continue
				}
			}
			// Generate a unique index ID (in a real implementation, this would come from the index metadata)
			indexID := fmt.Sprintf("%s_%s_idx", table.ID, index.Name)
			row := []interface{}{
				table.ID,              // relid
				indexID,               // indexrelid
				"public",              // schemaname
				tableStats.RelName,    // relname
				index.Name,            // indexname
				tableStats.IdxScan,    // idx_scan
				int64(0),              // idx_tup_read (not in TableStatistics)
				tableStats.IdxTupFetch,// idx_tup_fetch
				tableStats.LastUpdated,// last_updated
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "relid", Type: types.ColumnTypeText},
		{Name: "indexrelid", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "relname", Type: types.ColumnTypeText},
		{Name: "indexname", Type: types.ColumnTypeText},
		{Name: "idx_scan", Type: types.ColumnTypeBigInt},
		{Name: "idx_tup_read", Type: types.ColumnTypeBigInt},
		{Name: "idx_tup_fetch", Type: types.ColumnTypeBigInt},
		{Name: "last_updated", Type: types.ColumnTypeTimestamp},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}