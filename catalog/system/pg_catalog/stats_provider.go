package pgcatalog

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

// QueryPgStats implements the pg_stats query
func (p *Provider) QueryPgStats(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
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
		if filterTableName, ok := filter["tablename"].(string); ok {
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
		// For each column, get column statistics
		for _, column := range tableSchema.Columns {
			// Apply column name filter
			if filterAttName, ok := filter["attname"].(string); ok {
				if column.Name != filterAttName {
					continue
				}
			}
			// Get column statistics
			columnStats, err := statsCollector.GetColumnStats(ctx, tableID, column.Name)
			if err != nil {
				// If stats not found, create empty stats
				columnStats = &interfaces.ColumnStatistics{
					TableID:    tableID,
					ColumnName: column.Name,
					LastUpdated: time.Now(),
				}
			}
			// Convert slices to strings for display
			mcvStr := ""
			if len(columnStats.MostCommonVals) > 0 {
				mcvStr = fmt.Sprintf("{%s}", strings.Join(columnStats.MostCommonVals, ","))
			}
			mcfStr := ""
			if len(columnStats.MostCommonFreqs) > 0 {
				mcfParts := make([]string, len(columnStats.MostCommonFreqs))
				for i, freq := range columnStats.MostCommonFreqs {
					mcfParts[i] = fmt.Sprintf("%.6f", freq)
				}
				mcfStr = fmt.Sprintf("{%s}", strings.Join(mcfParts, ","))
			}
			hbStr := ""
			if len(columnStats.HistogramBounds) > 0 {
				hbStr = fmt.Sprintf("{%s}", strings.Join(columnStats.HistogramBounds, ","))
			}
			row := []interface{}{
				"public",                    // schemaname
				table.Name,                  // tablename
				columnStats.ColumnName,      // attname
				columnStats.NDistinct,       // n_distinct
				columnStats.NullFrac,        // null_frac
				columnStats.AvgWidth,        // avg_width
				mcvStr,                     // most_common_vals
				mcfStr,                     // most_common_freqs
				hbStr,                      // histogram_bounds
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "attname", Type: types.ColumnTypeText},
		{Name: "n_distinct", Type: types.ColumnTypeBigInt},
		{Name: "null_frac", Type: types.ColumnTypeDouble},
		{Name: "avg_width", Type: types.ColumnTypeInteger},
		{Name: "most_common_vals", Type: types.ColumnTypeText},
		{Name: "most_common_freqs", Type: types.ColumnTypeText},
		{Name: "histogram_bounds", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}