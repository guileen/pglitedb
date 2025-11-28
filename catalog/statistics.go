package catalog

import (
	"github.com/guileen/pglitedb/catalog/system"
)

// TableStatistics represents enhanced statistics for a table to support cost-based optimization
type TableStatistics = system.EnhancedTableStatistics

// ColumnStatistics represents detailed statistics for a column to support cost-based optimization
type ColumnStatistics = system.EnhancedColumnStatistics

// IndexStatistics represents statistics for an index to support cost-based optimization
type IndexStatistics = system.EnhancedIndexStatistics