package system

import (
	"context"
	"time"
	
	"github.com/guileen/pglitedb/types"
)

// SystemCatalog defines the interface for querying system tables
type SystemCatalog interface {
	QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
}

// TableManager interface defines the methods that system providers need from the catalog manager
type TableManager interface {
	ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error)
	GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error)
	GetStatsCollector() StatsManager
	GetEnhancedStatsManager() StatisticsManager
	QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
	SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
}

// ColumnManager interface defines the methods for column operations
type ColumnManager interface {
	GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error)
}

// IndexManager interface defines the methods for index operations
type IndexManager interface {
	ListIndexes(ctx context.Context, tenantID int64, tableName string) ([]*types.IndexDefinition, error)
}

// ViewManager interface defines the methods for view operations
type ViewManager interface {
	ListViews(ctx context.Context, tenantID int64) ([]*types.ViewDefinition, error)
	GetViewDefinition(ctx context.Context, tenantID int64, viewName string) (*types.ViewDefinition, error)
}

// ConstraintManager interface defines the methods for constraint operations
type ConstraintManager interface {
	ListConstraints(ctx context.Context, tenantID int64, tableName string) ([]*types.ConstraintDef, error)
}

// StatsManager interface defines the methods for statistics operations
type StatsManager interface {
	// GetTableStats retrieves statistics for a specific table
	GetTableStats(ctx context.Context, tableID uint64) (*TableStatistics, error)
	
	// GetColumnStats retrieves statistics for a specific column
	GetColumnStats(ctx context.Context, tableID uint64, columnName string) (*ColumnStatistics, error)
	
	// GetDatabaseStats retrieves overall database statistics
	GetDatabaseStats(ctx context.Context) (*DatabaseStatistics, error)
}

// StatisticsManager interface defines methods for managing table, column, and index statistics
type StatisticsManager interface {
	// Table statistics methods
	GetTableStatistics(tableID uint64) (*EnhancedTableStatistics, error)
	UpdateTableStatistics(tableID uint64, stats *EnhancedTableStatistics) error
	RefreshTableStatistics(tableID uint64) error
	
	// Column statistics methods
	GetColumnStatistics(tableID uint64, columnName string) (*EnhancedColumnStatistics, error)
	UpdateColumnStatistics(tableID uint64, columnName string, stats *EnhancedColumnStatistics) error
	RefreshColumnStatistics(tableID uint64, columnName string) error
	
	// Index statistics methods
	GetIndexStatistics(indexID uint64) (*EnhancedIndexStatistics, error)
	UpdateIndexStatistics(indexID uint64, stats *EnhancedIndexStatistics) error
	RefreshIndexStatistics(indexID uint64) error
	
	// Bulk operations
	CollectStatisticsForTable(tableID uint64, tableDef *types.TableDefinition) (*EnhancedTableStatistics, error)
}

// TableStatistics represents statistics for a table
type TableStatistics struct {
	RelID                uint64    `json:"relid"`
	RelName              string    `json:"relname"`
	SeqScan              int64     `json:"seq_scan"`
	SeqTupRead           int64     `json:"seq_tup_read"`
	IdxScan              int64     `json:"idx_scan"`
	IdxTupFetch          int64     `json:"idx_tup_fetch"`
	NScan                int64     `json:"n_tup_ins"`
	NUpdate              int64     `json:"n_tup_upd"`
	NDelete              int64     `json:"n_tup_del"`
	NLiveTup             int64     `json:"n_live_tup"`
	NDeadTup             int64     `json:"n_dead_tup"`
	NModSinceAnalyze     int64     `json:"n_mod_since_analyze"`
	LastVacuum           time.Time `json:"last_vacuum"`
	LastAutovacuum       time.Time `json:"last_autovacuum"`
	LastAnalyze          time.Time `json:"last_analyze"`
	LastAutoanalyze      time.Time `json:"last_autoanalyze"`
	VacuumCount          int64     `json:"vacuum_count"`
	AutovacuumCount      int64     `json:"autovacuum_count"`
	AnalyzeCount         int64     `json:"analyze_count"`
	AutoanalyzeCount     int64     `json:"autoanalyze_count"`
	LastUpdated          time.Time `json:"last_updated"`
}

// ColumnStatistics represents statistics for a column
type ColumnStatistics struct {
	TableID              uint64    `json:"table_id"`
	ColumnName           string    `json:"column_name"`
	NullFrac             float64   `json:"null_frac"`
	AvgWidth             int32     `json:"avg_width"`
	NDistinct            float64   `json:"n_distinct"`
	Correlation          float64   `json:"correlation"`
	MostCommonVals       []string  `json:"most_common_vals"`
	MostCommonFreqs      []float64 `json:"most_common_freqs"`
	HistogramBounds      []string  `json:"histogram_bounds"`
	LastUpdated          time.Time `json:"last_updated"`
}

// DatabaseStatistics represents overall database statistics
type DatabaseStatistics struct {
	DBName               string    `json:"datname"`
	NumBackends          int32     `json:"numbackends"`
	XactCommit           int64     `json:"xact_commit"`
	XactRollback         int64     `json:"xact_rollback"`
	BlksRead             int64     `json:"blks_read"`
	BlksHit              int64     `json:"blks_hit"`
	TupReturned          int64     `json:"tup_returned"`
	TupFetched           int64     `json:"tup_fetched"`
	TupInserted          int64     `json:"tup_inserted"`
	TupUpdated           int64     `json:"tup_updated"`
	TupDeleted           int64     `json:"tup_deleted"`
	Conflicts            int64     `json:"conflicts"`
	TempFiles            int64     `json:"temp_files"`
	TempBytes            int64     `json:"temp_bytes"`
	Deadlocks            int64     `json:"deadlocks"`
	BlkReadTime          float64   `json:"blk_read_time"`
	BlkWriteTime         float64   `json:"blk_write_time"`
	StatsReset           time.Time `json:"stats_reset"`
}

// EnhancedTableStatistics represents enhanced statistics for a table to support cost-based optimization
type EnhancedTableStatistics struct {
	// Basic table information
	TableID       uint64    `json:"table_id"`
	TableName     string    `json:"table_name"`
	RowCount      int64     `json:"row_count"`
	SizeInBytes   int64     `json:"size_in_bytes"`
	LastUpdated   time.Time `json:"last_updated"`
	
	// Column statistics mapping (column name -> statistics)
	ColumnStats   map[string]*EnhancedColumnStatistics `json:"column_stats"`
	
	// Index statistics mapping (index name -> statistics)
	IndexStats    map[string]*EnhancedIndexStatistics  `json:"index_stats"`
}

// EnhancedColumnStatistics represents detailed statistics for a column to support cost-based optimization
type EnhancedColumnStatistics struct {
	// Basic column information
	TableID       uint64    `json:"table_id"`
	ColumnName    string    `json:"column_name"`
	DataType      string    `json:"data_type"`
	
	// Statistical data
	DistinctCount int64     `json:"distinct_count"`      // Number of distinct values
	NullCount     int64     `json:"null_count"`          // Number of NULL values
	MinValue      interface{} `json:"min_value"`         // Minimum value (if applicable)
	MaxValue      interface{} `json:"max_value"`         // Maximum value (if applicable)
	AvgSize       int32     `json:"avg_size"`            // Average size in bytes
	
	// Distribution information
	MostCommonValues  []interface{} `json:"most_common_values"`  // Most frequently occurring values
	ValueFrequencies  []int64       `json:"value_frequencies"`   // Frequencies of most common values
	HistogramBounds   []interface{} `json:"histogram_bounds"`    // Boundary values for histogram
	
	// Correlation data for ordered scans
	Correlation   float64   `json:"correlation"`         // Correlation between physical row ordering and column value ordering
	
	LastUpdated   time.Time `json:"last_updated"`
}

// EnhancedIndexStatistics represents statistics for an index to support cost-based optimization
type EnhancedIndexStatistics struct {
	// Basic index information
	IndexID       uint64    `json:"index_id"`
	IndexName     string    `json:"index_name"`
	TableID       uint64    `json:"table_id"`
	
	// Structural information
	Height        int32     `json:"height"`              // Height of the index tree
	LeafPages     int64     `json:"leaf_pages"`          // Number of leaf pages
	InternalPages int64     `json:"internal_pages"`      // Number of internal pages
	RootPage      int64     `json:"root_page"`           // Root page ID
	
	// Size information
	AvgKeySize    int32     `json:"avg_key_size"`        // Average key size in bytes
	AvgValueSize  int32     `json:"avg_value_size"`      // Average value size in bytes
	TotalSize     int64     `json:"total_size"`          // Total size in bytes
	
	// Distribution information
	DistinctKeys  int64     `json:"distinct_keys"`       // Number of distinct keys
	NullKeys      int64     `json:"null_keys"`           // Number of NULL keys
	
	LastUpdated   time.Time `json:"last_updated"`
}

// InformationSchemaProvider defines the interface for information_schema queries
type InformationSchemaProvider interface {
	QueryTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryColumns(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
}

// PgCatalogProvider defines the interface for pg_catalog queries
type PgCatalogProvider interface {
	QueryPgTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgColumns(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgIndexes(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgConstraint(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgViews(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgStatUserTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgStatUserIndexes(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgStats(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgStatDatabase(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgStatBgWriter(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgIndex(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgInherits(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgClass(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgAttribute(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgType(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgNamespace(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
	QueryPgProc(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error)
}