package interfaces

import (
	"context"
	"time"
	
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/types"
)

// TableManager interface defines the methods that system providers need from the catalog manager
type TableManager interface {
	ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error)
	GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error)
	GetStatsCollector() StatsManager
	QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
	SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
	GetEngine() engineTypes.StorageEngine
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
	
	// CollectTableStats collects statistics for a specific table
	CollectTableStats(ctx context.Context, tableID uint64) (*TableStatistics, error)
	
	// CollectColumnStats collects statistics for a specific column
	CollectColumnStats(ctx context.Context, tableID uint64, columnName string) (*ColumnStatistics, error)
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