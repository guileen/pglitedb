package catalog

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/guileen/pglitedb/types"
)

type SystemTableQuery struct {
	schema    string
	tableName string
	filter    map[string]interface{}
}

func parseSystemTableQuery(fullTableName string) *SystemTableQuery {
	parts := strings.Split(fullTableName, ".")
	if len(parts) != 2 {
		return nil
	}
	
	return &SystemTableQuery{
		schema:    parts[0],
		tableName: parts[1],
		filter:    make(map[string]interface{}),
	}
}

func (m *tableManager) QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	query := parseSystemTableQuery(fullTableName)
	if query == nil {
		return nil, fmt.Errorf("invalid system table name: %s", fullTableName)
	}
	if filter != nil {
		query.filter = filter
	}
	
	switch query.schema {
	case "information_schema":
		switch query.tableName {
		case "tables":
			return m.queryInformationSchemaTables(ctx, query.filter)
		case "columns":
			return m.queryInformationSchemaColumns(ctx, query.filter)
		default:
			return nil, fmt.Errorf("unsupported information_schema table: %s", query.tableName)
		}
	case "pg_catalog":
		switch query.tableName {
		case "pg_tables":
			return m.queryPgTables(ctx, query.filter)
		case "pg_columns":
			return m.queryPgColumns(ctx, query.filter)
		case "pg_indexes":
			return m.queryPgIndexes(ctx, query.filter)
		case "pg_constraint":
			return m.queryPgConstraint(ctx, query.filter)
		case "pg_views":
			return m.queryPgViews(ctx, query.filter)
		case "pg_stat_user_tables":
			return m.queryPgStatUserTables(ctx, query.filter)
		case "pg_stat_user_indexes":
			return m.queryPgStatUserIndexes(ctx, query.filter)
		case "pg_stats":
			return m.queryPgStats(ctx, query.filter)
		case "pg_stat_database":
			return m.queryPgStatDatabase(ctx, query.filter)
		case "pg_stat_bgwriter":
			return m.queryPgStatBgWriter(ctx, query.filter)
		case "pg_index":
			return m.queryPgIndex(ctx, query.filter)
		case "pg_inherits":
			return m.queryPgInherits(ctx, query.filter)
		default:
			return nil, fmt.Errorf("unsupported pg_catalog table: %s", query.tableName)
		}
	default:
		return nil, fmt.Errorf("unsupported system schema: %s", query.schema)
	}
}

func (m *tableManager) queryInformationSchemaTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		if filterTableName, ok := filter["table_name"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		
		row := []interface{}{
			"def",
			"public",
			table.Name,
			"BASE TABLE",
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		}
		rows = append(rows, row)
	}
	
	columns := []types.ColumnInfo{
		{Name: "table_catalog", Type: types.ColumnTypeText},
		{Name: "table_schema", Type: types.ColumnTypeText},
		{Name: "table_name", Type: types.ColumnTypeText},
		{Name: "table_type", Type: types.ColumnTypeText},
		{Name: "self_referencing_column_name", Type: types.ColumnTypeText},
		{Name: "reference_generation", Type: types.ColumnTypeText},
		{Name: "user_defined_type_catalog", Type: types.ColumnTypeText},
		{Name: "user_defined_type_schema", Type: types.ColumnTypeText},
		{Name: "user_defined_type_name", Type: types.ColumnTypeText},
		{Name: "is_insertable_into", Type: types.ColumnTypeText},
		{Name: "is_typed", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func (m *tableManager) queryInformationSchemaColumns(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	filterTableName, hasTableFilter := filter["table_name"].(string)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		if hasTableFilter && table.Name != filterTableName {
			continue
		}
		
		tableSchema, err := m.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		for ordinal, col := range tableSchema.Columns {
			dataType := mapTypeToSQL(col.Type)
			
			row := []interface{}{
				"def",
				"public",
				table.Name,
				col.Name,
				ordinal + 1,
				col.Default,
				boolToYesNo(col.Nullable),
				dataType,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			}
			rows = append(rows, row)
		}
	}
	
	columns := []types.ColumnInfo{
		{Name: "table_catalog", Type: types.ColumnTypeText},
		{Name: "table_schema", Type: types.ColumnTypeText},
		{Name: "table_name", Type: types.ColumnTypeText},
		{Name: "column_name", Type: types.ColumnTypeText},
		{Name: "ordinal_position", Type: types.ColumnTypeInteger},
		{Name: "column_default", Type: types.ColumnTypeText},
		{Name: "is_nullable", Type: types.ColumnTypeText},
		{Name: "data_type", Type: types.ColumnTypeText},
		{Name: "character_maximum_length", Type: types.ColumnTypeInteger},
		{Name: "character_octet_length", Type: types.ColumnTypeInteger},
		{Name: "numeric_precision", Type: types.ColumnTypeInteger},
		{Name: "numeric_scale", Type: types.ColumnTypeInteger},
		{Name: "datetime_precision", Type: types.ColumnTypeInteger},
		{Name: "character_set_name", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func mapTypeToSQL(colType types.ColumnType) string {
	switch colType {
	case types.ColumnTypeInteger:
		return "integer"
	case types.ColumnTypeBigInt:
		return "bigint"
	case types.ColumnTypeSmallInt:
		return "smallint"
	case types.ColumnTypeText:
		return "text"
	case types.ColumnTypeVarchar:
		return "character varying"
	case types.ColumnTypeBoolean:
		return "boolean"
	case types.ColumnTypeTimestamp:
		return "timestamp without time zone"
	case types.ColumnTypeNumeric:
		return "numeric"
	case types.ColumnTypeJSONB:
		return "jsonb"
	default:
		return "text"
	}
}

func boolToYesNo(val bool) string {
	if val {
		return "YES"
	}
	return "NO"
}

func (m *tableManager) queryPgTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Apply filters
		if filterTableName, ok := filter["tablename"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		
		row := []interface{}{
			table.Name,     // tablename
			"public",       // schemaname
			nil,            // tableowner
			nil,            // hasindexes
			nil,            // hasrules
			nil,            // hastriggers
			nil,            // rowsecurity
		}
		rows = append(rows, row)
	}
	
	columns := []types.ColumnInfo{
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "tableowner", Type: types.ColumnTypeText},
		{Name: "hasindexes", Type: types.ColumnTypeBoolean},
		{Name: "hasrules", Type: types.ColumnTypeBoolean},
		{Name: "hastriggers", Type: types.ColumnTypeBoolean},
		{Name: "rowsecurity", Type: types.ColumnTypeBoolean},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func (m *tableManager) queryPgColumns(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	filterTableName, hasTableFilter := filter["tablename"].(string)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		if hasTableFilter && table.Name != filterTableName {
			continue
		}
		
		tableSchema, err := m.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		for _, col := range tableSchema.Columns {
			dataType := mapTypeToSQL(col.Type)
			
			row := []interface{}{
				table.Name,     // tablename
				col.Name,       // columnname
				"public",       // schemaname
				dataType,       // datatype
				nil,            // ordinal_position
				boolToYesNo(!col.Nullable), // notnull
				col.Default,    // column_default
				nil,            // is_primary_key
				nil,            // is_unique
				nil,            // is_serial
			}
			rows = append(rows, row)
		}
	}
	
	columns := []types.ColumnInfo{
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "columnname", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "datatype", Type: types.ColumnTypeText},
		{Name: "ordinal_position", Type: types.ColumnTypeInteger},
		{Name: "notnull", Type: types.ColumnTypeText},
		{Name: "column_default", Type: types.ColumnTypeText},
		{Name: "is_primary_key", Type: types.ColumnTypeBoolean},
		{Name: "is_unique", Type: types.ColumnTypeBoolean},
		{Name: "is_serial", Type: types.ColumnTypeBoolean},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func (m *tableManager) queryPgIndexes(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Apply table name filter
		if filterTableName, ok := filter["tablename"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		
		tableSchema, err := m.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		// Add row for each index
		for _, index := range tableSchema.Indexes {
			// Determine if this is a primary key index
			isPrimary := false
			for _, constraint := range tableSchema.Constraints {
				if constraint.Type == "primary_key" && len(constraint.Columns) == len(index.Columns) {
					match := true
					for i, col := range constraint.Columns {
						if i >= len(index.Columns) || col != index.Columns[i] {
							match = false
							break
						}
					}
					if match {
						isPrimary = true
						break
					}
				}
			}
			
			row := []interface{}{
				table.Name,                          // tablename
				index.Name,                          // indexname
				"public",                            // schemaname
				strings.Join(index.Columns, ","),    // columnnames
				index.Unique,                        // unique
				index.Type,                          // indextype
				nil,                                 // tablespace
				isPrimary,                           // indisprimary
				strings.Join(index.Columns, ","),    // indexdef (simplified)
			}
			rows = append(rows, row)
		}
	}
	
	columns := []types.ColumnInfo{
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "indexname", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "columnnames", Type: types.ColumnTypeText},
		{Name: "unique", Type: types.ColumnTypeBoolean},
		{Name: "indextype", Type: types.ColumnTypeText},
		{Name: "tablespace", Type: types.ColumnTypeText},
		{Name: "indisprimary", Type: types.ColumnTypeBoolean},
		{Name: "indexdef", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func (m *tableManager) queryPgConstraint(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Apply table name filter
		if filterTableName, ok := filter["tablename"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		
		tableSchema, err := m.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		// Add row for each constraint
		for _, constraint := range tableSchema.Constraints {
			// Prepare foreign key information
			var foreignTable, foreignColumns interface{}
			if constraint.Reference != nil {
				foreignTable = constraint.Reference.Table
				foreignColumns = strings.Join(constraint.Reference.Columns, ",")
			}
			
			// Map constraint type to PostgreSQL format
			contype := ""
			switch constraint.Type {
			case "primary_key":
				contype = "p" // Primary key
			case "unique":
				contype = "u" // Unique
			case "check":
				contype = "c" // Check
			case "foreign_key":
				contype = "f" // Foreign key
			default:
				contype = "x" // Other
			}
			
			row := []interface{}{
				constraint.Name,              // conname
				table.Name,                   // tablename
				"public",                     // schemaname
				contype,                      // contype
				strings.Join(constraint.Columns, ","), // columnnames
				constraint.CheckExpression,   // check_expr
				foreignTable,                 // foreign_table
				foreignColumns,               // foreign_columns
				constraint.Type,              // contypename (human-readable)
			}
			rows = append(rows, row)
		}
	}
	
	columns := []types.ColumnInfo{
		{Name: "conname", Type: types.ColumnTypeText},
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "contype", Type: types.ColumnTypeText},
		{Name: "columnnames", Type: types.ColumnTypeText},
		{Name: "check_expr", Type: types.ColumnTypeText},
		{Name: "foreign_table", Type: types.ColumnTypeText},
		{Name: "foreign_columns", Type: types.ColumnTypeText},
		{Name: "contypename", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// PgStatUserTables represents the structure of pg_stat_user_tables system table
type PgStatUserTables struct {
	RelID        string    // Table ID
	Schemaname   string    // Schema name
	Relname      string    // Table name
	SeqScan      int64     // Sequential scans
	SeqTupRead   int64     // Tuples read via sequential scans
	IdxScan      int64     // Index scans
	IdxTupFetch  int64     // Tuples fetched via index scans
	NTupIns      int64     // Tuples inserted
	NTupUpd      int64     // Tuples updated
	NTupDel      int64     // Tuples deleted
	NTupHotUpd   int64     // HOT updates
	NLiveTup     int64     // Live tuples
	NDeadTup     int64     // Dead tuples
	HeapBlksRead int64     // Heap blocks read
	HeapBlksHit  int64     // Heap blocks hit in buffer cache
	IdxBlksRead  int64     // Index blocks read
	IdxBlksHit   int64     // Index blocks hit in buffer cache
	LastUpdated  time.Time // Last update timestamp
}

// queryPgStatUserTables implements the pg_stat_user_tables system table
func (m *tableManager) queryPgStatUserTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	// Get stats collector
	statsCollector := m.GetStatsCollector()
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
			tableStats = &TableStatistics{
				RelID:       tableID,
				RelName:     table.Name,
				LastUpdated: time.Now(),
			}
		}
		
		row := []interface{}{
			table.ID,                // relid
			"public",                // schemaname
			tableStats.RelName,      // relname
			tableStats.SeqScan,      // seq_scan
			tableStats.SeqTupRead,   // seq_tup_read
			tableStats.IdxScan,      // idx_scan
			tableStats.IdxTupFetch,  // idx_tup_fetch
			tableStats.NTupIns,      // n_tup_ins
			tableStats.NTupUpd,      // n_tup_upd
			tableStats.NTupDel,      // n_tup_del
			tableStats.NTupHotUpd,   // n_tup_hot_upd
			tableStats.NLiveTup,     // n_live_tup
			tableStats.NDeadTup,     // n_dead_tup
			tableStats.HeapBlksRead, // heap_blks_read
			tableStats.HeapBlksHit,  // heap_blks_hit
			tableStats.IdxBlksRead,  // idx_blks_read
			tableStats.IdxBlksHit,   // idx_blks_hit
			tableStats.LastUpdated,  // last_updated
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

// PgStatUserIndexes represents the structure of pg_stat_user_indexes system table
type PgStatUserIndexes struct {
	RelID       string    // Table ID
	IndexRelID  string    // Index ID
	Schemaname  string    // Schema name
	Relname     string    // Table name
	Indexname   string    // Index name
	IdxScan     int64     // Index scans
	IdxTupRead  int64     // Index tuples read
	IdxTupFetch int64     // Index tuples fetched
	LastUpdated time.Time // Last update timestamp
}

// queryPgStatUserIndexes implements the pg_stat_user_indexes system table
func (m *tableManager) queryPgStatUserIndexes(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	// Get stats collector
	statsCollector := m.GetStatsCollector()
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
		
		tableSchema, err := m.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		// Get table statistics
		tableStats, err := statsCollector.GetTableStats(ctx, tableID)
		if err != nil {
			// If stats not found, create empty stats
			tableStats = &TableStatistics{
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

// PgStats represents the structure of pg_stats system table
type PgStats struct {
	Schemaname       string    // Schema name
	Tablename        string    // Table name
	Attname          string    // Column name
	Ndistinct        int64     // Number of distinct values
	NullFrac         float64   // Fraction of null values
	AvgWidth         int       // Average width in bytes
	MostCommonVals   []string  // Most common values
	MostCommonFreqs  []float64 // Frequencies of most common values
	HistogramBounds  []string  // Histogram boundary values
}

// PgStatDatabase represents the structure of pg_stat_database system table
// Provides database-wide statistics
type PgStatDatabase struct {
	DatID           uint64    // Database ID
	DatName         string    // Database name
	NumBackends     int64     // Number of backends currently connected to this database
	XactCommit      int64     // Number of transactions in this database that have been committed
	XactRollback    int64     // Number of transactions in this database that have been rolled back
	BlksRead        int64     // Number of disk blocks read in this database
	BlksHit         int64     // Number of times disk blocks were found already in the buffer cache
	TupReturned     int64     // Number of rows returned by queries in this database
	TupFetched      int64     // Number of rows fetched by queries in this database
	TupInserted     int64     // Number of rows inserted by queries in this database
	TupUpdated      int64     // Number of rows updated by queries in this database
	TupDeleted      int64     // Number of rows deleted by queries in this database
	StatsReset      time.Time // Time at which these statistics were last reset
}

// PgStatBgWriter represents the structure of pg_stat_bgwriter system table
// Provides statistics about the background writer process
type PgStatBgWriter struct {
	CheckpointsTimed    int64     // Number of scheduled checkpoints that have been performed
	CheckpointsReq      int64     // Number of requested checkpoints that have been performed
	CheckpointWriteTime float64   // Total amount of time that has been spent in the portion of checkpoint processing where files are written to disk
	CheckpointSyncTime  float64   // Total amount of time that has been spent in the portion of checkpoint processing where files are synchronized to disk
	BuffersCheckpoint   int64     // Number of buffers written during checkpoints
	BuffersClean        int64     // Number of buffers written by the background writer
	MaxWrittenClean     int64     // Number of times the background writer stopped a cleaning scan because it had written too many buffers
	BuffersBackend      int64     // Number of buffers written directly by a backend
	BuffersBackendFsync int64     // Number of times a backend had to execute its own fsync call
	BuffersAlloc        int64     // Number of buffers allocated
	StatsReset          time.Time // Time at which these statistics were last reset
}

// PgIndex represents the structure of pg_index system table
// Provides information about indexes
type PgIndex struct {
	IndexRelID     uint64   // OID of the index
	IndRelID       uint64   // OID of the table this index is for
	IndNatts       int      // Number of columns in the index
	IndIsUnique    bool     // True if this is a unique index
	IndIsPrimary   bool     // True if this is the primary key index
	IndIsClustered bool     // True if table is clustered on this index
	IndIsValid     bool     // True if this index is valid for use
	IndKey         []int    // Array of column numbers (1-based) that are indexed, or 0 for expression columns
	IndCollation   []uint64 // Array of collation OIDs for the index columns
	IndClass       []uint64 // Array of operator class OIDs for the index columns
	IndOption      []int    // Array of option values for the index columns
	IndexPred      string   // Partial index predicate, or null if none
}

// PgInherits represents the structure of pg_inherits system table
// Provides information about table inheritance hierarchies
type PgInherits struct {
	InhRelID   uint64 // OID of the child table
	InhParent  uint64 // OID of the parent table
	InhSeqNo   int    // Number of this parent among child's parents (1-based)
}

// queryPgStats implements the pg_stats system table
func (m *tableManager) queryPgStats(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	// Get stats collector
	statsCollector := m.GetStatsCollector()
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
		
		tableSchema, err := m.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		// For each column, get column statistics
		for i, column := range tableSchema.Columns {
			// Apply column name filter
			if filterAttName, ok := filter["attname"].(string); ok {
				if column.Name != filterAttName {
					continue
				}
			}
			
			// Get column statistics
			columnStats, err := statsCollector.GetColumnStats(ctx, tableID, i+1)
			if err != nil {
				// If stats not found, create empty stats
				columnStats = &ColumnStatistics{
					RelID:   tableID,
					AttNum:  i + 1,
					AttName: column.Name,
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
				"public",             // schemaname
				table.Name,           // tablename
				columnStats.AttName,  // attname
				columnStats.NDistinct,// n_distinct
				columnStats.NullFrac, // null_frac
				columnStats.AvgWidth, // avg_width
				mcvStr,               // most_common_vals
				mcfStr,               // most_common_freqs
				hbStr,                // histogram_bounds
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

// queryPgStatDatabase implements the pg_stat_database system table
func (m *tableManager) queryPgStatDatabase(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// Get stats collector
	statsCollector := m.GetStatsCollector()
	if statsCollector == nil {
		return nil, fmt.Errorf("stats collector not available")
	}
	
	// Get database statistics
	dbStats, err := statsCollector.GetDatabaseStats(ctx)
	if err != nil {
		// If stats not found, create empty stats
		dbStats = &DatabaseStatistics{
			DatID:      1,
			DatName:    "pglitedb",
			StatsReset: time.Now(),
		}
	}
	
	rows := make([][]interface{}, 0)
	
	// Apply database name filter
	if filterDatName, ok := filter["datname"].(string); ok {
		if dbStats.DatName != filterDatName {
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
		dbStats.DatID,        // datid
		dbStats.DatName,      // datname
		dbStats.NumBackends,  // numbackends
		dbStats.XactCommit,   // xact_commit
		dbStats.XactRollback, // xact_rollback
		dbStats.BlksRead,     // blks_read
		dbStats.BlksHit,      // blks_hit
		dbStats.TupReturned,  // tup_returned
		dbStats.TupFetched,   // tup_fetched
		dbStats.TupInserted,  // tup_inserted
		dbStats.TupUpdated,   // tup_updated
		dbStats.TupDeleted,   // tup_deleted
		dbStats.StatsReset,   // stats_reset
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

// queryPgStatBgWriter implements the pg_stat_bgwriter system table
func (m *tableManager) queryPgStatBgWriter(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// Get stats collector
	statsCollector := m.GetStatsCollector()
	if statsCollector == nil {
		return nil, fmt.Errorf("stats collector not available")
	}
	
	// Get database statistics to extract background writer info
	// In a full implementation, we would have separate bgwriter statistics
	dbStats, err := statsCollector.GetDatabaseStats(ctx)
	if err != nil {
		// If stats not found, create empty stats
		dbStats = &DatabaseStatistics{
			DatID:      1,
			DatName:    "pglitedb",
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

// queryPgIndex implements the pg_index system table
func (m *tableManager) queryPgIndex(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := m.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	
	// Iterate through all tables to get their indexes
	for _, table := range tables {
		tableID, err := strconv.ParseUint(table.ID, 10, 64)
		if err != nil {
			// Skip tables with invalid IDs
			continue
		}
		
		// Process each index for this table
		for i, index := range table.Indexes {
			indexID := tableID + uint64(i+1) // Simple index ID generation
			
			// Convert column names to column numbers
			indKey := make([]int, len(index.Columns))
			for j, colName := range index.Columns {
				// Find column position in table
				colNum := 0
				for k, col := range table.Columns {
					if col.Name == colName {
						colNum = k + 1 // 1-based indexing
						break
					}
				}
				indKey[j] = colNum
			}
			
			// Determine if this is a primary key or unique index
			isPrimary := false
			isUnique := index.Unique
			
			// Check if this index is for a primary key constraint
			for _, constraint := range table.Constraints {
				if constraint.Type == "primary_key" && 
				   len(constraint.Columns) == len(index.Columns) {
					match := true
					for k, col := range constraint.Columns {
						if col != index.Columns[k] {
							match = false
							break
						}
					}
					if match {
						isPrimary = true
						isUnique = true
						break
					}
				}
			}
			
			row := []interface{}{
				indexID,              // indexrelid
				tableID,              // indrelid
				len(index.Columns),   // indnatts
				isUnique,             // indisunique
				isPrimary,            // indisprimary
				false,                // indisclustered
				true,                 // indisvalid
				fmt.Sprintf("%v", indKey),               // indkey
				"{}",           // indcollation (empty for now)
				"{}",           // indclass (empty for now)
				"{}",              // indoption (empty for now)
				"",                   // indexpred (empty for now)
			}
			rows = append(rows, row)
		}
	}
	
	columns := []types.ColumnInfo{
		{Name: "indexrelid", Type: types.ColumnTypeBigInt},
		{Name: "indrelid", Type: types.ColumnTypeBigInt},
		{Name: "indnatts", Type: types.ColumnTypeInteger},
		{Name: "indisunique", Type: types.ColumnTypeBoolean},
		{Name: "indisprimary", Type: types.ColumnTypeBoolean},
		{Name: "indisclustered", Type: types.ColumnTypeBoolean},
		{Name: "indisvalid", Type: types.ColumnTypeBoolean},
		{Name: "indkey", Type: types.ColumnTypeText}, // Using Text instead of IntegerArray
		{Name: "indcollation", Type: types.ColumnTypeText}, // Using Text instead of BigIntArray
		{Name: "indclass", Type: types.ColumnTypeText}, // Using Text instead of BigIntArray
		{Name: "indoption", Type: types.ColumnTypeText}, // Using Text instead of IntegerArray
		{Name: "indexpred", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// queryPgInherits implements the pg_inherits system table
func (m *tableManager) queryPgInherits(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// For now, we'll return an empty result since we don't have table inheritance
	// In a full implementation, we would query actual inheritance relationships
	
	rows := make([][]interface{}, 0)
	
	columns := []types.ColumnInfo{
		{Name: "inhrelid", Type: types.ColumnTypeBigInt},
		{Name: "inhparent", Type: types.ColumnTypeBigInt},
		{Name: "inhseqno", Type: types.ColumnTypeInteger},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// queryPgViews implements the pg_views system table
func (m *tableManager) queryPgViews(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	
	// For now, we'll return an empty result since we don't have view metadata stored separately
	// In a full implementation, we would query the actual views
	
	rows := make([][]interface{}, 0)
	
	columns := []types.ColumnInfo{
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "viewname", Type: types.ColumnTypeText},
		{Name: "viewowner", Type: types.ColumnTypeText},
		{Name: "definition", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}