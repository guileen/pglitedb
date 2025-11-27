package catalog

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/types"
)

// TableStatistics represents table-level statistics
// These statistics are used by the query optimizer to make cost-based decisions
type TableStatistics struct {
	RelID        uint64    // Table ID
	RelName      string    // Table name
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

// ColumnStatistics represents column-level statistics
// These statistics help the query optimizer understand data distribution for better planning
type ColumnStatistics struct {
	RelID           uint64    // Table ID
	AttNum          int       // Column number
	AttName         string    // Column name
	NDistinct       int64     // Number of distinct values
	NullFrac        float64   // Fraction of null values
	AvgWidth        int       // Average width in bytes
	MostCommonVals  []string  // Most common values
	MostCommonFreqs []float64 // Frequencies of most common values
	HistogramBounds []string  // Histogram boundary values
}

// DatabaseStatistics represents database-wide statistics
// These statistics provide insights into overall database performance and usage
type DatabaseStatistics struct {
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

// StatsCollector defines the interface for statistics collection
// It provides methods to collect, retrieve, and manage both table and column level statistics
type StatsCollector interface {
	// CollectTableStats collects comprehensive table statistics from the storage engine
	CollectTableStats(ctx context.Context, tableID uint64) (*TableStatistics, error)

	// CollectColumnStats collects detailed column statistics using sampling techniques
	CollectColumnStats(ctx context.Context, tableID uint64, columnID int) (*ColumnStatistics, error)

	// UpdateStats updates existing table statistics
	UpdateStats(ctx context.Context, tableID uint64, stats *TableStatistics) error

	// GetTableStats retrieves previously collected table statistics
	GetTableStats(ctx context.Context, tableID uint64) (*TableStatistics, error)

	// GetColumnStats retrieves previously collected column statistics
	GetColumnStats(ctx context.Context, tableID uint64, columnID int) (*ColumnStatistics, error)

	// DeleteStats deletes all statistics for a table
	DeleteStats(ctx context.Context, tableID uint64) error

	// GetDatabaseStats retrieves database-wide statistics
	GetDatabaseStats(ctx context.Context) (*DatabaseStatistics, error)

	// UpdateDatabaseStats updates database-wide statistics
	UpdateDatabaseStats(ctx context.Context, stats *DatabaseStatistics) error
}

// statsCollector implements the StatsCollector interface
type statsCollector struct {
	manager Manager
	mu      sync.RWMutex
	stats   map[uint64]*tableStatsEntry // Map of tableID to table stats
	dbStats *DatabaseStatistics         // Database-wide statistics
}

// tableStatsEntry holds table stats and associated column stats
type tableStatsEntry struct {
	tableStats  *TableStatistics
	columnStats map[int]*ColumnStatistics
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector(manager Manager) StatsCollector {
	return &statsCollector{
		manager: manager,
		stats:   make(map[uint64]*tableStatsEntry),
		dbStats: &DatabaseStatistics{
			DatID:      1,
			DatName:    "pglitedb",
			StatsReset: time.Now(),
		},
	}
}

// CollectTableStats implements StatsCollector
func (sc *statsCollector) CollectTableStats(ctx context.Context, tableID uint64) (*TableStatistics, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Initialize stats entry if not exists
	if _, exists := sc.stats[tableID]; !exists {
		sc.stats[tableID] = &tableStatsEntry{
			tableStats:  &TableStatistics{},
			columnStats: make(map[int]*ColumnStatistics),
		}
	}

	// Create table stats
	stats := &TableStatistics{
		RelID:       tableID,
		RelName:     fmt.Sprintf("table_%d", tableID),
		LastUpdated: time.Now(),
	}

	// If we have a manager, try to get actual table definition
	if sc.manager != nil {
		tableDef, err := sc.manager.GetTableDefinition(ctx, 0, fmt.Sprintf("%d", tableID))
		if err == nil {
			stats.RelName = tableDef.Name
			// Collect actual table statistics from storage engine
			err = sc.collectActualTableStats(ctx, tableID, tableDef, stats)
			if err != nil {
				return nil, fmt.Errorf("failed to collect actual table stats: %w", err)
			}
		}
	}

	// Store the collected stats
	sc.stats[tableID].tableStats = stats

	return stats, nil
}

// CollectColumnStats implements StatsCollector
func (sc *statsCollector) CollectColumnStats(ctx context.Context, tableID uint64, columnID int) (*ColumnStatistics, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Initialize stats entry if not exists
	if _, exists := sc.stats[tableID]; !exists {
		sc.stats[tableID] = &tableStatsEntry{
			tableStats:  &TableStatistics{},
			columnStats: make(map[int]*ColumnStatistics),
		}
	}

	// Create column stats
	stats := &ColumnStatistics{
		RelID:   tableID,
		AttNum:  columnID,
		AttName: fmt.Sprintf("column_%d", columnID),
	}

	// If we have a manager, try to get actual table definition
	if sc.manager != nil {
		tableDef, err := sc.manager.GetTableDefinition(ctx, 0, fmt.Sprintf("%d", tableID))
		if err == nil {
			// Validate column ID
			if columnID > 0 && columnID <= len(tableDef.Columns) {
				columnDef := tableDef.Columns[columnID-1]
				stats.AttName = columnDef.Name
				
				// Collect actual column statistics from storage engine
				err = sc.collectActualColumnStats(ctx, tableID, tableDef, columnID, stats)
				if err != nil {
					return nil, fmt.Errorf("failed to collect actual column stats: %w", err)
				}
			}
		}
	}

	// Store the collected stats
	sc.stats[tableID].columnStats[columnID] = stats

	return stats, nil
}

// UpdateStats implements StatsCollector
func (sc *statsCollector) UpdateStats(ctx context.Context, tableID uint64, stats *TableStatistics) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Initialize stats entry if not exists
	if _, exists := sc.stats[tableID]; !exists {
		sc.stats[tableID] = &tableStatsEntry{
			tableStats:  &TableStatistics{},
			columnStats: make(map[int]*ColumnStatistics),
		}
	}

	// Update the stats
	sc.stats[tableID].tableStats = stats

	return nil
}

// GetTableStats implements StatsCollector
func (sc *statsCollector) GetTableStats(ctx context.Context, tableID uint64) (*TableStatistics, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	entry, exists := sc.stats[tableID]
	if !exists {
		return nil, fmt.Errorf("statistics not found for table %d", tableID)
	}

	return entry.tableStats, nil
}

// GetColumnStats implements StatsCollector
func (sc *statsCollector) GetColumnStats(ctx context.Context, tableID uint64, columnID int) (*ColumnStatistics, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	entry, exists := sc.stats[tableID]
	if !exists {
		return nil, fmt.Errorf("statistics not found for table %d", tableID)
	}

	colStats, exists := entry.columnStats[columnID]
	if !exists {
		return nil, fmt.Errorf("column statistics not found for table %d, column %d", tableID, columnID)
	}

	return colStats, nil
}

// DeleteStats implements StatsCollector
func (sc *statsCollector) DeleteStats(ctx context.Context, tableID uint64) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.stats, tableID)
	return nil
}

// GetDatabaseStats implements StatsCollector
func (sc *statsCollector) GetDatabaseStats(ctx context.Context) (*DatabaseStatistics, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	
	// Return a copy of the database statistics
	if sc.dbStats == nil {
		return nil, fmt.Errorf("database statistics not available")
	}
	
	// Create a copy to avoid race conditions
	stats := &DatabaseStatistics{
		DatID:           sc.dbStats.DatID,
		DatName:         sc.dbStats.DatName,
		NumBackends:     sc.dbStats.NumBackends,
		XactCommit:      sc.dbStats.XactCommit,
		XactRollback:    sc.dbStats.XactRollback,
		BlksRead:        sc.dbStats.BlksRead,
		BlksHit:         sc.dbStats.BlksHit,
		TupReturned:     sc.dbStats.TupReturned,
		TupFetched:      sc.dbStats.TupFetched,
		TupInserted:     sc.dbStats.TupInserted,
		TupUpdated:      sc.dbStats.TupUpdated,
		TupDeleted:      sc.dbStats.TupDeleted,
		StatsReset:      sc.dbStats.StatsReset,
	}
	
	return stats, nil
}

// UpdateDatabaseStats implements StatsCollector
func (sc *statsCollector) UpdateDatabaseStats(ctx context.Context, stats *DatabaseStatistics) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	
	// Update the database statistics
	sc.dbStats = stats
	return nil
}

// collectActualTableStats collects actual table statistics from the storage engine
// This method scans through all rows in the table to gather comprehensive table-level statistics
// including tuple counts, page counts, and other table-level metrics.
func (sc *statsCollector) collectActualTableStats(ctx context.Context, tableID uint64, tableDef *types.TableDefinition, stats *TableStatistics) error {
	// Cast manager to access storage engine
	tableManager, ok := sc.manager.(*tableManager)
	if !ok {
		return fmt.Errorf("manager is not of type *tableManager")
	}

	// Get storage engine
	eng := tableManager.engine

	// Scan all rows to collect statistics
	scanOpts := &engine.ScanOptions{
		Limit: 1000, // Process in batches to avoid memory issues
	}

	// Initialize counters
	var pageCount, tupleCount int64
	var heapBlksRead, heapBlksHit int64
	offset := 0
	
	// For sampling, we'll scan all data for now
	// In a production environment, we might want to implement sampling strategies
	for {
		scanOpts.Offset = offset
		iterator, err := eng.ScanRows(ctx, 0, int64(tableID), tableDef, scanOpts)
		if err != nil {
			return fmt.Errorf("failed to scan rows: %w", err)
		}

		// Process batch
		batchCount := 0
		for iterator.Next() {
			batchCount++
			tupleCount++
			
			// Process row for statistics
			row := iterator.Row()
			if row != nil {
				// Count non-nil rows
				// In a real implementation, we might track more detailed statistics
			}
		}

		// Check for iteration errors
		if err := iterator.Error(); err != nil {
			iterator.Close()
			return fmt.Errorf("error during row iteration: %w", err)
		}

		// Close iterator
		iterator.Close()

		// Update page count estimate (simplified)
		pageCount += int64((batchCount + 999) / 1000) // Rough estimate
		heapBlksRead += pageCount
		heapBlksHit += pageCount // Simplified assumption

		// Break if we've processed all rows
		if batchCount < scanOpts.Limit {
			break
		}

		// Update offset for next batch
		offset += batchCount
	}

	// Update statistics
	stats.NLiveTup = tupleCount
	stats.HeapBlksRead = heapBlksRead
	stats.HeapBlksHit = heapBlksHit
	stats.LastUpdated = time.Now()

	return nil
}

// collectActualColumnStats collects actual column statistics using sampling
// This method implements a basic sampling algorithm to efficiently gather column statistics
// without scanning the entire table. It calculates null fraction, average width, distinct values,
// most common values, and histogram bounds.
func (sc *statsCollector) collectActualColumnStats(ctx context.Context, tableID uint64, tableDef *types.TableDefinition, columnID int, stats *ColumnStatistics) error {
	// Cast manager to access storage engine
	tableManager, ok := sc.manager.(*tableManager)
	if !ok {
		return fmt.Errorf("manager is not of type *tableManager")
	}

	// Get storage engine
	eng := tableManager.engine

	// Get column definition
	if columnID <= 0 || columnID > len(tableDef.Columns) {
		return fmt.Errorf("invalid column ID: %d", columnID)
	}
	columnDef := tableDef.Columns[columnID-1]
	stats.AttName = columnDef.Name

	// Sample data to collect statistics
	// For demonstration, we'll use a simple random sampling approach
	sampleSize := 1000 // Configurable sample size
	values := make([]interface{}, 0, sampleSize)
	nullCount := 0
	totalCount := 0

	// Scan rows with limit for sampling
	scanOpts := &engine.ScanOptions{
		Limit: sampleSize,
	}

	iterator, err := eng.ScanRows(ctx, 0, int64(tableID), tableDef, scanOpts)
	if err != nil {
		return fmt.Errorf("failed to scan rows: %w", err)
	}
	defer iterator.Close()

	// Collect sample data
	for iterator.Next() {
		totalCount++
		row := iterator.Row()
		
		if row != nil && row.Data != nil {
			// Get value for the specific column
			value, exists := row.Data[columnDef.Name]
			if !exists || value == nil || value.Data == nil {
				nullCount++
				continue
			}
			
			values = append(values, value.Data)
		} else {
			nullCount++
		}
	}

	// Check for iteration errors
	if err := iterator.Error(); err != nil {
		return fmt.Errorf("error during row iteration: %w", err)
	}

	// Calculate statistics based on sampled data
	if totalCount > 0 {
		stats.NullFrac = float64(nullCount) / float64(totalCount)
	}

	// Calculate average width
	if len(values) > 0 {
		totalWidth := 0
		for _, v := range values {
			// Simplified width calculation
			switch val := v.(type) {
			case string:
				totalWidth += len(val)
			case []byte:
				totalWidth += len(val)
			case int, int32, int64:
				totalWidth += 8 // Assume 8 bytes for integers
			case float32:
				totalWidth += 4
			case float64:
				totalWidth += 8
			case bool:
				totalWidth += 1
			default:
				totalWidth += 16 // Default estimate
			}
		}
		stats.AvgWidth = totalWidth / len(values)
	}

	// Calculate distinct values and most common values
	distinctValues := make(map[string]int)
	for _, v := range values {
		strVal := fmt.Sprintf("%v", v)
		distinctValues[strVal]++
	}

	stats.NDistinct = int64(len(distinctValues))

	// Find most common values (top 10)
	type freqPair struct {
		value string
		count int
	}
	freqPairs := make([]freqPair, 0, len(distinctValues))
	for value, count := range distinctValues {
		freqPairs = append(freqPairs, freqPair{value, count})
	}

	// Sort by frequency (descending)
	sort.Slice(freqPairs, func(i, j int) bool {
		return freqPairs[i].count > freqPairs[j].count
	})

	// Take top 10 most common values
	maxCommonVals := 10
	if len(freqPairs) < maxCommonVals {
		maxCommonVals = len(freqPairs)
	}

	stats.MostCommonVals = make([]string, 0, maxCommonVals)
	stats.MostCommonFreqs = make([]float64, 0, maxCommonVals)
	
	for i := 0; i < maxCommonVals; i++ {
		stats.MostCommonVals = append(stats.MostCommonVals, freqPairs[i].value)
		freq := float64(freqPairs[i].count) / float64(len(values))
		stats.MostCommonFreqs = append(stats.MostCommonFreqs, freq)
	}

	// Generate histogram bounds (simplified quartiles)
	if len(values) > 0 {
		// For simplicity, we'll just take min, 25%, 50%, 75%, max as histogram bounds
		// In a real implementation, this would be more sophisticated
		stats.HistogramBounds = make([]string, 0, 5)
		
		// Convert values to strings for histogram (simplified)
		strValues := make([]string, len(values))
		for i, v := range values {
			strValues[i] = fmt.Sprintf("%v", v)
		}
		
		// Sort string values (this is a simplification)
		// In practice, we'd sort by actual value type
		sort.Strings(strValues)
		
		// Take boundary values
		if len(strValues) > 0 {
			stats.HistogramBounds = append(stats.HistogramBounds, strValues[0]) // Min
			
			if len(strValues) >= 4 {
				stats.HistogramBounds = append(stats.HistogramBounds, strValues[len(strValues)/4])   // 25%
				stats.HistogramBounds = append(stats.HistogramBounds, strValues[len(strValues)/2])   // 50%
				stats.HistogramBounds = append(stats.HistogramBounds, strValues[3*len(strValues)/4]) // 75%
			}
			
			stats.HistogramBounds = append(stats.HistogramBounds, strValues[len(strValues)-1]) // Max
		}
	}

	return nil
}

// simpleRandomSample performs a simple random sampling of rows
// This method implements a basic random sampling algorithm that can be used
// for statistical analysis. For large tables, a more sophisticated reservoir
// sampling approach would be more appropriate.
func (sc *statsCollector) simpleRandomSample(ctx context.Context, eng engine.StorageEngine, tableID uint64, tableDef *types.TableDefinition, sampleSize int) ([]*types.Record, error) {
	// First, get total count of rows (simplified approach)
	scanOpts := &engine.ScanOptions{
		Limit: sampleSize,
	}
	
	iterator, err := eng.ScanRows(ctx, 0, int64(tableID), tableDef, scanOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to scan rows: %w", err)
	}
	defer iterator.Close()

	// Collect all rows (in a real implementation, we'd use reservoir sampling for large tables)
	rows := make([]*types.Record, 0, sampleSize)
	for iterator.Next() {
		row := iterator.Row()
		if row != nil {
			// Create a copy of the row to avoid reference issues
			rowCopy := &types.Record{
				ID:        row.ID,
				Table:     row.Table,
				Data:      make(map[string]*types.Value),
				Metadata:  row.Metadata,
				CreatedAt: row.CreatedAt,
				UpdatedAt: row.UpdatedAt,
				Version:   row.Version,
			}
			
			// Copy data
			for k, v := range row.Data {
				if v != nil {
					valCopy := &types.Value{
						Data: v.Data,
						Type: v.Type,
					}
					rowCopy.Data[k] = valCopy
				} else {
					rowCopy.Data[k] = nil
				}
			}
			
			rows = append(rows, rowCopy)
		}
	}

	// Check for iteration errors
	if err := iterator.Error(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	// If we have more rows than sample size, randomly select sampleSize rows
	if len(rows) > sampleSize {
		rand.Shuffle(len(rows), func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		
		rows = rows[:sampleSize]
	}

	return rows, nil
}