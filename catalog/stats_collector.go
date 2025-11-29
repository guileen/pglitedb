package catalog

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)









// statsCollector implements the StatsCollector interface
type statsCollector struct {
	manager interfaces.TableManager
	mu      sync.RWMutex
	stats   map[uint64]*tableStatsEntry // Map of tableID to table stats
	dbStats *interfaces.DatabaseStatistics         // Database-wide statistics
}

// tableStatsEntry holds table stats and associated column stats
type tableStatsEntry struct {
	tableStats  *interfaces.TableStatistics
	columnStats map[string]*interfaces.ColumnStatistics
}

// initStatsEntryIfNeeded initializes a stats entry for the given tableID if it doesn't exist
func (sc *statsCollector) initStatsEntryIfNeeded(tableID uint64) {
	if _, exists := sc.stats[tableID]; !exists {
		sc.stats[tableID] = &tableStatsEntry{
			tableStats:  &interfaces.TableStatistics{},
			columnStats: make(map[string]*interfaces.ColumnStatistics),
		}
	}
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector(manager interfaces.TableManager) interfaces.StatsManager {
	return &statsCollector{
		manager: manager,
		stats:   make(map[uint64]*tableStatsEntry),
		dbStats: &interfaces.DatabaseStatistics{
			DBName:               "pglitedb",
			NumBackends:          0,
			XactCommit:           0,
			XactRollback:         0,
			BlksRead:             0,
			BlksHit:              0,
			TupReturned:          0,
			TupFetched:           0,
			TupInserted:          0,
			TupUpdated:           0,
			TupDeleted:           0,
			Conflicts:            0,
			TempFiles:            0,
			TempBytes:            0,
			Deadlocks:            0,
			BlkReadTime:          0,
			BlkWriteTime:         0,
			StatsReset:           time.Now(),
		},
	}
}

// CollectTableStats implements StatsCollector
func (sc *statsCollector) CollectTableStats(ctx context.Context, tableID uint64) (*interfaces.TableStatistics, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Initialize stats entry if not exists
	sc.initStatsEntryIfNeeded(tableID)

	// Create table stats with default values
	stats := &interfaces.TableStatistics{
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
func (sc *statsCollector) CollectColumnStats(ctx context.Context, tableID uint64, columnName string) (*interfaces.ColumnStatistics, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Initialize stats entry if not exists
	sc.initStatsEntryIfNeeded(tableID)

	// Create column stats
	stats := &interfaces.ColumnStatistics{
		TableID:     tableID,
		ColumnName:  columnName,
		LastUpdated: time.Now(),
	}

	// If we have a manager, try to get actual table definition
	if sc.manager != nil {
		tableDef, err := sc.manager.GetTableDefinition(ctx, 0, fmt.Sprintf("%d", tableID))
		if err == nil {
			// Find the column by name
			for i, columnDef := range tableDef.Columns {
				if columnDef.Name == columnName {
					stats.ColumnName = columnDef.Name

					// Collect actual column statistics from storage engine
					err = sc.collectActualColumnStats(ctx, tableID, tableDef, i+1, stats)
					if err != nil {
						return nil, fmt.Errorf("failed to collect actual column stats: %w", err)
					}
					break
				}
			}
		}
	}

	// Store the collected stats
	sc.stats[tableID].columnStats[columnName] = stats

	return stats, nil
}

// UpdateStats implements StatsCollector
func (sc *statsCollector) UpdateStats(ctx context.Context, tableID uint64, stats *interfaces.TableStatistics) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Initialize stats entry if not exists
	sc.initStatsEntryIfNeeded(tableID)

	// Update the stats
	sc.stats[tableID].tableStats = stats

	return nil
}

// GetTableStats implements StatsCollector
func (sc *statsCollector) GetTableStats(ctx context.Context, tableID uint64) (*interfaces.TableStatistics, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	entry, exists := sc.stats[tableID]
	if !exists {
		return nil, fmt.Errorf("statistics not found for table %d", tableID)
	}

	// Return the table statistics directly
	return entry.tableStats, nil
}

// GetColumnStats implements StatsCollector
func (sc *statsCollector) GetColumnStats(ctx context.Context, tableID uint64, columnName string) (*interfaces.ColumnStatistics, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	entry, exists := sc.stats[tableID]
	if !exists {
		return nil, fmt.Errorf("statistics not found for table %d", tableID)
	}

	colStats, exists := entry.columnStats[columnName]
	if !exists {
		return nil, fmt.Errorf("column statistics not found for table %d, column %s", tableID, columnName)
	}

	// Return the column statistics directly
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
func (sc *statsCollector) GetDatabaseStats(ctx context.Context) (*interfaces.DatabaseStatistics, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	
	// Return the database statistics directly
	if sc.dbStats == nil {
		return nil, fmt.Errorf("database statistics not available")
	}
	
	return sc.dbStats, nil
}

// UpdateDatabaseStats implements StatsCollector
func (sc *statsCollector) UpdateDatabaseStats(ctx context.Context, stats *interfaces.DatabaseStatistics) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	
	// Update the database statistics
	sc.dbStats = stats
	return nil
}

// collectActualTableStats collects actual table statistics from the storage engine
// This method uses efficient counting mechanisms instead of scanning all rows
func (sc *statsCollector) collectActualTableStats(ctx context.Context, tableID uint64, tableDef *types.TableDefinition, stats *interfaces.TableStatistics) error {
	// Early exit if no manager
	if sc.manager == nil {
		return nil // No stats to collect
	}

	// Cast manager to access storage engine
	tableManager, ok := sc.manager.(interfaces.TableManager)
	if !ok {
		return fmt.Errorf("manager is not of type TableManager")
	}

	// Get storage engine
	eng := tableManager.GetEngine()
	if eng == nil {
		return nil // No engine, no stats to collect
	}

	// Use a more efficient approach: limit scanning to a smaller number of rows
	// This prevents performance issues with large tables during stats collection
	// Reduced from 10,000 to 1,000 for better performance
	scanOpts := &engineTypes.ScanOptions{
		Limit: 1000, // Limit to 1,000 rows maximum for stats collection
	}

	// Initialize counter
	var tupleCount int64
	
	iterator, err := eng.ScanRows(ctx, 0, int64(tableID), tableDef, scanOpts)
	if err != nil {
		return fmt.Errorf("failed to scan rows: %w", err)
	}
	defer iterator.Close() // Ensure iterator is closed

	// Process rows
	for iterator.Next() {
		tupleCount++
	}

	// Check for iteration errors
	if err := iterator.Error(); err != nil {
		return fmt.Errorf("error during row iteration: %w", err)
	}

	// Update statistics
	stats.NLiveTup = tupleCount
	stats.LastUpdated = time.Now()

	return nil
}

// collectActualColumnStats collects actual column statistics using sampling
// This method implements a basic sampling algorithm to efficiently gather column statistics
// without scanning the entire table. It calculates null fraction, average width, distinct values,
// most common values, and histogram bounds.
func (sc *statsCollector) collectActualColumnStats(ctx context.Context, tableID uint64, tableDef *types.TableDefinition, columnID int, stats *interfaces.ColumnStatistics) error {
	// Early exit if no manager
	if sc.manager == nil {
		return nil // No stats to collect
	}

	// Cast manager to access storage engine
	tableManager, ok := sc.manager.(interfaces.TableManager)
	if !ok {
		return fmt.Errorf("manager is not of type TableManager")
	}

	// Get storage engine
	eng := tableManager.GetEngine()
	if eng == nil {
		return nil // No engine, no stats to collect
	}

	// Get column definition
	if columnID <= 0 || columnID > len(tableDef.Columns) {
		return fmt.Errorf("invalid column ID: %d", columnID)
	}
	columnDef := tableDef.Columns[columnID-1]
	stats.ColumnName = columnDef.Name

	// Sample data to collect statistics
	// For demonstration, we'll use a simple random sampling approach
	// Reduced sample size from 100 to 50 for better performance
	sampleSize := 50
	values := make([]interface{}, 0, sampleSize)
	nullCount := 0
	totalCount := 0

	// Scan rows with limit for sampling
	scanOpts := &engineTypes.ScanOptions{
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

	// Early exit if no data
	if totalCount == 0 {
		return nil
	}

	// Calculate statistics based on sampled data
	stats.NullFrac = float64(nullCount) / float64(totalCount)

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
		stats.AvgWidth = int32(totalWidth / len(values))
	}

	// Calculate distinct values and most common values
	distinctValues := make(map[string]int)
	for _, v := range values {
		strVal := fmt.Sprintf("%v", v)
		distinctValues[strVal]++
	}

	stats.NDistinct = float64(len(distinctValues))

	// Find most common values (top 3 to reduce memory usage)
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

	// Take top 3 most common values (reduced from 5 to 3)
	maxCommonVals := 3
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
		// For simplicity, we'll just take min, 50%, max as histogram bounds
		// Reduced from 5 to 3 bounds for better performance
		stats.HistogramBounds = make([]string, 0, 3)
		
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
			
			if len(strValues) >= 2 {
				stats.HistogramBounds = append(stats.HistogramBounds, strValues[len(strValues)/2]) // 50%
			}
			
			stats.HistogramBounds = append(stats.HistogramBounds, strValues[len(strValues)-1]) // Max
		}
	}

	return nil
}
