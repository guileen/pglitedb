package batch

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	dbTypes "github.com/guileen/pglitedb/types"
)

// BatchProcessorConfig holds configuration for batch processing
type BatchProcessorConfig struct {
	MaxBatchSize     int // Maximum batch size
	MinBatchSize     int // Minimum batch size
	TargetBatchSize  int // Target batch size for optimal performance
	AdaptiveBatching bool // Whether to use adaptive batching
}

// DefaultBatchProcessorConfig returns the default batch processor configuration
func DefaultBatchProcessorConfig() *BatchProcessorConfig {
	return &BatchProcessorConfig{
		MaxBatchSize:     50000,  // Increased from 10000
		MinBatchSize:     500,    // Increased from 100
		TargetBatchSize:  5000,   // Increased from 1000
		AdaptiveBatching: true,
	}
}

// BatchProcessorImpl implements batch processing operations
type BatchProcessorImpl struct {
	kv     storage.KV
	codec  codec.Codec
	config *BatchProcessorConfig
	stats  *BatchProcessorStats
}

// BatchProcessorStats holds statistics for batch processing
type BatchProcessorStats struct {
	TotalBatchesProcessed int64
	TotalRowsProcessed    int64
	AverageBatchSize      float64
	QueueLength           int64
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(kv storage.KV, codec codec.Codec) *BatchProcessorImpl {
	return &BatchProcessorImpl{
		kv:     kv,
		codec:  codec,
		config: DefaultBatchProcessorConfig(),
		stats:  &BatchProcessorStats{},
	}
}

// NewBatchProcessorWithConfig creates a new batch processor with custom configuration
func NewBatchProcessorWithConfig(kv storage.KV, codec codec.Codec, config *BatchProcessorConfig) *BatchProcessorImpl {
	return &BatchProcessorImpl{
		kv:     kv,
		codec:  codec,
		config: config,
		stats:  &BatchProcessorStats{},
	}
}

// ProcessBatchInsert processes a batch insert operation
func (bp *BatchProcessorImpl) ProcessBatchInsert(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}

	// Pre-allocate rowIDs slice with exact capacity
	rowIDs := make([]int64, len(rows))
	
	// Generate all row IDs first to minimize lock contention
	for i := range rows {
		rowID, err := bp.generateRowID(ctx, tenantID, tableID)
		if err != nil {
			return nil, fmt.Errorf("generate row id: %w", err)
		}
		rowIDs[i] = rowID
	}

	batch := bp.kv.NewBatch()
	defer batch.Close()

	// Process rows in batch with reduced allocations
	for i, row := range rows {
		key := bp.codec.EncodeTableKey(tenantID, tableID, rowIDs[i])
		value, err := bp.codec.EncodeRow(row, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("encode row %d: %w", i, err)
		}

		if err := batch.Set(key, value); err != nil {
			return nil, fmt.Errorf("batch set row %d: %w", i, err)
		}
	}

	if err := bp.kv.Commit(ctx, batch); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	// Update statistics
	atomic.AddInt64(&bp.stats.TotalBatchesProcessed, 1)
	atomic.AddInt64(&bp.stats.TotalRowsProcessed, int64(len(rows)))

	return rowIDs, nil
}

// ProcessBatchUpdate processes a batch update operation
func (bp *BatchProcessorImpl) ProcessBatchUpdate(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	if len(updates) == 0 {
		return nil
	}

	rowIDs := make([]int64, len(updates))
	for i, update := range updates {
		rowIDs[i] = update.RowID
	}

	oldRows, err := bp.getRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := bp.kv.NewBatch()
	defer batch.Close()

	updatedRows := make(map[int64]*dbTypes.Record, len(updates))
	for _, update := range updates {
		oldRow, ok := oldRows[update.RowID]
		if !ok {
			return fmt.Errorf("row %d not found", update.RowID)
		}

		for colName, newValue := range update.Updates {
			oldRow.Data[colName] = newValue
		}
		updatedRows[update.RowID] = oldRow

		value, err := bp.codec.EncodeRow(oldRow, schemaDef)
		if err != nil {
			return fmt.Errorf("encode row %d: %w", update.RowID, err)
		}

		key := bp.codec.EncodeTableKey(tenantID, tableID, update.RowID)
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("batch set row %d: %w", update.RowID, err)
		}
	}

	if err := bp.kv.CommitBatchWithOptions(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// ProcessBatchDelete processes a batch delete operation
func (bp *BatchProcessorImpl) ProcessBatchDelete(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if len(rowIDs) == 0 {
		return nil
	}

	oldRows, err := bp.getRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := bp.kv.NewBatch()
	defer batch.Close()

	for _, rowID := range rowIDs {
		if _, ok := oldRows[rowID]; !ok {
			continue
		}

		key := bp.codec.EncodeTableKey(tenantID, tableID, rowID)
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("batch delete row %d: %w", rowID, err)
		}
	}

	if err := bp.kv.CommitBatchWithOptions(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// generateRowID generates a new row ID
func (bp *BatchProcessorImpl) generateRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	// This is a placeholder implementation
	// In a real implementation, this would use a proper ID generator
	return 0, fmt.Errorf("not implemented")
}

// getRowBatch retrieves multiple rows by their IDs
func (bp *BatchProcessorImpl) getRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error) {
	// This is a simplified implementation
	// In a real implementation, this would be optimized
	result := make(map[int64]*dbTypes.Record)
	for _, rowID := range rowIDs {
		row, err := bp.getRow(ctx, tenantID, tableID, rowID, schemaDef)
		if err != nil {
			return nil, err
		}
		result[rowID] = row
	}
	return result, nil
}

// getRow retrieves a single row by its ID
func (bp *BatchProcessorImpl) getRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	key := bp.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := bp.kv.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, fmt.Errorf("record not found")
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := bp.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

// Helper functions for min/max
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// getOptimalBatchSize determines the optimal batch size based on current queue length and configuration
func (bp *BatchProcessorImpl) getOptimalBatchSize(currentQueueLength int) int {
	if !bp.config.AdaptiveBatching {
		return bp.config.TargetBatchSize
	}
	
	// Adjust batch size based on queue length
	if currentQueueLength > bp.config.MaxBatchSize*2 {
		// High queue load, increase batch size to reduce queue pressure
		return min(bp.config.MaxBatchSize, bp.config.TargetBatchSize*2)
	} else if currentQueueLength < bp.config.MinBatchSize {
		// Low queue load, decrease batch size for better responsiveness
		return max(bp.config.MinBatchSize, bp.config.TargetBatchSize/2)
	}
	
	// Normal conditions, use target batch size
	return bp.config.TargetBatchSize
}

// processBatchInsertInChunks processes large batch inserts in smaller chunks
func (bp *BatchProcessorImpl) processBatchInsertInChunks(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition, chunkSize int) ([]int64, error) {
	totalRows := len(rows)
	allRowIDs := make([]int64, 0, totalRows)
	
	for i := 0; i < totalRows; i += chunkSize {
		end := i + chunkSize
		if end > totalRows {
			end = totalRows
		}
		
		chunk := rows[i:end]
		rowIDs, err := bp.ProcessBatchInsert(ctx, tenantID, tableID, chunk, schemaDef)
		if err != nil {
			return nil, err
		}
		
		allRowIDs = append(allRowIDs, rowIDs...)
	}
	
	return allRowIDs, nil
}

// GetStats returns the current batch processor statistics
func (bp *BatchProcessorImpl) GetStats() *BatchProcessorStats {
	return &BatchProcessorStats{
		TotalBatchesProcessed: atomic.LoadInt64(&bp.stats.TotalBatchesProcessed),
		TotalRowsProcessed:    atomic.LoadInt64(&bp.stats.TotalRowsProcessed),
		AverageBatchSize:      bp.calculateAverageBatchSize(),
		QueueLength:           atomic.LoadInt64(&bp.stats.QueueLength),
	}
}

// calculateAverageBatchSize calculates the average batch size
func (bp *BatchProcessorImpl) calculateAverageBatchSize() float64 {
	totalBatches := atomic.LoadInt64(&bp.stats.TotalBatchesProcessed)
	totalRows := atomic.LoadInt64(&bp.stats.TotalRowsProcessed)
	
	if totalBatches == 0 {
		return 0
	}
	
	return float64(totalRows) / float64(totalBatches)
}