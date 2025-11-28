package pebble

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// Scanner handles row and index scanning operations
type Scanner struct {
	kv    storage.KV
	codec codec.Codec
	engine interface {
		EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
		GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error)
	}
}

// NewScanner creates a new Scanner
func NewScanner(kv storage.KV, c codec.Codec, engine interface {
	EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
	GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error)
}) *Scanner {
	return &Scanner{
		kv:    kv,
		codec: c,
		engine: engine,
	}
}

// ScanRows performs a table scan
func (s *Scanner) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		startKey = s.codec.EncodeTableKey(tenantID, tableID, 0)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		endKey = s.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := s.kv.NewIterator(iterOpts)

	return scan.NewRowIterator(iter, s.codec, schemaDef, opts, s.engine), nil
}

// ScanIndex performs an index scan
func (s *Scanner) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	// Find the index definition to get column types
	var indexDef *dbTypes.IndexDefinition
	for i, idx := range schemaDef.Indexes {
		if int64(i+1) == indexID {
			indexDef = &idx
			break
		}
	}

	if indexDef == nil {
		return nil, fmt.Errorf("index not found: %d", indexID)
	}

	// Get column types for the index columns
	columnTypes := make([]dbTypes.ColumnType, len(indexDef.Columns))
	for i, colName := range indexDef.Columns {
		found := false
		for _, col := range schemaDef.Columns {
			if col.Name == colName {
				columnTypes[i] = col.Type
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found in schema: %s", colName)
		}
	}

	var startKey, endKey []byte

	if opts != nil && opts.Filter != nil {
		startKey, endKey = s.buildIndexRangeFromFilter(tenantID, tableID, indexID, opts.Filter, indexDef)
	} else if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
		if opts.EndKey != nil {
			endKey = opts.EndKey
		} else {
			endKey = s.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
	} else {
		startKey = s.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		endKey = s.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := s.kv.NewIterator(iterOpts)
	
	// Check if this is an index-only scan (covering index)
	isCovering := false
	if opts != nil && opts.Projection != nil && len(opts.Projection) > 0 {
		isCovering = s.isIndexCovering(indexDef, opts.Projection)
	}
	
	if isCovering {
		return scan.NewIndexOnlyIterator(
			iter,
			s.codec,
			indexDef,
			opts.Projection,
			opts,
			columnTypes,
			tenantID,
			tableID,
			indexID,
			s.engine,
		), nil
	}

	return scan.NewIndexIterator(
		iter,
		s.codec,
		schemaDef,
		opts,
		columnTypes,
		tenantID,
		tableID,
		s.engine,
	), nil
}

// buildIndexRangeFromFilter constructs index scan range based on filter expression
func (s *Scanner) buildIndexRangeFromFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	// Create a temporary MultiColumnOptimizer for scanning operations
	optimizer := NewMultiColumnOptimizer(s.codec)
	return optimizer.buildIndexRangeFromFilter(tenantID, tableID, indexID, filter, indexDef)
}

// extractSimpleFilter finds a simple filter for the given column in a complex filter tree
func (s *Scanner) extractSimpleFilter(filter *engineTypes.FilterExpression, columnName string) *engineTypes.FilterExpression {
	if filter.Type == "simple" && filter.Column == columnName {
		return filter
	}
	
	if filter.Type == "and" {
		// For AND, we can use any matching condition
		for _, child := range filter.Children {
			if result := s.extractSimpleFilter(child, columnName); result != nil {
				return result
			}
		}
	}
	
	// For OR/NOT, we cannot safely extract a simple filter
	return nil
}

// buildRangeFromSimpleFilter constructs index range for a simple filter
// This is a simplified version - the full implementation should be in the engine
func (s *Scanner) buildRangeFromSimpleFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression) ([]byte, []byte) {
	maxRowID := int64(^uint64(0) >> 1)
	
	switch filter.Operator {
	case "=":
		start, _ := s.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end, _ := s.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	case ">":
		start, _ := s.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		end := s.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case ">=":
		start, _ := s.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end := s.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case "<":
		start := s.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := s.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		return start, end
		
	case "<=":
		start := s.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := s.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	default:
		// Unsupported operator, full scan
		return s.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			s.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
}

// isIndexCovering checks if an index covers all projection columns
func (s *Scanner) isIndexCovering(indexDef *dbTypes.IndexDefinition, projection []string) bool {
	indexColSet := make(map[string]bool)
	for _, col := range indexDef.Columns {
		indexColSet[col] = true
	}
	
	for _, col := range projection {
		if col != "_rowid" && !indexColSet[col] {
			return false
		}
	}
	
	return true
}