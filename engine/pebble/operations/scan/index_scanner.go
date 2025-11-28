package scan

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// IndexScanner implements index scanning operations
type IndexScanner struct {
	kv    storage.KV
	codec codec.Codec
}

// NewIndexScanner creates a new index scanner
func NewIndexScanner(kv storage.KV, codec codec.Codec) *IndexScanner {
	return &IndexScanner{
		kv:    kv,
		codec: codec,
	}
}

// ScanIndex performs an index scan
func (is *IndexScanner) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
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
		startKey, endKey = is.buildIndexRangeFromFilter(tenantID, tableID, indexID, opts.Filter, indexDef)
	} else if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
		if opts.EndKey != nil {
			endKey = opts.EndKey
		} else {
			endKey = is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
	} else {
		startKey = is.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		endKey = is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := is.kv.NewIterator(iterOpts)
	
	// Check if this is an index-only scan (covering index)
	isCovering := false
	if opts != nil && opts.Projection != nil && len(opts.Projection) > 0 {
		isCovering = is.isIndexCovering(indexDef, opts.Projection)
	}
	
	if isCovering {
		return NewIndexOnlyIterator(
			iter,
			is.codec,
			indexDef,
			opts.Projection,
			opts,
			columnTypes,
			tenantID,
			tableID,
			indexID,
			nil, // engine will be set by caller
		), nil
	}

	return NewIndexIterator(
		iter,
		is.codec,
		schemaDef,
		opts,
		columnTypes,
		tenantID,
		tableID,
		nil, // engine will be set by caller
	), nil
}

// buildIndexRangeFromFilter constructs index scan range based on filter expression
func (is *IndexScanner) buildIndexRangeFromFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	if filter == nil {
		return is.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// For complex filters (AND/OR/NOT), do full index scan and filter in iterator
	// TODO: Support multi-column index range optimization for AND filters
	if filter.Type == "and" || filter.Type == "or" || filter.Type == "not" {
		// Do full index scan, filtering happens in indexOnlyIterator.Next()
		return is.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// Simple filter
	return is.buildRangeFromSimpleFilter(tenantID, tableID, indexID, filter)
}

// extractSimpleFilter finds a simple filter for the given column in a complex filter tree
func (is *IndexScanner) extractSimpleFilter(filter *engineTypes.FilterExpression, columnName string) *engineTypes.FilterExpression {
	if filter.Type == "simple" && filter.Column == columnName {
		return filter
	}
	
	if filter.Type == "and" {
		// For AND, we can use any matching condition
		for _, child := range filter.Children {
			if result := is.extractSimpleFilter(child, columnName); result != nil {
				return result
			}
		}
	}
	
	// For OR/NOT, we cannot safely extract a simple filter
	return nil
}

// buildRangeFromSimpleFilter constructs index range for a simple filter
func (is *IndexScanner) buildRangeFromSimpleFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression) ([]byte, []byte) {
	maxRowID := int64(^uint64(0) >> 1)
	
	switch filter.Operator {
	case "=":
		start, _ := is.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end, _ := is.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	case ">":
		start, _ := is.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		end := is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case ">=":
		start, _ := is.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end := is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case "<":
		start := is.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := is.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		return start, end
		
	case "<=":
		start := is.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := is.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	default:
		// Unsupported operator, full scan
		return is.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			is.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
}

// isIndexCovering checks if an index covers all projection columns
func (is *IndexScanner) isIndexCovering(indexDef *dbTypes.IndexDefinition, projection []string) bool {
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