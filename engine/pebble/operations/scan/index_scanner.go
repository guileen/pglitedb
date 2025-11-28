package scan

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// MultiColumnOptimizer handles optimization of multi-column index scans
type MultiColumnOptimizer struct{}

// NewMultiColumnOptimizer creates a new MultiColumnOptimizer
func NewMultiColumnOptimizer() *MultiColumnOptimizer {
	return &MultiColumnOptimizer{}
}

// OptimizeMultiColumnIndexRange optimizes index range for multi-column indexes with AND filters
func (mco *MultiColumnOptimizer) OptimizeMultiColumnIndexRange(
	tenantID, tableID, indexID int64,
	filter *engineTypes.FilterExpression,
	indexDef *dbTypes.IndexDefinition,
	codec interface {
		EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error)
		EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error)
		EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte
		EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte
	},
) ([]byte, []byte) {
	// For non-AND filters or single column indexes, fall back to existing logic
	if filter.Type != "and" || len(indexDef.Columns) <= 1 {
		return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	// Extract conditions for each indexed column
	columnConditions := make(map[string]*engineTypes.FilterExpression)
	mco.extractColumnConditions(filter, columnConditions)

	// Build optimized range based on index column order
	return mco.buildOptimizedRange(tenantID, tableID, indexID, columnConditions, indexDef, codec)
}

// extractColumnConditions extracts simple filter conditions for each column from an AND filter
func (mco *MultiColumnOptimizer) extractColumnConditions(filter *engineTypes.FilterExpression, columnConditions map[string]*engineTypes.FilterExpression) {
	if filter.Type == "simple" {
		// For simple filters, store the condition if we don't already have one for this column
		// (later conditions override earlier ones for the same column)
		columnConditions[filter.Column] = filter
	} else if filter.Type == "and" {
		// For AND filters, recursively extract conditions from all children
		for _, child := range filter.Children {
			mco.extractColumnConditions(child, columnConditions)
		}
	}
	// For OR/NOT filters, we don't extract conditions as they can't be optimized with range scans
}

// buildOptimizedRange builds an optimized index range based on column conditions
func (mco *MultiColumnOptimizer) buildOptimizedRange(
	tenantID, tableID, indexID int64,
	columnConditions map[string]*engineTypes.FilterExpression,
	indexDef *dbTypes.IndexDefinition,
	codec interface {
		EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error)
		EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error)
		EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte
		EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte
	},
) ([]byte, []byte) {
	// Process index columns in order to build the most restrictive range possible
	var startValues, endValues []interface{}

	// For each column in the index, try to find a condition that can restrict the range
	for _, colName := range indexDef.Columns {
		condition, exists := columnConditions[colName]
		if !exists {
			// No condition for this column, we can't further restrict the range
			// All subsequent columns also can't restrict the range
			break
		}

		// Only equality conditions can be used to restrict subsequent columns
		// For inequality conditions, we can only restrict up to this point
		if condition.Operator == "=" {
			startValues = append(startValues, condition.Value)
			endValues = append(endValues, condition.Value)
		} else {
			// For inequality operators, we can only use this column to partially restrict the range
			// and cannot use subsequent columns for range restriction
			startValues, endValues = mco.buildPartialRange(condition, startValues, endValues)
			break
		}
	}

	// If we have no values to encode, fall back to full scan
	if len(startValues) == 0 {
		return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	// Encode start and end keys
	var startKey, endKey []byte
	var err error

	if len(startValues) == 1 {
		// For single value, use simple index key encoding
		// Start key uses minimum rowID (0)
		startKey, err = codec.EncodeIndexKey(tenantID, tableID, indexID, startValues[0], 0)
		if err != nil {
			return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
				codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
		
		// End key uses maximum rowID
		endKey, err = codec.EncodeIndexKey(tenantID, tableID, indexID, endValues[0], int64(^uint64(0)>>1))
		if err != nil {
			return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
				codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
	} else {
		// For multiple values, use composite index key encoding
		// Start key uses minimum rowID (0)
		startKey, err = codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, startValues, 0)
		if err != nil {
			return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
				codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
		
		// End key uses maximum rowID
		endKey, err = codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, endValues, int64(^uint64(0)>>1))
		if err != nil {
			return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
				codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
	}

	return startKey, endKey
}

// buildPartialRange builds a partial range for inequality operators
func (mco *MultiColumnOptimizer) buildPartialRange(
	condition *engineTypes.FilterExpression,
	startValues, endValues []interface{},
) ([]interface{}, []interface{}) {
	maxRowID := int64(^uint64(0) >> 1)
	
	switch condition.Operator {
	case "=":
		// Already handled in the main function
		return append(startValues, condition.Value), append(endValues, condition.Value)
	case ">":
		// Start from the value (exclusive) onwards
		return append(startValues, condition.Value), append(endValues, maxRowID)
	case ">=":
		// Start from the value (inclusive) onwards
		return append(startValues, condition.Value), append(endValues, maxRowID)
	case "<":
		// End before the value
		return append(startValues, 0), append(endValues, condition.Value)
	case "<=":
		// End at the value (inclusive)
		return append(startValues, 0), append(endValues, condition.Value)
	default:
		// Unsupported operator, return unchanged
		return startValues, endValues
	}
}

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
	// Support multi-column index range optimization for AND filters
	if filter.Type == "and" || filter.Type == "or" || filter.Type == "not" {
		// For AND filters with multi-column indexes, try to optimize the range
		if filter.Type == "and" && len(indexDef.Columns) > 1 {
			optimizer := NewMultiColumnOptimizer()
			return optimizer.OptimizeMultiColumnIndexRange(tenantID, tableID, indexID, filter, indexDef, is.codec)
		}
		
		// For OR/NOT or single-column indexes, do full index scan
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