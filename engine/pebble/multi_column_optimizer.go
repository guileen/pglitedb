package pebble

import (
	"bytes"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

// MultiColumnOptimizer handles optimization for multi-column indexes
type MultiColumnOptimizer struct {
	codec codec.Codec
}

// NewMultiColumnOptimizer creates a new multi-column optimizer
func NewMultiColumnOptimizer(codec codec.Codec) *MultiColumnOptimizer {
	return &MultiColumnOptimizer{
		codec: codec,
	}
}

// buildMultiColumnIndexRange constructs index scan range for multi-column indexes with AND filters
func (mco *MultiColumnOptimizer) buildMultiColumnIndexRange(tenantID, tableID, indexID int64, filter *types.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	if filter == nil || filter.Type != "and" || len(indexDef.Columns) <= 1 {
		// Don't recurse, handle directly
		return mco.buildDirectRange(tenantID, tableID, indexID, filter, indexDef)
	}
	
	// Extract conditions for each column in the index
	columnConditions := make(map[string]*types.FilterExpression)
	for _, child := range filter.Children {
		if child.Type == "simple" {
			columnConditions[child.Column] = child
		}
	}
	
	// Build range values for each column in index order
	rangeValues := make([]interface{}, 0, len(indexDef.Columns))
	allEqual := true
	
	for _, colName := range indexDef.Columns {
		if cond, exists := columnConditions[colName]; exists {
			rangeValues = append(rangeValues, cond.Value)
			if cond.Operator != "=" {
				allEqual = false
			}
		} else {
			// If we don't have a condition for this column, we can't optimize further
			break
		}
	}
	
	// If we have conditions for all columns and they are all equality conditions,
	// we can create a precise range
	if len(rangeValues) == len(indexDef.Columns) && allEqual {
		start, _ := mco.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, rangeValues, 0)
		end, _ := mco.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, rangeValues, int64(^uint64(0)>>1))
		return start, end
	}
	
	// If we have conditions for some columns, create a range for those
	if len(rangeValues) > 0 {
		start, _ := mco.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, rangeValues, 0)
		end, _ := mco.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, rangeValues, int64(^uint64(0)>>1))
		return start, end
	}
	
	// Fall back to full index scan
	return mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
		mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
}

// buildDirectRange handles range construction without recursion
func (mco *MultiColumnOptimizer) buildDirectRange(tenantID, tableID, indexID int64, filter *types.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	if filter == nil {
		return mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// For complex filters (AND/OR/NOT), do full index scan and filter in iterator
	if filter.Type == "and" || filter.Type == "or" || filter.Type == "not" {
		// Do full index scan, filtering happens in indexOnlyIterator.Next()
		return mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// Simple filter
	return mco.buildRangeFromSimpleFilter(tenantID, tableID, indexID, filter)
}

// buildIndexRangeFromFilter constructs index scan range based on filter expression
func (mco *MultiColumnOptimizer) buildIndexRangeFromFilter(tenantID, tableID, indexID int64, filter *types.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	if filter == nil {
		return mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// For complex filters (AND/OR/NOT), do full index scan and filter in iterator
	if filter.Type == "and" || filter.Type == "or" || filter.Type == "not" {
		// For AND filters with multi-column indexes, try multi-column optimization
		if filter.Type == "and" && len(indexDef.Columns) > 1 {
			// Use the correct method that handles inequalities properly
			start, end := mco.OptimizeMultiColumnIndexRange(tenantID, tableID, indexID, filter, indexDef, mco.codec)
			// If optimization was successful (not full scan), use it
			if !bytes.Equal(start, mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)) ||
				!bytes.Equal(end, mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)) {
				return start, end
			}
		}
		// Do full index scan, filtering happens in indexOnlyIterator.Next()
		return mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// Simple filter
	return mco.buildRangeFromSimpleFilter(tenantID, tableID, indexID, filter)
}

// buildRangeFromSimpleFilter constructs index range for a simple filter
func (mco *MultiColumnOptimizer) buildRangeFromSimpleFilter(tenantID, tableID, indexID int64, filter *types.FilterExpression) ([]byte, []byte) {
	maxRowID := int64(^uint64(0) >> 1)
	
	switch filter.Operator {
	case "=":
		start, _ := mco.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end, _ := mco.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	case ">":
		start, _ := mco.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		end := mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case ">=":
		start, _ := mco.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end := mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case "<":
		start := mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := mco.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		return start, end
		
	case "<=":
		start := mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := mco.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	default:
		// Unsupported operator, full scan
		return mco.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			mco.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
}

// OptimizeMultiColumnIndexRange optimizes index range for multi-column indexes with AND filters
// This method is compatible with the operations/scan package
func (mco *MultiColumnOptimizer) OptimizeMultiColumnIndexRange(
	tenantID, tableID, indexID int64,
	filter *types.FilterExpression,
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
	columnConditions := make(map[string]*types.FilterExpression)
	mco.extractColumnConditions(filter, columnConditions)

	// Build optimized range based on index column order
	return mco.buildOptimizedRange(tenantID, tableID, indexID, columnConditions, indexDef, codec)
}

// extractColumnConditions extracts simple filter conditions for each column from an AND filter
func (mco *MultiColumnOptimizer) extractColumnConditions(filter *types.FilterExpression, columnConditions map[string]*types.FilterExpression) {
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
	columnConditions map[string]*types.FilterExpression,
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
	condition *types.FilterExpression,
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