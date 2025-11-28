package utils

import (
	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

// RangeBuilder provides utility functions for building index ranges
type RangeBuilder struct {
	codec codec.Codec
}

// NewRangeBuilder creates a new RangeBuilder
func NewRangeBuilder(codec codec.Codec) *RangeBuilder {
	return &RangeBuilder{
		codec: codec,
	}
}

// BuildRangeFromSimpleFilter constructs index range for a simple filter
func (rb *RangeBuilder) BuildRangeFromSimpleFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression) ([]byte, []byte) {
	maxRowID := int64(^uint64(0) >> 1)
	
	switch filter.Operator {
	case "=":
		start, _ := rb.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end, _ := rb.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	case ">":
		start, _ := rb.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		end := rb.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case ">=":
		start, _ := rb.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end := rb.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case "<":
		start := rb.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := rb.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		return start, end
		
	case "<=":
		start := rb.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := rb.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	default:
		// Unsupported operator, full scan
		return rb.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			rb.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
}

// ExtractSimpleFilter finds a simple filter for the given column in a complex filter tree
func ExtractSimpleFilter(filter *engineTypes.FilterExpression, columnName string) *engineTypes.FilterExpression {
	if filter.Type == "simple" && filter.Column == columnName {
		return filter
	}
	
	if filter.Type == "and" {
		// For AND, we can use any matching condition
		for _, child := range filter.Children {
			if result := ExtractSimpleFilter(child, columnName); result != nil {
				return result
			}
		}
	}
	
	// For OR/NOT, we cannot safely extract a simple filter
	return nil
}

// IsIndexCovering checks if an index covers all projection columns
func IsIndexCovering(indexDef *dbTypes.IndexDefinition, projection []string) bool {
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