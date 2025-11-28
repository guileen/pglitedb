package utils

import (
	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

// BuildIndexRangeFromFilter constructs index scan range based on filter expression
func BuildIndexRangeFromFilter(codec codec.Codec, tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	// For now, this is a simplified version that delegates to BuildRangeFromSimpleFilter
	// A more sophisticated implementation could handle multi-column indexes
	
	if filter == nil {
		return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	// For complex filters (AND/OR/NOT), do full index scan and filter in iterator
	if filter.Type == "and" || filter.Type == "or" || filter.Type == "not" {
		// Do full index scan, filtering happens in indexOnlyIterator.Next()
		return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	// Simple filter
	return BuildRangeFromSimpleFilter(codec, tenantID, tableID, indexID, filter)
}

// BuildRangeFromSimpleFilter constructs index range for a simple filter
func BuildRangeFromSimpleFilter(codec codec.Codec, tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression) ([]byte, []byte) {
	maxRowID := int64(^uint64(0) >> 1)

	switch filter.Operator {
	case "=":
		start, _ := codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end, _ := codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end

	case ">":
		start, _ := codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		end := codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end

	case ">=":
		start, _ := codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end := codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end

	case "<":
		start := codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		return start, end

	case "<=":
		start := codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end

	default:
		// Unsupported operator, full scan
		return codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
}