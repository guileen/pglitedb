package pebble

import (
	"github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

// FilterEvaluator handles filter evaluation operations
type FilterEvaluator struct{}

// NewFilterEvaluator creates a new FilterEvaluator
func NewFilterEvaluator() *FilterEvaluator {
	return &FilterEvaluator{}
}

// EvaluateFilter evaluates a filter expression against a record
func (fe *FilterEvaluator) EvaluateFilter(filter *types.FilterExpression, record *dbTypes.Record) bool {
	if filter == nil {
		return true
	}

	switch filter.Type {
	case "simple":
		return fe.evaluateSimpleFilter(filter, record)
	case "and":
		for _, child := range filter.Children {
			if !fe.EvaluateFilter(child, record) {
				return false
			}
		}
		return true
	case "or":
		for _, child := range filter.Children {
			if fe.EvaluateFilter(child, record) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// evaluateSimpleFilter evaluates a simple filter condition
func (fe *FilterEvaluator) evaluateSimpleFilter(filter *types.FilterExpression, record *dbTypes.Record) bool {
	val, exists := record.Data[filter.Column]
	if !exists {
		return false
	}

	if val.Data == nil {
		return filter.Value == nil
	}

	switch filter.Operator {
	case "=":
		return fe.compareValues(val.Data, filter.Value) == 0
	case ">":
		return fe.compareValues(val.Data, filter.Value) > 0
	case ">=":
		return fe.compareValues(val.Data, filter.Value) >= 0
	case "<":
		return fe.compareValues(val.Data, filter.Value) < 0
	case "<=":
		return fe.compareValues(val.Data, filter.Value) <= 0
	case "IN":
		for _, v := range filter.Values {
			if fe.compareValues(val.Data, v) == 0 {
				return true
			}
		}
		return false
	case "BETWEEN":
		if len(filter.Values) >= 2 {
			return fe.compareValues(val.Data, filter.Values[0]) >= 0 &&
				fe.compareValues(val.Data, filter.Values[1]) <= 0
		}
		return false
	default:
		return true
	}
}

// compareValues compares two values, returns -1/0/1 like strcmp
func (fe *FilterEvaluator) compareValues(a, b interface{}) int {
	// Handle nil cases
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Type-specific comparison
	switch av := a.(type) {
	case int64:
		bv := toInt64(b)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case int:
		return fe.compareValues(int64(av), b)

	case float64:
		bv := toFloat64(b)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case string:
		bv, ok := b.(string)
		if !ok {
			return 1
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 1
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1

	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}