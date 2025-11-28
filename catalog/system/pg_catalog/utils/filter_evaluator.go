package utils

import (
	"fmt"
	"strconv"
	"strings"
	"github.com/guileen/pglitedb/types"
)

// MapTypeToSQL maps internal column types to SQL standard types
func MapTypeToSQL(colType types.ColumnType) string {
	switch colType {
	case types.ColumnTypeInteger:
		return "integer"
	case types.ColumnTypeBigInt:
		return "bigint"
	case types.ColumnTypeSmallInt:
		return "smallint"
	case types.ColumnTypeText:
		return "text"
	case types.ColumnTypeVarchar:
		return "character varying"
	case types.ColumnTypeBoolean:
		return "boolean"
	case types.ColumnTypeTimestamp:
		return "timestamp without time zone"
	case types.ColumnTypeNumeric:
		return "numeric"
	case types.ColumnTypeJSONB:
		return "jsonb"
	default:
		return "text"
	}
}

// BoolToYesNo converts a boolean value to YES/NO string
func BoolToYesNo(val bool) string {
	if val {
		return "YES"
	}
	return "NO"
}

// MatchSystemTableFilter checks if a row matches the given filter conditions
func MatchSystemTableFilter(filter map[string]interface{}, fieldName string, fieldValue interface{}) bool {
	if filter == nil {
		return true
	}
	filterValue, exists := filter[fieldName]
	if !exists {
		// No filter for this field, so it matches
		return true
	}
	// Handle complex filter conditions (operator/value pairs)
	if complexFilter, ok := filterValue.(map[string]interface{}); ok {
		operator, hasOp := complexFilter["operator"].(string)
		value, hasVal := complexFilter["value"]
		if !hasOp || !hasVal {
			// Malformed complex filter, treat as simple equality
			return ValuesEqual(filterValue, fieldValue)
		}
		switch operator {
		case "=":
			return ValuesEqual(fieldValue, value)
		case ">":
			return ValueGreaterThan(fieldValue, value)
		case "<":
			return ValueLessThan(fieldValue, value)
		case ">=":
			return ValuesEqual(fieldValue, value) || ValueGreaterThan(fieldValue, value)
		case "<=":
			return ValuesEqual(fieldValue, value) || ValueLessThan(fieldValue, value)
		case "!=":
			return !ValuesEqual(fieldValue, value)
		default:
			// Unknown operator, fall back to equality
			return ValuesEqual(fieldValue, value)
		}
	}
	// Simple equality check
	return ValuesEqual(filterValue, fieldValue)
}

// ValuesEqual compares two values for equality, handling type conversions
func ValuesEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Direct comparison for same types
	if fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b) {
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	}
	// Try to convert to common types for comparison
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return aStr == bStr
	}
	// Try numeric conversions
	aNum, aErr := ToFloat64(a)
	bNum, bErr := ToFloat64(b)
	if aErr == nil && bErr == nil {
		return aNum == bNum
	}
	// Try boolean conversions
	aBool, aBoolErr := ToBool(a)
	bBool, bBoolErr := ToBool(b)
	if aBoolErr == nil && bBoolErr == nil {
		return aBool == bBool
	}
	// Fall back to string comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// ValueGreaterThan checks if a > b, handling type conversions
func ValueGreaterThan(a, b interface{}) bool {
	aNum, aErr := ToFloat64(a)
	bNum, bErr := ToFloat64(b)
	if aErr == nil && bErr == nil {
		return aNum > bNum
	}
	// For non-numeric types, compare as strings
	return fmt.Sprintf("%v", a) > fmt.Sprintf("%v", b)
}

// ValueLessThan checks if a < b, handling type conversions
func ValueLessThan(a, b interface{}) bool {
	aNum, aErr := ToFloat64(a)
	bNum, bErr := ToFloat64(b)
	if aErr == nil && bErr == nil {
		return aNum < bNum
	}
	// For non-numeric types, compare as strings
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// ToFloat64 converts various numeric types to float64
func ToFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// ToBool converts various types to boolean
func ToBool(v interface{}) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		switch strings.ToLower(val) {
		case "true", "t", "yes", "y", "1":
			return true, nil
		case "false", "f", "no", "n", "0":
			return false, nil
		}
	case int, int8, int16, int32, int64:
		num, _ := ToFloat64(v)
		return num != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		num, _ := ToFloat64(v)
		return num != 0, nil
	case float32, float64:
		num, _ := ToFloat64(v)
		return num != 0, nil
	}
	return false, fmt.Errorf("cannot convert %T to bool", v)
}