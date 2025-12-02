package utils

import (
	"testing"
	"github.com/guileen/pglitedb/types"
)

func TestMapTypeToSQL(t *testing.T) {
	testCases := []struct {
		input    types.ColumnType
		expected string
	}{
		{types.ColumnTypeInteger, "integer"},
		{types.ColumnTypeBigInt, "bigint"},
		{types.ColumnTypeSmallInt, "smallint"},
		{types.ColumnTypeText, "text"},
		{types.ColumnTypeVarchar, "character varying"},
		{types.ColumnTypeBoolean, "boolean"},
		{types.ColumnTypeTimestamp, "timestamp without time zone"},
		{types.ColumnTypeNumeric, "numeric"},
		{types.ColumnTypeJSONB, "jsonb"},
		{types.ColumnType("unknown"), "text"}, // Default case
	}
	
	for _, tc := range testCases {
		result := MapTypeToSQL(tc.input)
		if result != tc.expected {
			t.Errorf("MapTypeToSQL(%v) = %s; expected %s", tc.input, result, tc.expected)
		}
	}
}

func TestBoolToYesNo(t *testing.T) {
	if result := BoolToYesNo(true); result != "YES" {
		t.Errorf("BoolToYesNo(true) = %s; expected YES", result)
	}
	
	if result := BoolToYesNo(false); result != "NO" {
		t.Errorf("BoolToYesNo(false) = %s; expected NO", result)
	}
}

func TestMatchSystemTableFilter_NoFilter(t *testing.T) {
	filter := map[string]interface{}(nil)
	fieldName := "test_field"
	fieldValue := "test_value"
	
	// Should match when no filter is provided
	if !MatchSystemTableFilter(filter, fieldName, fieldValue) {
		t.Error("Expected to match when no filter is provided")
	}
	
	// Should match when filter is empty
	emptyFilter := map[string]interface{}{}
	if !MatchSystemTableFilter(emptyFilter, fieldName, fieldValue) {
		t.Error("Expected to match when filter is empty")
	}
}

func TestMatchSystemTableFilter_SimpleEquality(t *testing.T) {
	filter := map[string]interface{}{
		"name": "users",
	}
	
	// Should match when field value equals filter value
	if !MatchSystemTableFilter(filter, "name", "users") {
		t.Error("Expected to match when field value equals filter value")
	}
	
	// Should not match when field value differs from filter value
	if MatchSystemTableFilter(filter, "name", "orders") {
		t.Error("Expected not to match when field value differs from filter value")
	}
	
	// Should match when no filter exists for the field
	if !MatchSystemTableFilter(filter, "other_field", "any_value") {
		t.Error("Expected to match when no filter exists for the field")
	}
}

func TestMatchSystemTableFilter_ComplexFilter(t *testing.T) {
	// Test equality operator
	filter := map[string]interface{}{
		"count": map[string]interface{}{
			"operator": "=",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 5) {
		t.Error("Expected to match with equality operator")
	}
	
	if MatchSystemTableFilter(filter, "count", 6) {
		t.Error("Expected not to match with equality operator when values differ")
	}
	
	// Test greater than operator
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"operator": ">",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 6) {
		t.Error("Expected to match with greater than operator")
	}
	
	if MatchSystemTableFilter(filter, "count", 4) {
		t.Error("Expected not to match with greater than operator when value is less")
	}
	
	// Test less than operator
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"operator": "<",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 4) {
		t.Error("Expected to match with less than operator")
	}
	
	if MatchSystemTableFilter(filter, "count", 6) {
		t.Error("Expected not to match with less than operator when value is greater")
	}
	
	// Test greater than or equal operator
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"operator": ">=",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 5) {
		t.Error("Expected to match with greater than or equal operator (equal case)")
	}
	
	if !MatchSystemTableFilter(filter, "count", 6) {
		t.Error("Expected to match with greater than or equal operator (greater case)")
	}
	
	if MatchSystemTableFilter(filter, "count", 4) {
		t.Error("Expected not to match with greater than or equal operator when value is less")
	}
	
	// Test less than or equal operator
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"operator": "<=",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 5) {
		t.Error("Expected to match with less than or equal operator (equal case)")
	}
	
	if !MatchSystemTableFilter(filter, "count", 4) {
		t.Error("Expected to match with less than or equal operator (less case)")
	}
	
	if MatchSystemTableFilter(filter, "count", 6) {
		t.Error("Expected not to match with less than or equal operator when value is greater")
	}
	
	// Test not equal operator
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"operator": "!=",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 6) {
		t.Error("Expected to match with not equal operator")
	}
	
	if MatchSystemTableFilter(filter, "count", 5) {
		t.Error("Expected not to match with not equal operator when values are equal")
	}
	
	// Test unknown operator (should fall back to equality)
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"operator": "??",
			"value":    5,
		},
	}
	
	if !MatchSystemTableFilter(filter, "count", 5) {
		t.Error("Expected to match with unknown operator (fallback to equality)")
	}
	
	// Test malformed complex filter (should fall back to simple equality)
	filter = map[string]interface{}{
		"count": map[string]interface{}{
			"value": 5, // Missing operator
		},
	}
	
	// This should not match because the complex filter is malformed
	if MatchSystemTableFilter(filter, "count", 5) {
		t.Error("Expected not to match with malformed complex filter")
	}
}

func TestValuesEqual(t *testing.T) {
	// Test nil cases
	if !ValuesEqual(nil, nil) {
		t.Error("Expected nil to equal nil")
	}
	
	if ValuesEqual(nil, "test") {
		t.Error("Expected nil to not equal non-nil value")
	}
	
	if ValuesEqual("test", nil) {
		t.Error("Expected non-nil value to not equal nil")
	}
	
	// Test same types
	if !ValuesEqual("test", "test") {
		t.Error("Expected same string values to be equal")
	}
	
	if !ValuesEqual(5, 5) {
		t.Error("Expected same integer values to be equal")
	}
	
	if ValuesEqual("test", "other") {
		t.Error("Expected different string values to not be equal")
	}
	
	// Test different types with string conversion
	if !ValuesEqual("5", 5) {
		t.Error("Expected string '5' to equal integer 5")
	}
	
	if !ValuesEqual(5, "5") {
		t.Error("Expected integer 5 to equal string '5'")
	}
	
	// Test different types with numeric conversion
	if !ValuesEqual(5.0, 5) {
		t.Error("Expected float 5.0 to equal integer 5")
	}
	
	// Test boolean conversions
	if !ValuesEqual(true, "true") {
		t.Error("Expected boolean true to equal string 'true'")
	}
	
	if !ValuesEqual("false", false) {
		t.Error("Expected string 'false' to equal boolean false")
	}
	
	if !ValuesEqual(1, true) {
		t.Error("Expected integer 1 to equal boolean true")
	}
	
	if !ValuesEqual(0, false) {
		t.Error("Expected integer 0 to equal boolean false")
	}
	
	// Test fallback to string comparison
	type customType string
	customVal := customType("test")
	if !ValuesEqual(customVal, "test") {
		t.Error("Expected custom type to equal string through fallback")
	}
}

func TestValueGreaterThan(t *testing.T) {
	// Test numeric comparisons
	if !ValueGreaterThan(5, 3) {
		t.Error("Expected 5 to be greater than 3")
	}
	
	if ValueGreaterThan(3, 5) {
		t.Error("Expected 3 not to be greater than 5")
	}
	
	if ValueGreaterThan(5, 5) {
		t.Error("Expected 5 not to be greater than 5")
	}
	
	// Test string comparisons
	if !ValueGreaterThan("b", "a") {
		t.Error("Expected 'b' to be greater than 'a'")
	}
	
	if ValueGreaterThan("a", "b") {
		t.Error("Expected 'a' not to be greater than 'b'")
	}
	
	// Test mixed type comparisons (converted to strings)
	if !ValueGreaterThan("10", 5) {
		t.Error("Expected '10' to be greater than 5 when compared as strings")
	}
}

func TestValueLessThan(t *testing.T) {
	// Test numeric comparisons
	if !ValueLessThan(3, 5) {
		t.Error("Expected 3 to be less than 5")
	}
	
	if ValueLessThan(5, 3) {
		t.Error("Expected 5 not to be less than 3")
	}
	
	if ValueLessThan(5, 5) {
		t.Error("Expected 5 not to be less than 5")
	}
	
	// Test string comparisons
	if !ValueLessThan("a", "b") {
		t.Error("Expected 'a' to be less than 'b'")
	}
	
	if ValueLessThan("b", "a") {
		t.Error("Expected 'b' not to be less than 'a'")
	}
	
	// Test mixed type comparisons (converted to strings)
	if !ValueLessThan(5, "10") {
		t.Error("Expected 5 to be less than '10' when compared as strings")
	}
}

func TestToFloat64(t *testing.T) {
	// Test float64
	if val, err := ToFloat64(float64(5.5)); err != nil || val != 5.5 {
		t.Errorf("Expected 5.5, got %v, error: %v", val, err)
	}
	
	// Test float32
	if val, err := ToFloat64(float32(3.14)); err != nil || val < 3.13 || val > 3.15 {
		t.Errorf("Expected approximately 3.14, got %v, error: %v", val, err)
	}
	
	// Test integers
	if val, err := ToFloat64(int(5)); err != nil || val != 5.0 {
		t.Errorf("Expected 5.0, got %v, error: %v", val, err)
	}
	
	if val, err := ToFloat64(int64(100)); err != nil || val != 100.0 {
		t.Errorf("Expected 100.0, got %v, error: %v", val, err)
	}
	
	// Test unsigned integers
	if val, err := ToFloat64(uint(10)); err != nil || val != 10.0 {
		t.Errorf("Expected 10.0, got %v, error: %v", val, err)
	}
	
	// Test string numbers
	if val, err := ToFloat64("123.45"); err != nil || val != 123.45 {
		t.Errorf("Expected 123.45, got %v, error: %v", val, err)
	}
	
	// Test invalid string
	if _, err := ToFloat64("not_a_number"); err == nil {
		t.Error("Expected error for invalid string")
	}
	
	// Test unsupported type
	if _, err := ToFloat64([]int{1, 2, 3}); err == nil {
		t.Error("Expected error for unsupported type")
	}
}

func TestToBool(t *testing.T) {
	// Test boolean
	if val, err := ToBool(true); err != nil || !val {
		t.Errorf("Expected true, got %v, error: %v", val, err)
	}
	
	if val, err := ToBool(false); err != nil || val {
		t.Errorf("Expected false, got %v, error: %v", val, err)
	}
	
	// Test string representations of true
	trueStrings := []string{"true", "TRUE", "True", "t", "T", "yes", "YES", "Yes", "y", "Y", "1"}
	for _, str := range trueStrings {
		if val, err := ToBool(str); err != nil || !val {
			t.Errorf("Expected true for string '%s', got %v, error: %v", str, val, err)
		}
	}
	
	// Test string representations of false
	falseStrings := []string{"false", "FALSE", "False", "f", "F", "no", "NO", "No", "n", "N", "0"}
	for _, str := range falseStrings {
		if val, err := ToBool(str); err != nil || val {
			t.Errorf("Expected false for string '%s', got %v, error: %v", str, val, err)
		}
	}
	
	// Test numeric representations
	if val, err := ToBool(1); err != nil || !val {
		t.Errorf("Expected true for integer 1, got %v, error: %v", val, err)
	}
	
	if val, err := ToBool(0); err != nil || val {
		t.Errorf("Expected false for integer 0, got %v, error: %v", val, err)
	}
	
	if val, err := ToBool(5.0); err != nil || !val {
		t.Errorf("Expected true for float 5.0, got %v, error: %v", val, err)
	}
	
	if val, err := ToBool(0.0); err != nil || val {
		t.Errorf("Expected false for float 0.0, got %v, error: %v", val, err)
	}
	
	// Test invalid string
	if _, err := ToBool("maybe"); err == nil {
		t.Error("Expected error for invalid boolean string")
	}
	
	// Test unsupported type
	if _, err := ToBool([]int{1, 2, 3}); err == nil {
		t.Error("Expected error for unsupported type")
	}
}