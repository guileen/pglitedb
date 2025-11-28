package utils

// IsNumeric checks if a value is numeric
func IsNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

// CompareNumerics compares two numeric values
func CompareNumerics(a, b interface{}) int {
	af := ToFloat64(a)
	bf := ToFloat64(b)
	
	if af < bf {
		return -1
	} else if af > bf {
		return 1
	}
	return 0
}

// ToFloat64 converts a value to float64
func ToFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case int16:
		return float64(val)
	case int8:
		return float64(val)
	case int:
		return float64(val)
	case uint64:
		return float64(val)
	case uint32:
		return float64(val)
	case uint16:
		return float64(val)
	case uint8:
		return float64(val)
	case uint:
		return float64(val)
	default:
		return 0
	}
}