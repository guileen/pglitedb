package codec

import (
	"github.com/guileen/pglitedb/table"
)

// Serializer defines the interface for serializing and deserializing data types
type Serializer interface {
	// Serialize converts a value to its byte representation
	Serialize(value interface{}, colType table.ColumnType) ([]byte, error)

	// Deserialize converts byte representation back to a value
	Deserialize(data []byte, colType table.ColumnType) (interface{}, error)

	// Validate checks if a value is valid for a given column type
	Validate(value interface{}, colType table.ColumnType) error

	// Convert converts a value from one type to another
	Convert(value interface{}, fromType, toType table.ColumnType) (interface{}, error)
}

// TypeConverter provides type conversion utilities
type TypeConverter struct{}

// NewTypeConverter creates a new TypeConverter
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// Serialize converts a value to its byte representation using the codec
func (tc *TypeConverter) Serialize(value interface{}, colType table.ColumnType) ([]byte, error) {
	codec := NewMemComparableCodec()
	return codec.EncodeValue(value, colType)
}

// Deserialize converts byte representation back to a value using the codec
func (tc *TypeConverter) Deserialize(data []byte, colType table.ColumnType) (interface{}, error) {
	codec := NewMemComparableCodec()
	return codec.DecodeValue(data, colType)
}

// Validate checks if a value is valid for a given column type
func (tc *TypeConverter) Validate(value interface{}, colType table.ColumnType) error {
	switch colType {
	case table.ColumnTypeString, table.ColumnTypeText:
		_, ok := value.(string)
		if !ok {
			return &table.TypeError{Message: "expected string value"}
		}
	case table.ColumnTypeNumber:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			// Valid number types
		default:
			return &table.TypeError{Message: "expected numeric value"}
		}
	case table.ColumnTypeBoolean:
		_, ok := value.(bool)
		if !ok {
			return &table.TypeError{Message: "expected boolean value"}
		}
	case table.ColumnTypeDate, table.ColumnTypeTimestamp:
		switch value.(type) {
		case int64, string:
			// Valid timestamp types
		default:
			return &table.TypeError{Message: "expected timestamp value"}
		}
	case table.ColumnTypeJSON:
		// JSON can be any valid JSON-serializable type
		_, err := tc.Serialize(value, colType)
		if err != nil {
			return &table.TypeError{Message: "invalid JSON value: " + err.Error()}
		}
	case table.ColumnTypeUUID:
		switch v := value.(type) {
		case string:
			// Try to parse as UUID
			_, err := tc.Serialize(v, colType)
			if err != nil {
				return &table.TypeError{Message: "invalid UUID string: " + err.Error()}
			}
		case []byte:
			if len(v) != 16 {
				return &table.TypeError{Message: "UUID byte slice must be 16 bytes"}
			}
		default:
			return &table.TypeError{Message: "expected UUID string or 16-byte array"}
		}
	case table.ColumnTypeBinary:
		_, ok := value.([]byte)
		if !ok {
			return &table.TypeError{Message: "expected byte array"}
		}
	default:
		return &table.TypeError{Message: "unsupported column type: " + string(colType)}
	}

	return nil
}

// Convert converts a value from one type to another
func (tc *TypeConverter) Convert(value interface{}, fromType, toType table.ColumnType) (interface{}, error) {
	// If types are the same, no conversion needed
	if fromType == toType {
		return value, nil
	}

	// Handle specific conversions
	switch fromType {
	case table.ColumnTypeString, table.ColumnTypeText:
		strValue, ok := value.(string)
		if !ok {
			return nil, &table.TypeError{Message: "expected string value"}
		}

		switch toType {
		case table.ColumnTypeNumber:
			// Try to convert string to number
			// This is a simplified implementation - in practice, you'd want more robust parsing
			return strValue, nil // Return as-is, let database handle conversion
		case table.ColumnTypeBoolean:
			// Convert string to boolean
			if strValue == "true" || strValue == "1" {
				return true, nil
			} else if strValue == "false" || strValue == "0" {
				return false, nil
			}
			return nil, &table.TypeError{Message: "cannot convert string to boolean"}
		case table.ColumnTypeJSON:
			// Try to parse as JSON
			return strValue, nil // Return as-is, let database handle conversion
		case table.ColumnTypeUUID:
			// Try to parse as UUID
			return strValue, nil // Return as-is, let database handle conversion
		}

	case table.ColumnTypeNumber:
		switch toType {
		case table.ColumnTypeString, table.ColumnTypeText:
			return value, nil // Convert to string representation
		case table.ColumnTypeBoolean:
			// Convert number to boolean (0 = false, non-zero = true)
			switch v := value.(type) {
			case int:
				return v != 0, nil
			case int64:
				return v != 0, nil
			case float64:
				return v != 0.0, nil
			default:
				return nil, &table.TypeError{Message: "cannot convert number to boolean"}
			}
		}

	case table.ColumnTypeBoolean:
		boolValue, ok := value.(bool)
		if !ok {
			return nil, &table.TypeError{Message: "expected boolean value"}
		}

		switch toType {
		case table.ColumnTypeString, table.ColumnTypeText:
			if boolValue {
				return "true", nil
			}
			return "false", nil
		case table.ColumnTypeNumber:
			if boolValue {
				return 1, nil
			}
			return 0, nil
		}
	}

	// If we can't convert directly, try serializing and deserializing
	serialized, err := tc.Serialize(value, fromType)
	if err != nil {
		return nil, &table.TypeError{Message: "cannot serialize source value: " + err.Error()}
	}

	result, err := tc.Deserialize(serialized, toType)
	if err != nil {
		return nil, &table.TypeError{Message: "cannot deserialize to target type: " + err.Error()}
	}

	return result, nil
}
