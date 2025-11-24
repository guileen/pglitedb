package codec

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/guileen/pglitedb/types"
)

// TypeValidator provides utilities for validating data types
type TypeValidator struct{}

// NewTypeValidator creates a new TypeValidator
func NewTypeValidator() *TypeValidator {
	return &TypeValidator{}
}

// ValidateValue validates a value against a column type
func (tv *TypeValidator) ValidateValue(value interface{}, colType types.ColumnType) error {
	switch colType {
	case types.ColumnTypeString, types.ColumnTypeText:
		switch value.(type) {
		case string:
			return nil
		case nil:
			return nil
		default:
			return fmt.Errorf("expected string value, got %T", value)
		}

	case types.ColumnTypeNumber:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			return nil
		case string:
			// Try to parse as number
			if _, err := strconv.ParseFloat(value.(string), 64); err != nil {
				return fmt.Errorf("invalid number string: %v", err)
			}
			return nil
		case nil:
			return nil
		default:
			return fmt.Errorf("expected numeric value, got %T", value)
		}

	case types.ColumnTypeBoolean:
		switch value.(type) {
		case bool:
			return nil
		case string:
			str := value.(string)
			if str == "true" || str == "false" || str == "1" || str == "0" {
				return nil
			}
			return fmt.Errorf("invalid boolean string: %s", str)
		case int, int64:
			// 0 and 1 are valid boolean representations
			return nil
		case nil:
			return nil
		default:
			return fmt.Errorf("expected boolean value, got %T", value)
		}

	case types.ColumnTypeDate, types.ColumnTypeTimestamp:
		switch value.(type) {
		case time.Time:
			return nil
		case int64:
			return nil
		case string:
			// Try to parse as time
			if _, err := time.Parse(time.RFC3339, value.(string)); err == nil {
				return nil
			}
			if _, err := time.Parse("2006-01-02", value.(string)); err == nil {
				return nil
			}
			return fmt.Errorf("invalid timestamp string: %s", value.(string))
		case nil:
			return nil
		default:
			return fmt.Errorf("expected timestamp value, got %T", value)
		}

	case types.ColumnTypeJSON:
		// Try to marshal to JSON to validate
		if value == nil {
			return nil
		}
		_, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("invalid JSON value: %v", err)
		}
		return nil

	case types.ColumnTypeUUID:
		switch v := value.(type) {
		case uuid.UUID:
			return nil
		case string:
			if v == "" {
				return nil
			}
			_, err := uuid.Parse(v)
			if err != nil {
				return fmt.Errorf("invalid UUID string: %v", err)
			}
			return nil
		case []byte:
			if len(v) == 0 {
				return nil
			}
			if len(v) != 16 {
				return fmt.Errorf("UUID byte slice must be 16 bytes, got %d", len(v))
			}
			return nil
		case nil:
			return nil
		default:
			return fmt.Errorf("expected UUID value, got %T", value)
		}

	case types.ColumnTypeBinary:
		switch value.(type) {
		case []byte:
			return nil
		case string:
			// String can be converted to bytes
			return nil
		case nil:
			return nil
		default:
			return fmt.Errorf("expected binary value, got %T", value)
		}

	default:
		return fmt.Errorf("unsupported column type: %s", colType)
	}
}

// ConvertValue converts a value from one type to another
func (tv *TypeValidator) ConvertValue(value interface{}, fromType, toType types.ColumnType) (interface{}, error) {
	// If types are the same, no conversion needed
	if fromType == toType {
		return value, nil
	}

	// Handle nil values
	if value == nil {
		return nil, nil
	}

	// Handle specific conversions
	switch fromType {
	case types.ColumnTypeString, types.ColumnTypeText:
		strValue := value.(string)

		switch toType {
		case types.ColumnTypeNumber:
			// Try to convert string to number
			if f, err := strconv.ParseFloat(strValue, 64); err == nil {
				return f, nil
			}
			return nil, fmt.Errorf("cannot convert string '%s' to number", strValue)

		case types.ColumnTypeBoolean:
			// Convert string to boolean
			switch strValue {
			case "true", "1", "TRUE", "True":
				return true, nil
			case "false", "0", "FALSE", "False":
				return false, nil
			default:
				return nil, fmt.Errorf("cannot convert string '%s' to boolean", strValue)
			}

		case types.ColumnTypeDate, types.ColumnTypeTimestamp:
			// Try to parse as time
			if t, err := time.Parse(time.RFC3339, strValue); err == nil {
				return t, nil
			}
			if t, err := time.Parse("2006-01-02", strValue); err == nil {
				return t, nil
			}
			return nil, fmt.Errorf("cannot convert string '%s' to timestamp", strValue)

		case types.ColumnTypeUUID:
			if strValue == "" {
				return nil, nil
			}
			if u, err := uuid.Parse(strValue); err == nil {
				return u, nil
			}
			return nil, fmt.Errorf("cannot convert string '%s' to UUID", strValue)

		case types.ColumnTypeBinary:
			return []byte(strValue), nil
		}

	case types.ColumnTypeNumber:
		switch toType {
		case types.ColumnTypeString, types.ColumnTypeText:
			return fmt.Sprintf("%v", value), nil

		case types.ColumnTypeBoolean:
			// Convert number to boolean (0 = false, non-zero = true)
			switch v := value.(type) {
			case int:
				return v != 0, nil
			case int64:
				return v != 0, nil
			case float64:
				return v != 0.0, nil
			default:
				return nil, fmt.Errorf("cannot convert number to boolean")
			}

		case types.ColumnTypeJSON:
			// Numbers are valid JSON values
			return value, nil
		}

	case types.ColumnTypeBoolean:
		boolValue := value.(bool)

		switch toType {
		case types.ColumnTypeString, types.ColumnTypeText:
			if boolValue {
				return "true", nil
			}
			return "false", nil

		case types.ColumnTypeNumber:
			if boolValue {
				return 1, nil
			}
			return 0, nil

		case types.ColumnTypeJSON:
			// Booleans are valid JSON values
			return boolValue, nil
		}

	case types.ColumnTypeDate, types.ColumnTypeTimestamp:
		timeValue, ok := value.(time.Time)
		if !ok {
			// Try to convert from int64
			if ts, ok := value.(int64); ok {
				timeValue = time.Unix(ts, 0)
			} else {
				return nil, fmt.Errorf("expected time.Time or int64, got %T", value)
			}
		}

		switch toType {
		case types.ColumnTypeString, types.ColumnTypeText:
			return timeValue.Format(time.RFC3339), nil

		case types.ColumnTypeNumber:
			return timeValue.Unix(), nil

		case types.ColumnTypeJSON:
			// Times are valid JSON values when serialized
			return timeValue, nil
		}

	case types.ColumnTypeJSON:
		switch toType {
		case types.ColumnTypeString, types.ColumnTypeText:
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("cannot marshal JSON to string: %v", err)
			}
			return string(jsonBytes), nil
		}

	case types.ColumnTypeUUID:
		uuidValue, ok := value.(uuid.UUID)
		if !ok {
			// Try to convert from string
			if str, ok := value.(string); ok && str != "" {
				var err error
				uuidValue, err = uuid.Parse(str)
				if err != nil {
					return nil, fmt.Errorf("cannot parse UUID string: %v", err)
				}
			} else {
				return nil, fmt.Errorf("expected uuid.UUID, got %T", value)
			}
		}

		switch toType {
		case types.ColumnTypeString, types.ColumnTypeText:
			return uuidValue.String(), nil

		case types.ColumnTypeBinary:
			return uuidValue[:], nil
		}

	case types.ColumnTypeBinary:
		byteValue, ok := value.([]byte)
		if !ok {
			// Try to convert from string
			if str, ok := value.(string); ok {
				byteValue = []byte(str)
			} else {
				return nil, fmt.Errorf("expected []byte, got %T", value)
			}
		}

		switch toType {
		case types.ColumnTypeString, types.ColumnTypeText:
			return string(byteValue), nil

		case types.ColumnTypeUUID:
			if len(byteValue) != 16 {
				return nil, fmt.Errorf("cannot convert byte array to UUID: expected 16 bytes, got %d", len(byteValue))
			}
			var u uuid.UUID
			copy(u[:], byteValue)
			return u, nil
		}
	}

	return nil, fmt.Errorf("cannot convert from %s to %s", fromType, toType)
}

// IsCompatibleType checks if a value can be stored in a column of a given type
func (tv *TypeValidator) IsCompatibleType(value interface{}, colType types.ColumnType) bool {
	err := tv.ValidateValue(value, colType)
	return err == nil
}
