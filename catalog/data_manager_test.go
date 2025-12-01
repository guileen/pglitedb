package catalog

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataManager_ConvertValue_String(t *testing.T) {
	m := &dataManager{}
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "valid string",
			input:   "hello",
			colType: types.ColumnTypeString,
			expected: &types.Value{
				Data: "hello",
				Type: types.ColumnTypeString,
			},
		},
		{
			name:    "valid text",
			input:   "long text content",
			colType: types.ColumnTypeText,
			expected: &types.Value{
				Data: "long text content",
				Type: types.ColumnTypeText,
			},
		},
		{
			name:     "invalid type",
			input:    123,
			colType:  types.ColumnTypeString,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDataManager_ConvertValue_UUID(t *testing.T) {
	m := &dataManager{}
	validUUID := "550e8400-e29b-41d4-a716-446655440000"
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "valid UUID string",
			input:   validUUID,
			colType: types.ColumnTypeUUID,
			expected: &types.Value{
				Data: validUUID,
				Type: types.ColumnTypeUUID,
			},
		},
		{
			name:    "empty UUID string",
			input:   "",
			colType: types.ColumnTypeUUID,
			expected: &types.Value{
				Data: nil,
				Type: types.ColumnTypeUUID,
			},
		},
		{
			name:     "invalid UUID string",
			input:    "invalid-uuid",
			colType:  types.ColumnTypeUUID,
			hasError: true,
		},
		{
			name:    "valid UUID object",
			input:   uuid.MustParse(validUUID),
			colType: types.ColumnTypeUUID,
			expected: &types.Value{
				Data: validUUID,
				Type: types.ColumnTypeUUID,
			},
		},
		{
			name:     "invalid type",
			input:    123,
			colType:  types.ColumnTypeUUID,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDataManager_ConvertValue_Number(t *testing.T) {
	m := &dataManager{}
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "int to number",
			input:   42,
			colType: types.ColumnTypeNumber,
			expected: &types.Value{
				Data: int64(42),
				Type: types.ColumnTypeNumber,
			},
		},
		{
			name:    "int32 to number",
			input:   int32(42),
			colType: types.ColumnTypeNumber,
			expected: &types.Value{
				Data: int64(42),
				Type: types.ColumnTypeNumber,
			},
		},
		{
			name:    "int64 to number",
			input:   int64(42),
			colType: types.ColumnTypeNumber,
			expected: &types.Value{
				Data: int64(42),
				Type: types.ColumnTypeNumber,
			},
		},
		{
			name:    "float32 to number",
			input:   float32(3.14),
			colType: types.ColumnTypeNumber,
			expected: &types.Value{
				Data: float64(float32(3.14)),
				Type: types.ColumnTypeNumber,
			},
		},
		{
			name:    "float64 to number",
			input:   3.14159,
			colType: types.ColumnTypeNumber,
			expected: &types.Value{
				Data: 3.14159,
				Type: types.ColumnTypeNumber,
			},
		},
		{
			name:    "string number to number",
			input:   "123.45",
			colType: types.ColumnTypeNumber,
			expected: &types.Value{
				Data: 123.45,
				Type: types.ColumnTypeNumber,
			},
		},
		{
			name:     "invalid string to number",
			input:    "not-a-number",
			colType:  types.ColumnTypeNumber,
			hasError: true,
		},
		{
			name:     "invalid type",
			input:    []byte{1, 2, 3},
			colType:  types.ColumnTypeNumber,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDataManager_ConvertValue_Boolean(t *testing.T) {
	m := &dataManager{}
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "bool true",
			input:   true,
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: true,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "bool false",
			input:   false,
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: false,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "string true",
			input:   "true",
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: true,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "string TRUE",
			input:   "TRUE",
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: true,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "string 1",
			input:   "1",
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: true,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "string false",
			input:   "false",
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: false,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "string FALSE",
			input:   "FALSE",
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: false,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "string 0",
			input:   "0",
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: false,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "int 1 to boolean",
			input:   1,
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: true,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:    "int 0 to boolean",
			input:   0,
			colType: types.ColumnTypeBoolean,
			expected: &types.Value{
				Data: false,
				Type: types.ColumnTypeBoolean,
			},
		},
		{
			name:     "invalid string",
			input:    "maybe",
			colType:  types.ColumnTypeBoolean,
			hasError: true,
		},
		{
			name:     "invalid type",
			input:    []byte{1},
			colType:  types.ColumnTypeBoolean,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDataManager_ConvertValue_Timestamp(t *testing.T) {
	m := &dataManager{}
	now := time.Now()
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "time.Time",
			input:   now,
			colType: types.ColumnTypeTimestamp,
			expected: &types.Value{
				Data: now,
				Type: types.ColumnTypeTimestamp,
			},
		},
		{
			name:    "unix timestamp",
			input:   now.Unix(),
			colType: types.ColumnTypeTimestamp,
			expected: &types.Value{
				Data: time.Unix(now.Unix(), 0),
				Type: types.ColumnTypeTimestamp,
			},
		},
		{
			name:    "RFC3339 string",
			input:   now.Format(time.RFC3339),
			colType: types.ColumnTypeTimestamp,
			expected: &types.Value{
				Data: now,
				Type: types.ColumnTypeTimestamp,
			},
		},
		{
			name:    "date string",
			input:   "2023-12-01",
			colType: types.ColumnTypeTimestamp,
			expected: &types.Value{
				Data: time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC),
				Type: types.ColumnTypeTimestamp,
			},
		},
		{
			name:     "invalid string",
			input:    "not-a-timestamp",
			colType:  types.ColumnTypeTimestamp,
			hasError: true,
		},
		{
			name:     "invalid type",
			input:    []byte{1, 2, 3},
			colType:  types.ColumnTypeTimestamp,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				// For time comparison, we need to handle potential timezone differences
				actualTime := result.Data.(time.Time)
				expectedTime := tt.expected.Data.(time.Time)
				assert.WithinDuration(t, expectedTime, actualTime, time.Second)
				assert.Equal(t, tt.expected.Type, result.Type)
			}
		})
	}
}

func TestDataManager_ConvertValue_Binary(t *testing.T) {
	m := &dataManager{}
	testData := []byte{1, 2, 3, 4, 5}
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "byte slice",
			input:   testData,
			colType: types.ColumnTypeBinary,
			expected: &types.Value{
				Data: testData,
				Type: types.ColumnTypeBinary,
			},
		},
		{
			name:    "string to binary",
			input:   "hello",
			colType: types.ColumnTypeBinary,
			expected: &types.Value{
				Data: []byte("hello"),
				Type: types.ColumnTypeBinary,
			},
		},
		{
			name:     "invalid type",
			input:    123,
			colType:  types.ColumnTypeBinary,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDataManager_ConvertValue_JSON(t *testing.T) {
	m := &dataManager{}
	
	tests := []struct {
		name     string
		input    interface{}
		colType  types.ColumnType
		expected *types.Value
		hasError bool
	}{
		{
			name:    "nil value",
			input:   nil,
			colType: types.ColumnTypeJSON,
			expected: &types.Value{
				Data: nil,
				Type: types.ColumnTypeJSON,
			},
		},
		{
			name:    "valid JSON-compatible map",
			input:   map[string]interface{}{"key": "value"},
			colType: types.ColumnTypeJSON,
			expected: &types.Value{
				Data: map[string]interface{}{"key": "value"},
				Type: types.ColumnTypeJSON,
			},
		},
		{
			name:    "valid JSON-compatible slice",
			input:   []interface{}{"item1", "item2"},
			colType: types.ColumnTypeJSON,
			expected: &types.Value{
				Data: []interface{}{"item1", "item2"},
				Type: types.ColumnTypeJSON,
			},
		},
		{
			name:    "simple string",
			input:   "simple string",
			colType: types.ColumnTypeJSON,
			expected: &types.Value{
				Data: "simple string",
				Type: types.ColumnTypeJSON,
			},
		},
		{
			name:    "number",
			input:   42,
			colType: types.ColumnTypeJSON,
			expected: &types.Value{
				Data: 42,
				Type: types.ColumnTypeJSON,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.convertValue(tt.input, tt.colType)
			
			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDataManager_ConvertValue_NumericTypes(t *testing.T) {
	m := &dataManager{}
	
	// Test SmallInt
	t.Run("SmallInt", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected *types.Value
			hasError bool
		}{
			{
				name:  "int16",
				input: int16(100),
				expected: &types.Value{
					Data: int16(100),
					Type: types.ColumnTypeSmallInt,
				},
			},
			{
				name:  "valid int in range",
				input: 100,
				expected: &types.Value{
					Data: int16(100),
					Type: types.ColumnTypeSmallInt,
				},
			},
			{
				name:  "string number",
				input: "100",
				expected: &types.Value{
					Data: int16(100),
					Type: types.ColumnTypeSmallInt,
				},
			},
			{
				name:     "out of range positive",
				input:    40000,
				hasError: true,
			},
			{
				name:     "out of range negative",
				input:    -40000,
				hasError: true,
			},
			{
				name:     "invalid string",
				input:    "not-a-number",
				hasError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := m.convertValue(tt.input, types.ColumnTypeSmallInt)
				
				if tt.hasError {
					assert.Error(t, err)
					assert.Nil(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	// Test Integer
	t.Run("Integer", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected *types.Value
			hasError bool
		}{
			{
				name:  "int32",
				input: int32(100000),
				expected: &types.Value{
					Data: int32(100000),
					Type: types.ColumnTypeInteger,
				},
			},
			{
				name:  "valid int in range",
				input: 100000,
				expected: &types.Value{
					Data: int32(100000),
					Type: types.ColumnTypeInteger,
				},
			},
			{
				name:  "string number",
				input: "100000",
				expected: &types.Value{
					Data: int32(100000),
					Type: types.ColumnTypeInteger,
				},
			},
			{
				name:     "out of range positive",
				input:    3000000000, // > 2^31-1
				hasError: true,
			},
			{
				name:     "out of range negative",
				input:    -3000000000, // < -2^31
				hasError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := m.convertValue(tt.input, types.ColumnTypeInteger)
				
				if tt.hasError {
					assert.Error(t, err)
					assert.Nil(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	// Test BigInt
	t.Run("BigInt", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected *types.Value
			hasError bool
		}{
			{
				name:  "int64",
				input: int64(10000000000),
				expected: &types.Value{
					Data: int64(10000000000),
					Type: types.ColumnTypeBigInt,
				},
			},
			{
				name:  "int",
				input: 10000000000,
				expected: &types.Value{
					Data: int64(10000000000),
					Type: types.ColumnTypeBigInt,
				},
			},
			{
				name:  "string number",
				input: "10000000000",
				expected: &types.Value{
					Data: int64(10000000000),
					Type: types.ColumnTypeBigInt,
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := m.convertValue(tt.input, types.ColumnTypeBigInt)
				
				if tt.hasError {
					assert.Error(t, err)
					assert.Nil(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	// Test Real
	t.Run("Real", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected *types.Value
			hasError bool
		}{
			{
				name:  "float32",
				input: float32(3.14),
				expected: &types.Value{
					Data: float32(3.14),
					Type: types.ColumnTypeReal,
				},
			},
			{
				name:  "float64 to real",
				input: 3.14159,
				expected: &types.Value{
					Data: float32(3.14159),
					Type: types.ColumnTypeReal,
				},
			},
			{
				name:  "int to real",
				input: 42,
				expected: &types.Value{
					Data: float32(42),
					Type: types.ColumnTypeReal,
				},
			},
			{
				name:  "string number",
				input: "3.14",
				expected: &types.Value{
					Data: float32(3.14),
					Type: types.ColumnTypeReal,
				},
			},
			{
				name:     "invalid string",
				input:    "not-a-number",
				hasError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := m.convertValue(tt.input, types.ColumnTypeReal)
				
				if tt.hasError {
					assert.Error(t, err)
					assert.Nil(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	// Test Double
	t.Run("Double", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected *types.Value
			hasError bool
		}{
			{
				name:  "float64",
				input: 3.14159265359,
				expected: &types.Value{
					Data: 3.14159265359,
					Type: types.ColumnTypeDouble,
				},
			},
			{
				name:  "float32 to double",
				input: float32(3.14),
				expected: &types.Value{
					Data: float64(float32(3.14)),
					Type: types.ColumnTypeDouble,
				},
			},
			{
				name:  "int to double",
				input: 42,
				expected: &types.Value{
					Data: float64(42),
					Type: types.ColumnTypeDouble,
				},
			},
			{
				name:  "string number",
				input: "3.14159",
				expected: &types.Value{
					Data: 3.14159,
					Type: types.ColumnTypeDouble,
				},
			},
			{
				name:     "invalid string",
				input:    "not-a-number",
				hasError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := m.convertValue(tt.input, types.ColumnTypeDouble)
				
				if tt.hasError {
					assert.Error(t, err)
					assert.Nil(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})

	// Test Numeric
	t.Run("Numeric", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected *types.Value
			hasError bool
		}{
			{
				name:  "string number",
				input: "123.456789",
				expected: &types.Value{
					Data: "123.456789",
					Type: types.ColumnTypeNumeric,
				},
			},
			{
				name:  "float64 to numeric",
				input: 123.456,
				expected: &types.Value{
					Data: "123.456",
					Type: types.ColumnTypeNumeric,
				},
			},
			{
				name:  "float32 to numeric",
				input: float32(123.456),
				expected: &types.Value{
					Data: "123.456",
					Type: types.ColumnTypeNumeric,
				},
			},
			{
				name:  "int to numeric",
				input: 123,
				expected: &types.Value{
					Data: "123",
					Type: types.ColumnTypeNumeric,
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := m.convertValue(tt.input, types.ColumnTypeNumeric)
				
				if tt.hasError {
					assert.Error(t, err)
					assert.Nil(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})
}

func TestDataManager_ConvertValue_UnsupportedType(t *testing.T) {
	m := &dataManager{}
	
	result, err := m.convertValue("test", "unsupported_type")
	
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "unsupported column type")
}

func TestDataManager_ConvertValue_NullValue(t *testing.T) {
	m := &dataManager{}
	
	result, err := m.convertValue(nil, types.ColumnTypeString)
	
	require.NoError(t, err)
	assert.Equal(t, &types.Value{Data: nil, Type: types.ColumnTypeString}, result)
}

func TestDataManager_InferColumnType(t *testing.T) {
	m := &dataManager{}
	
	tests := []struct {
		name     string
		input    interface{}
		expected types.ColumnType
	}{
		{
			name:     "string",
			input:    "hello",
			expected: types.ColumnTypeString,
		},
		{
			name:     "int",
			input:    42,
			expected: types.ColumnTypeNumber,
		},
		{
			name:     "int32",
			input:    int32(42),
			expected: types.ColumnTypeNumber,
		},
		{
			name:     "int64",
			input:    int64(42),
			expected: types.ColumnTypeNumber,
		},
		{
			name:     "float32",
			input:    float32(3.14),
			expected: types.ColumnTypeNumber,
		},
		{
			name:     "float64",
			input:    3.14159,
			expected: types.ColumnTypeNumber,
		},
		{
			name:     "bool",
			input:    true,
			expected: types.ColumnTypeBoolean,
		},
		{
			name:     "time.Time",
			input:    time.Now(),
			expected: types.ColumnTypeTimestamp,
		},
		{
			name:     "[]byte",
			input:    []byte{1, 2, 3},
			expected: types.ColumnTypeBinary,
		},
		{
			name:     "nil",
			input:    nil,
			expected: types.ColumnTypeString,
		},
		{
			name:     "complex object",
			input:    map[string]interface{}{"key": "value"},
			expected: types.ColumnTypeJSON,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.inferColumnType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDataManager_ToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{
			name:     "int",
			input:    42,
			expected: 42,
		},
		{
			name:     "int32",
			input:    int32(42),
			expected: 42,
		},
		{
			name:     "int64",
			input:    int64(42),
			expected: 42,
		},
		{
			name:     "unsupported type",
			input:    "string",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDataManager_InsertBatch(t *testing.T) {
	// This test requires a full setup with a real engine and KV store
	// We'll add this as an integration test in a separate test file
	t.Skip("Skipping InsertBatch test - requires integration test setup")
}