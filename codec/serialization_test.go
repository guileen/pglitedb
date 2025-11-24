package codec

import (
	"testing"

	"github.com/google/uuid"
	"github.com/guileen/pglitedb/types"
)

func TestJSONEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	// Test JSON encoding and decoding
	testData := map[string]interface{}{
		"name":   "John Doe",
		"age":    30,
		"active": true,
		"address": map[string]interface{}{
			"street": "123 Main St",
			"city":   "Anytown",
		},
	}

	encoded, err := codec.EncodeValue(testData, types.ColumnTypeJSON)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := codec.DecodeValue(encoded, types.ColumnTypeJSON)
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	// Compare the decoded data
	decodedMap, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got %T", decoded)
	}

	if decodedMap["name"] != testData["name"] {
		t.Errorf("Expected name %s, got %s", testData["name"], decodedMap["name"])
	}

	// For numeric values, we need to handle type differences
	decodedAge, ok := decodedMap["age"].(float64)
	if !ok {
		t.Fatalf("Expected age to be float64, got %T", decodedMap["age"])
	}

	if int(decodedAge) != testData["age"] {
		t.Errorf("Expected age %v, got %v", testData["age"], int(decodedAge))
	}

	if decodedMap["active"] != testData["active"] {
		t.Errorf("Expected active %v, got %v", testData["active"], decodedMap["active"])
	}
}

func TestUUIDEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	// Test UUID encoding and decoding
	testUUID := "550e8400-e29b-41d4-a716-446655440000"

	encoded, err := codec.EncodeValue(testUUID, types.ColumnTypeUUID)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := codec.DecodeValue(encoded, types.ColumnTypeUUID)
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	// Compare the decoded UUID
	decodedUUID, ok := decoded.(uuid.UUID)
	if !ok {
		t.Fatalf("Expected uuid.UUID, got %T", decoded)
	}

	if decodedUUID.String() != testUUID {
		t.Errorf("Expected UUID %s, got %s", testUUID, decodedUUID.String())
	}
}

func TestBinaryEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	// Test binary encoding and decoding
	testData := []byte("Hello, World!")

	encoded, err := codec.EncodeValue(testData, types.ColumnTypeBinary)
	if err != nil {
		t.Fatalf("EncodeValue failed: %v", err)
	}

	decoded, err := codec.DecodeValue(encoded, types.ColumnTypeBinary)
	if err != nil {
		t.Fatalf("DecodeValue failed: %v", err)
	}

	// Compare the decoded data
	decodedBytes, ok := decoded.([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T", decoded)
	}

	if string(decodedBytes) != string(testData) {
		t.Errorf("Expected %s, got %s", string(testData), string(decodedBytes))
	}
}

func TestTypeConverter(t *testing.T) {
	converter := NewTypeConverter()

	// Test JSON serialization
	jsonData := map[string]interface{}{"key": "value"}
	serialized, err := converter.Serialize(jsonData, types.ColumnTypeJSON)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := converter.Deserialize(serialized, types.ColumnTypeJSON)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Test UUID serialization
	uuidStr := "550e8400-e29b-41d4-a716-446655440000"
	serialized, err = converter.Serialize(uuidStr, types.ColumnTypeUUID)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err = converter.Deserialize(serialized, types.ColumnTypeUUID)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	uuidObj, ok := deserialized.(uuid.UUID)
	if !ok {
		t.Fatalf("Expected uuid.UUID, got %T", deserialized)
	}

	if uuidObj.String() != uuidStr {
		t.Errorf("Expected UUID %s, got %s", uuidStr, uuidObj.String())
	}
}

func TestTypeValidator(t *testing.T) {
	validator := NewTypeValidator()

	// Test valid JSON
	jsonData := map[string]interface{}{"key": "value"}
	if !validator.IsCompatibleType(jsonData, types.ColumnTypeJSON) {
		t.Error("Valid JSON should be compatible with JSON column type")
	}

	// Test invalid JSON
	invalidData := make(chan int)
	if validator.IsCompatibleType(invalidData, types.ColumnTypeJSON) {
		t.Error("Invalid JSON should not be compatible with JSON column type")
	}

	// Test valid UUID
	uuidStr := "550e8400-e29b-41d4-a716-446655440000"
	if !validator.IsCompatibleType(uuidStr, types.ColumnTypeUUID) {
		t.Error("Valid UUID string should be compatible with UUID column type")
	}

	// Test invalid UUID
	invalidUUID := "invalid-uuid"
	if validator.IsCompatibleType(invalidUUID, types.ColumnTypeUUID) {
		t.Error("Invalid UUID string should not be compatible with UUID column type")
	}

	// Test valid binary
	binaryData := []byte("test data")
	if !validator.IsCompatibleType(binaryData, types.ColumnTypeBinary) {
		t.Error("Valid binary data should be compatible with binary column type")
	}
}

func TestTypeConversion(t *testing.T) {
	validator := NewTypeValidator()

	// Test string to number conversion
	result, err := validator.ConvertValue("123", types.ColumnTypeString, types.ColumnTypeNumber)
	if err != nil {
		t.Fatalf("ConvertValue failed: %v", err)
	}

	if result != 123.0 {
		t.Errorf("Expected 123.0, got %v", result)
	}

	// Test number to string conversion
	result, err = validator.ConvertValue(123, types.ColumnTypeNumber, types.ColumnTypeString)
	if err != nil {
		t.Fatalf("ConvertValue failed: %v", err)
	}

	if result != "123" {
		t.Errorf("Expected '123', got %v", result)
	}

	// Test string to boolean conversion
	result, err = validator.ConvertValue("true", types.ColumnTypeString, types.ColumnTypeBoolean)
	if err != nil {
		t.Fatalf("ConvertValue failed: %v", err)
	}

	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// Test boolean to number conversion
	result, err = validator.ConvertValue(true, types.ColumnTypeBoolean, types.ColumnTypeNumber)
	if err != nil {
		t.Fatalf("ConvertValue failed: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %v", result)
	}

	// Test UUID conversions
	uuidStr := "550e8400-e29b-41d4-a716-446655440000"
	result, err = validator.ConvertValue(uuidStr, types.ColumnTypeUUID, types.ColumnTypeString)
	if err != nil {
		t.Fatalf("ConvertValue failed: %v", err)
	}

	if result != uuidStr {
		t.Errorf("Expected %s, got %s", uuidStr, result)
	}
}
