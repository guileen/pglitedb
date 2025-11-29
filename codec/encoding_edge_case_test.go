package codec

import (
	"testing"
	"time"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeRowWithEmptyColumnName(t *testing.T) {
	codec := NewMemComparableCodec()

	// Create a schema with valid columns
	schema := &types.TableDefinition{
		Version: 1,
		Columns: []types.ColumnDefinition{
			{
				Name: "name",
				Type: types.ColumnTypeString,
			},
			{
				Name: "age",
				Type: types.ColumnTypeNumber,
			},
		},
	}

	// Create a record with an empty column name
	record := &types.Record{
		Data: map[string]*types.Value{
			"":      {Data: "empty_key_value", Type: types.ColumnTypeString},
			"name":  {Data: "John Doe", Type: types.ColumnTypeString},
			"age":   {Data: 30, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Version:   1,
	}

	// This should fail with empty column name error
	_, err := codec.EncodeRow(record, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty column name found in row data")
}

func TestEncodeRowWithSchemaEmptyColumnName(t *testing.T) {
	codec := NewMemComparableCodec()

	// Create a schema with an empty column name (this should not happen in practice but let's test it)
	schema := &types.TableDefinition{
		Version: 1,
		Columns: []types.ColumnDefinition{
			{
				Name: "",
				Type: types.ColumnTypeString,
			},
			{
				Name: "age",
				Type: types.ColumnTypeNumber,
			},
		},
	}

	// Create a record with valid data
	record := &types.Record{
		Data: map[string]*types.Value{
			"name": {Data: "John Doe", Type: types.ColumnTypeString},
			"age":  {Data: 30, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Version:   1,
	}

	// This should fail with schema validation error
	_, err := codec.EncodeRow(record, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema contains column with empty name")
}

func TestEncodeRowWithSchemaEmptyColumnType(t *testing.T) {
	codec := NewMemComparableCodec()

	// Create a schema with an empty column type
	schema := &types.TableDefinition{
		Version: 1,
		Columns: []types.ColumnDefinition{
			{
				Name: "name",
				Type: "", // Empty type
			},
			{
				Name: "age",
				Type: types.ColumnTypeNumber,
			},
		},
	}

	// Create a record with valid data
	record := &types.Record{
		Data: map[string]*types.Value{
			"name": {Data: "John Doe", Type: types.ColumnTypeString},
			"age":  {Data: 30, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Version:   1,
	}

	// This should fail with schema validation error
	_, err := codec.EncodeRow(record, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema column 'name' has empty type")
}

func TestEncodeValueWithEmptyColumnType(t *testing.T) {
	codec := NewMemComparableCodec()

	// This should fail with empty column type error
	_, err := codec.EncodeValue("test value", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty column type is not supported")
}