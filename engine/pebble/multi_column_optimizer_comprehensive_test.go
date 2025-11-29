package pebble

import (
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

// enhancedMockCodec extends the existing mockCodec with more detailed behavior
type enhancedMockCodec struct {
	startKey []byte
	endKey   []byte
}

func (emc *enhancedMockCodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	// Create a more realistic key based on inputs
	key := []byte("index_key")
	if indexValue != nil {
		// Append the index value to make keys distinguishable
		switch v := indexValue.(type) {
		case string:
			key = append(key, []byte(v)...)
		case int:
			key = append(key, byte(v))
		case int64:
			key = append(key, byte(v))
		}
	}
	// Append rowID to make keys unique
	key = append(key, byte(rowID))
	return key, nil
}

func (emc *enhancedMockCodec) EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error) {
	// Create a composite key based on all values
	key := []byte("composite_index_key")
	for _, val := range indexValues {
		switch v := val.(type) {
		case string:
			key = append(key, []byte(v)...)
		case int:
			key = append(key, byte(v))
		case int64:
			key = append(key, byte(v))
		}
	}
	// Append rowID to make keys unique
	key = append(key, byte(rowID))
	return key, nil
}

func (emc *enhancedMockCodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	return emc.startKey
}

func (emc *enhancedMockCodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	return emc.endKey
}

// Add required methods to satisfy the interface
func (emc *enhancedMockCodec) EncodeTableKey(tenantID, tableID, rowID int64) []byte { return nil }
func (emc *enhancedMockCodec) EncodeTableKeyBuffer(tenantID, tableID, rowID int64, buf []byte) ([]byte, error) { return nil, nil }
func (emc *enhancedMockCodec) EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error) { return nil, nil }
func (emc *enhancedMockCodec) EncodeMetaKey(tenantID int64, metaType string, key string) []byte { return nil }
func (emc *enhancedMockCodec) EncodeSequenceKey(tenantID int64, seqName string) []byte { return nil }
func (emc *enhancedMockCodec) DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error) { return 0, 0, 0, nil }
func (emc *enhancedMockCodec) DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 0, 0, 0, nil, 0, nil
}
func (emc *enhancedMockCodec) DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []dbTypes.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 0, 0, 0, nil, 0, nil
}
func (emc *enhancedMockCodec) DecodePKKey(key []byte) (tenantID, tableID int64, err error) { return 0, 0, nil }
func (emc *enhancedMockCodec) EncodeRow(row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]byte, error) { return nil, nil }
func (emc *enhancedMockCodec) DecodeRow(data []byte, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) { return nil, nil }
func (emc *enhancedMockCodec) EncodeValue(value interface{}, colType dbTypes.ColumnType) ([]byte, error) { return nil, nil }
func (emc *enhancedMockCodec) DecodeValue(data []byte, colType dbTypes.ColumnType) (interface{}, error) { return nil, nil }
func (emc *enhancedMockCodec) EncodeCompositeKey(values []interface{}, types []dbTypes.ColumnType) ([]byte, error) { return nil, nil }
func (emc *enhancedMockCodec) DecodeCompositeKey(data []byte, types []dbTypes.ColumnType) ([]interface{}, error) { return nil, nil }
func (emc *enhancedMockCodec) ExtractRowIDFromIndexKey(key []byte) (int64, error) { return 0, nil }
func (emc *enhancedMockCodec) ReleaseTableKey(buf []byte) {}
func (emc *enhancedMockCodec) ReleaseIndexKey(buf []byte) {}
func (emc *enhancedMockCodec) ReleaseCompositeIndexKey(buf []byte) {}
func (emc *enhancedMockCodec) ReleasePKKey(buf []byte) {}
func (emc *enhancedMockCodec) ReleaseMetaKey(buf []byte) {}
func (emc *enhancedMockCodec) ReleaseSequenceKey(buf []byte) {}
func (emc *enhancedMockCodec) ReleaseIndexScanKey(buf []byte) {}

func TestMultiColumnOptimizer_EqualityConditions(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Multiple equality conditions on all index columns
	t.Run("MultipleEqualityConditions", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "value1",
				},
				{
					Type:     "simple",
					Column:   "col2",
					Operator: "=",
					Value:    "value2",
				},
				{
					Type:     "simple",
					Column:   "col3",
					Operator: "=",
					Value:    "value3",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2", "col3"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Just verify that start and end are not equal to the fallback full scan keys
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
	
	// Test case: Single equality condition
	t.Run("SingleEqualityCondition", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type:     "simple",
			Column:   "col1",
			Operator: "=",
			Value:    "single_value",
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should use simple index key encoding
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
}

func TestMultiColumnOptimizer_MixedConditions(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Mix of equality and inequality conditions
	t.Run("EqualityAndInequalityMix", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "equal_value",
				},
				{
					Type:     "simple",
					Column:   "col2",
					Operator: ">",
					Value:    "greater_value",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should use composite index key encoding for partial range
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
	
	// Test case: Inequality as first condition
	t.Run("InequalityFirstCondition", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: ">",
					Value:    "greater_value",
				},
				{
					Type:     "simple",
					Column:   "col2",
					Operator: "=",
					Value:    "equal_value",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should use composite index key encoding but stop at inequality
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
}

func TestMultiColumnOptimizer_PrefixMatching(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Only prefix columns have conditions
	t.Run("PrefixColumnConditions", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "prefix_value",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2", "col3"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should use simple index key encoding for the first column only
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
	
	// Test case: Gap in column conditions
	t.Run("GapInColumnConditions", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "first_value",
				},
				{
					Type:     "simple",
					Column:   "col3",
					Operator: "=",
					Value:    "third_value",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2", "col3"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should only use the first column since col2 is missing
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
}

func TestMultiColumnOptimizer_EdgeCases(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Empty filter
	t.Run("EmptyFilter", func(t *testing.T) {
		var filter *engineTypes.FilterExpression = nil
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should fall back to full scan
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case: Single column index
	t.Run("SingleColumnIndex", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "value1",
				},
				{
					Type:     "simple",
					Column:   "col2",
					Operator: "=",
					Value:    "value2",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1"}, // Single column
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should fall back to full scan for single column index with AND filter
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case: OR filter
	t.Run("ORFilter", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "or",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "value1",
				},
				{
					Type:     "simple",
					Column:   "col2",
					Operator: "=",
					Value:    "value2",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should fall back to full scan for OR filters
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case: NOT filter
	t.Run("NOTFilter", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "not",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "value1",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should fall back to full scan for NOT filters
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
}

func TestMultiColumnOptimizer_BoundaryConditions(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Max values
	t.Run("MaxValues", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    int64(9223372036854775807), // Max int64
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// For single column index with AND filter, should fall back to full scan
		// This matches the behavior in the existing test
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case: Min values
	t.Run("MinValues", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    int64(-9223372036854775808), // Min int64
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// For single column index with AND filter, should fall back to full scan
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case: String values
	t.Run("StringValues", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "test_string_value",
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// For single column index with AND filter, should fall back to full scan
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
}

func TestMultiColumnOptimizer_UnsupportedOperators(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Unsupported operator
	t.Run("UnsupportedOperator", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type:     "simple",
			Column:   "col1",
			Operator: "!=",
			Value:    "value1",
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should fall back to full scan for unsupported operators
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case: LIKE operator
	t.Run("LIKEOperator", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type:     "simple",
			Column:   "col1",
			Operator: "LIKE",
			Value:    "%value%",
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should fall back to full scan for unsupported operators
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
}

func TestMultiColumnOptimizer_ComplexFilterChains(t *testing.T) {
	mockCodec := &enhancedMockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case: Deeply nested AND conditions
	t.Run("DeeplyNestedAND", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type: "and",
			Children: []*engineTypes.FilterExpression{
				{
					Type:     "simple",
					Column:   "col1",
					Operator: "=",
					Value:    "value1",
				},
				{
					Type: "and",
					Children: []*engineTypes.FilterExpression{
						{
							Type:     "simple",
							Column:   "col2",
							Operator: "=",
							Value:    "value2",
						},
						{
							Type:     "simple",
							Column:   "col3",
							Operator: "=",
							Value:    "value3",
						},
					},
				},
			},
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2", "col3"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should extract all conditions and use composite key
		assert.NotEqual(t, []byte("start"), start)
		assert.NotEqual(t, []byte("end"), end)
		assert.NotNil(t, start)
		assert.NotNil(t, end)
	})
}