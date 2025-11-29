package pebble

import (
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

// mockCodec implements the codec interface for testing
type mockCodec struct {
	startKey []byte
	endKey   []byte
}

func (mc *mockCodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	return []byte("index_key"), nil
}

func (mc *mockCodec) EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error) {
	return []byte("composite_index_key"), nil
}

func (mc *mockCodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	return mc.startKey
}

func (mc *mockCodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	return mc.endKey
}

// Add required methods to satisfy the interface
func (mc *mockCodec) EncodeTableKey(tenantID, tableID, rowID int64) []byte { return nil }
func (mc *mockCodec) EncodeTableKeyBuffer(tenantID, tableID, rowID int64, buf []byte) ([]byte, error) { return nil, nil }
func (mc *mockCodec) EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error) { return nil, nil }
func (mc *mockCodec) EncodeMetaKey(tenantID int64, metaType string, key string) []byte { return nil }
func (mc *mockCodec) EncodeSequenceKey(tenantID int64, seqName string) []byte { return nil }
func (mc *mockCodec) DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error) { return 0, 0, 0, nil }
func (mc *mockCodec) DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 0, 0, 0, nil, 0, nil
}
func (mc *mockCodec) DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []dbTypes.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 0, 0, 0, nil, 0, nil
}
func (mc *mockCodec) DecodePKKey(key []byte) (tenantID, tableID int64, err error) { return 0, 0, nil }
func (mc *mockCodec) EncodeRow(row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]byte, error) { return nil, nil }
func (mc *mockCodec) DecodeRow(data []byte, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) { return nil, nil }
func (mc *mockCodec) EncodeValue(value interface{}, colType dbTypes.ColumnType) ([]byte, error) { return nil, nil }
func (mc *mockCodec) DecodeValue(data []byte, colType dbTypes.ColumnType) (interface{}, error) { return nil, nil }
func (mc *mockCodec) EncodeCompositeKey(values []interface{}, types []dbTypes.ColumnType) ([]byte, error) { return nil, nil }
func (mc *mockCodec) DecodeCompositeKey(data []byte, types []dbTypes.ColumnType) ([]interface{}, error) { return nil, nil }
func (mc *mockCodec) ExtractRowIDFromIndexKey(key []byte) (int64, error) { return 0, nil }
func (mc *mockCodec) ReleaseTableKey(buf []byte) {}
func (mc *mockCodec) ReleaseIndexKey(buf []byte) {}
func (mc *mockCodec) ReleaseCompositeIndexKey(buf []byte) {}
func (mc *mockCodec) ReleasePKKey(buf []byte) {}
func (mc *mockCodec) ReleaseMetaKey(buf []byte) {}
func (mc *mockCodec) ReleaseSequenceKey(buf []byte) {}
func (mc *mockCodec) ReleaseIndexScanKey(buf []byte) {}

func TestMultiColumnOptimizer_BuildIndexRangeFromFilter(t *testing.T) {
	// Create a mock codec for testing
	mockCodec := &mockCodec{
		startKey: []byte("start"),
		endKey:   []byte("end"),
	}
	
	optimizer := NewMultiColumnOptimizer(mockCodec)
	
	// Test case 1: Simple filter (should fall back to full scan)
	t.Run("SimpleFilter", func(t *testing.T) {
		filter := &engineTypes.FilterExpression{
			Type:     "simple",
			Column:   "col1",
			Operator: "=",
			Value:    "value1",
		}
		
		indexDef := &dbTypes.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should use simple index key encoding
		assert.Equal(t, []byte("index_key"), start)
		assert.Equal(t, []byte("index_key"), end)
	})
	
	// Test case 2: AND filter with multi-column index
	t.Run("AndFilterMultiColumn", func(t *testing.T) {
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
			Columns: []string{"col1", "col2"},
			Unique:  false,
			Type:    "btree",
		}
		
		start, end := optimizer.buildIndexRangeFromFilter(1, 1, 1, filter, indexDef)
		
		// Should use composite index key encoding
		assert.Equal(t, []byte("composite_index_key"), start)
		assert.Equal(t, []byte("composite_index_key"), end)
	})
	
	// Test case 3: AND filter with inequality operator
	t.Run("AndFilterInequality", func(t *testing.T) {
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
					Operator: ">",
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
		
		// Should use composite index key encoding for partial range with multiple values
		assert.Equal(t, []byte("composite_index_key"), start)
		assert.Equal(t, []byte("composite_index_key"), end)
	})
	
	// Test case 4: OR filter (should fall back to full scan)
	t.Run("OrFilter", func(t *testing.T) {
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
		
		assert.Equal(t, []byte("start"), start)
		assert.Equal(t, []byte("end"), end)
	})
	
	// Test case 5: Single column index with AND filter (should fall back to full scan)
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
}