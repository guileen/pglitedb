package codec

import (
	"bytes"
	"encoding/json"
	
	"github.com/google/uuid"
	"github.com/guileen/pglitedb/engine/errors"
	"github.com/guileen/pglitedb/types"
)

// =============================================================================
// ENCODING METHODS
// =============================================================================

func (c *memcodec) EncodeTableKey(tenantID, tableID, rowID int64) []byte {
	// Pre-allocate with a reasonable size for table keys
	buf := make([]byte, 0, 32)
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeRow))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, rowID)
	return buf
}

func (c *memcodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	// Pre-allocate with a reasonable size for index keys
	buf := make([]byte, 0, 64)
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	valueBytes, err := c.encodeMemComparableValue(indexValue)
	if err != nil {
		return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeIndexKey", "failed to encode index value")
	}
	buf = append(buf, valueBytes...)
	buf = appendMemComparableInt64(buf, rowID)

	return buf, nil
}

func (c *memcodec) EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error) {
	if len(indexValues) == 0 {
		return nil, errors.New(errors.ErrCodeCodec, "index values cannot be empty")
	}

	// Pre-allocate with a reasonable size for composite index keys
	buf := make([]byte, 0, 128)
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	// Encode each index value
	for _, value := range indexValues {
		valueBytes, err := c.encodeMemComparableValue(value)
		if err != nil {
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeCompositeIndexKey", "failed to encode index value")
		}
		buf = append(buf, valueBytes...)
	}

	buf = appendMemComparableInt64(buf, rowID)
	return buf, nil
}

// EncodeIndexScanStartKey creates a start key for index scanning
func (c *memcodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	// Pre-allocate with a reasonable size for index scan keys
	buf := make([]byte, 0, 32)
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)
	return buf
}

// EncodeIndexScanEndKey creates an end key for index scanning
func (c *memcodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	// Pre-allocate with a reasonable size for index scan keys
	buf := make([]byte, 0, 33)
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	// Add a max byte to ensure we scan all indexes for this table+index
	buf = append(buf, maxFlag)
	return buf
}

// EncodePKKey encodes a primary key
func (c *memcodec) EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error) {
	// Pre-allocate with a reasonable size for PK keys
	buf := make([]byte, 0, 64)
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypePK))
	buf = appendMemComparableInt64(buf, tableID)

	valueBytes, err := c.encodeMemComparableValue(pkValue)
	if err != nil {
		return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodePKKey", "failed to encode pk value")
	}
	buf = append(buf, valueBytes...)

	return buf, nil
}

// EncodeMetaKey encodes a metadata key
func (c *memcodec) EncodeMetaKey(tenantID int64, metaType string, key string) []byte {
	// Pre-allocate with a reasonable size for meta keys
	buf := make([]byte, 0, 64)
	buf = append(buf, byte(KeyTypeMeta))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, metaType...)
	buf = append(buf, ':')
	buf = append(buf, key...)
	return buf
}

// EncodeSequenceKey encodes a sequence key
func (c *memcodec) EncodeSequenceKey(tenantID int64, seqName string) []byte {
	// Pre-allocate with a reasonable size for sequence keys
	buf := make([]byte, 0, 32)
	buf = append(buf, byte(KeyTypeSequence))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, seqName...)
	return buf
}

func (c *memcodec) EncodeRow(row *types.Record, schemaDef *types.TableDefinition) ([]byte, error) {
	// Use pooled EncodedRow object
	encoded := AcquireEncodedRow()
	encoded.SchemaVersion = uint32(schemaDef.Version)
	encoded.CreatedAt = row.CreatedAt.Unix()
	encoded.UpdatedAt = row.UpdatedAt.Unix()
	encoded.Version = row.Version
	// Transaction metadata will be set by the transaction implementation
	encoded.TxnID = 0
	encoded.TxnTimestamp = 0

	for colName, value := range row.Data {
		var colType types.ColumnType
		for _, col := range schemaDef.Columns {
			if col.Name == colName {
				colType = col.Type
				break
			}
		}

		if colType == "" {
			ReleaseEncodedRow(encoded) // Return to pool on error
			return nil, errors.Errorf(errors.ErrCodeCodec, "column %s not found in schema", colName)
		}

		valueBytes, err := c.EncodeValue(value.Data, colType)
		if err != nil {
			ReleaseEncodedRow(encoded) // Return to pool on error
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeRow", "failed to encode column %s", colName)
		}
		encoded.Columns[colName] = valueBytes
	}

	return encodeEncodedRow(encoded)
}

func (c *memcodec) EncodeValue(value interface{}, colType types.ColumnType) ([]byte, error) {
	if value == nil {
		return []byte{nilFlag}, nil
	}

	switch colType {
	case types.ColumnTypeString, types.ColumnTypeText:
		return encodeString(value)
	case types.ColumnTypeNumber, types.ColumnTypeSmallInt, types.ColumnTypeInteger, types.ColumnTypeBigInt:
		return encodeNumber(value)
	case types.ColumnTypeBoolean:
		return encodeBoolean(value)
	case types.ColumnTypeTimestamp, types.ColumnTypeDate:
		return encodeTimestamp(value)
	case types.ColumnTypeJSON, types.ColumnTypeJSONB:
		return encodeJSON(value)
	case types.ColumnTypeUUID:
		return encodeUUID(value)
	case types.ColumnTypeBinary:
		return encodeBytes(value)
	default:
		return nil, errors.Errorf(errors.ErrCodeCodec, "unsupported column type: %s", colType)
	}
}

func (c *memcodec) EncodeCompositeKey(values []interface{}, types []types.ColumnType) ([]byte, error) {
	if len(values) != len(types) {
		return nil, errors.New(errors.ErrCodeCodec, "values and types length mismatch")
	}

	buf := &bytes.Buffer{}
	for i, value := range values {
		valueBytes, err := c.EncodeValue(value, types[i])
		if err != nil {
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeCompositeKey", "failed to encode value %d", i)
		}
		buf.Write(valueBytes)
	}

	return buf.Bytes(), nil
}

func (c *memcodec) encodeMemComparableValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return []byte{nilFlag}, nil
	case bool:
		buf := &bytes.Buffer{}
		if v {
			buf.WriteByte(0x01)
		} else {
			buf.WriteByte(0x00)
		}
		return buf.Bytes(), nil
	case int, int8, int16, int32, int64:
		buf := &bytes.Buffer{}
		buf.WriteByte(intFlag)
		writeMemComparableInt64(buf, toInt64(v))
		return buf.Bytes(), nil
	case uint, uint8, uint16, uint32, uint64:
		buf := &bytes.Buffer{}
		buf.WriteByte(uintFlag)
		writeMemComparableUint64(buf, toUint64(v))
		return buf.Bytes(), nil
	case float32, float64:
		buf := &bytes.Buffer{}
		buf.WriteByte(floatFlag)
		writeMemComparableFloat64(buf, toFloat64(v))
		return buf.Bytes(), nil
	case string:
		buf := &bytes.Buffer{}
		buf.WriteByte(bytesFlag)
		writeMemComparableString(buf, v)
		return buf.Bytes(), nil
	case []byte:
		buf := &bytes.Buffer{}
		buf.WriteByte(bytesFlag)
		writeMemComparableBytes(buf, v)
		return buf.Bytes(), nil
	case uuid.UUID:
		buf := &bytes.Buffer{}
		buf.WriteByte(bytesFlag)
		writeMemComparableBytes(buf, v[:])
		return buf.Bytes(), nil
	default:
		// Try to handle as JSON for complex types
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Errorf(errors.ErrCodeCodec, "unsupported value type: %T", value)
		}
		buf := &bytes.Buffer{}
		buf.WriteByte(jsonFlag)
		writeMemComparableBytes(buf, jsonBytes)
		return buf.Bytes(), nil
	}
}

// EncodeTableKeyBuffer encodes a table key into the provided buffer, reusing it when possible
func (c *memcodec) EncodeTableKeyBuffer(tenantID, tableID, rowID int64, buf []byte) ([]byte, error) {
	// Reuse buffer when possible to eliminate allocations
	if buf == nil {
		buf = make([]byte, 0, 32) // Pre-allocate with reasonable size
	}
	buf = buf[:0]

	// Encode the key components
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeRow))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, rowID)

	return buf, nil
}

// EncodeIndexKeyBuffer encodes an index key into the provided buffer, reusing it when possible
func (c *memcodec) EncodeIndexKeyBuffer(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64, buf []byte) ([]byte, error) {
	// Reuse buffer when possible to eliminate allocations
	if buf == nil {
		buf = make([]byte, 0, 64) // Pre-allocate with reasonable size
	}
	buf = buf[:0]

	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	valueBytes, err := c.encodeMemComparableValue(indexValue)
	if err != nil {
		return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeIndexKeyBuffer", "failed to encode index value")
	}
	buf = append(buf, valueBytes...)
	buf = appendMemComparableInt64(buf, rowID)

	return buf, nil
}

// EncodeCompositeIndexKeyBuffer encodes a composite index key into the provided buffer, reusing it when possible
func (c *memcodec) EncodeCompositeIndexKeyBuffer(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, buf []byte) ([]byte, error) {
	if len(indexValues) == 0 {
		return nil, errors.New(errors.ErrCodeCodec, "index values cannot be empty")
	}

	// Reuse buffer when possible to eliminate allocations
	if buf == nil {
		buf = make([]byte, 0, 128) // Pre-allocate with reasonable size
	}
	buf = buf[:0]

	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	// Encode each index value
	for _, value := range indexValues {
		valueBytes, err := c.encodeMemComparableValue(value)
		if err != nil {
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeCompositeIndexKeyBuffer", "failed to encode index value")
		}
		buf = append(buf, valueBytes...)
	}

	buf = appendMemComparableInt64(buf, rowID)
	return buf, nil
}