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
	// Use pooled buffer for table keys
	buf := c.keyBufferPools.AcquireTableKeyBuffer()
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeRow))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, rowID)
	return buf
}

func (c *memcodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	// Use pooled buffer for index keys
	buf := c.keyBufferPools.AcquireIndexKeyBuffer()
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	valueBytes, err := c.encodeMemComparableValue(indexValue)
	if err != nil {
		c.keyBufferPools.ReleaseIndexKeyBuffer(buf)
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

	// Use pooled buffer for composite index keys
	buf := c.keyBufferPools.AcquireCompositeIndexKeyBuffer()
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	// Encode each index value
	for _, value := range indexValues {
		valueBytes, err := c.encodeMemComparableValue(value)
		if err != nil {
			c.keyBufferPools.ReleaseCompositeIndexKeyBuffer(buf)
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeCompositeIndexKey", "failed to encode index value")
		}
		buf = append(buf, valueBytes...)
	}

	buf = appendMemComparableInt64(buf, rowID)
	return buf, nil
}

// EncodeIndexScanStartKey creates a start key for index scanning
func (c *memcodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	// Use pooled buffer for index scan keys
	buf := c.keyBufferPools.AcquireIndexScanKeyBuffer()
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)
	return buf
}

// EncodeIndexScanEndKey creates an end key for index scanning
func (c *memcodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	// Use pooled buffer for index scan keys
	buf := c.keyBufferPools.AcquireFromSizeTieredPool(33)
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
	// Use pooled buffer for PK keys
	buf := c.keyBufferPools.AcquirePKKeyBuffer()
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypePK))
	buf = appendMemComparableInt64(buf, tableID)

	valueBytes, err := c.encodeMemComparableValue(pkValue)
	if err != nil {
		c.keyBufferPools.ReleasePKKeyBuffer(buf)
		return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodePKKey", "failed to encode pk value")
	}
	buf = append(buf, valueBytes...)

	return buf, nil
}

// EncodeMetaKey encodes a metadata key
func (c *memcodec) EncodeMetaKey(tenantID int64, metaType string, key string) []byte {
	// Calculate approximate size needed
	sizeHint := 1 + 8 + len(metaType) + 1 + len(key) // keyType + tenantID + metaType + separator + key
	if sizeHint < 64 {
		sizeHint = 64 // Minimum size for meta key buffer
	}
	
	// Use pooled buffer for meta keys
	buf := c.keyBufferPools.AcquireFromSizeTieredPool(sizeHint)
	buf = append(buf, byte(KeyTypeMeta))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, metaType...)
	buf = append(buf, ':')
	buf = append(buf, key...)
	return buf
}

// EncodeSequenceKey encodes a sequence key
func (c *memcodec) EncodeSequenceKey(tenantID int64, seqName string) []byte {
	// Use pooled buffer for sequence keys
	buf := c.keyBufferPools.AcquireSequenceKeyBuffer()
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
	// If buffer is provided, use it; otherwise acquire from pool
	shouldRelease := false
	if buf == nil {
		buf = c.keyBufferPools.AcquireTableKeyBuffer()
		shouldRelease = true
	} else {
		buf = buf[:0]
	}

	// Encode the key components
	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeRow))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, rowID)

	// If we acquired from pool but the caller provided their own buffer, release back to pool
	if shouldRelease && cap(buf) != 32 {
		c.keyBufferPools.ReleaseToSizeTieredPool(buf)
	}

	return buf, nil
}

// EncodeIndexKeyBuffer encodes an index key into the provided buffer, reusing it when possible
func (c *memcodec) EncodeIndexKeyBuffer(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64, buf []byte) ([]byte, error) {
	// If buffer is provided, use it; otherwise acquire from pool
	shouldRelease := false
	if buf == nil {
		buf = c.keyBufferPools.AcquireIndexKeyBuffer()
		shouldRelease = true
	} else {
		buf = buf[:0]
	}

	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	valueBytes, err := c.encodeMemComparableValue(indexValue)
	if err != nil {
		// If we acquired from pool, release it back
		if shouldRelease {
			c.keyBufferPools.ReleaseIndexKeyBuffer(buf)
		}
		return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeIndexKeyBuffer", "failed to encode index value")
	}
	buf = append(buf, valueBytes...)
	buf = appendMemComparableInt64(buf, rowID)

	// If we acquired from pool but the caller provided their own buffer, release back to pool
	if shouldRelease && cap(buf) != 64 {
		c.keyBufferPools.ReleaseToSizeTieredPool(buf)
	}

	return buf, nil
}

// EncodeCompositeIndexKeyBuffer encodes a composite index key into the provided buffer, reusing it when possible
func (c *memcodec) EncodeCompositeIndexKeyBuffer(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, buf []byte) ([]byte, error) {
	if len(indexValues) == 0 {
		return nil, errors.New(errors.ErrCodeCodec, "index values cannot be empty")
	}

	// If buffer is provided, use it; otherwise acquire from pool
	shouldRelease := false
	if buf == nil {
		buf = c.keyBufferPools.AcquireCompositeIndexKeyBuffer()
		shouldRelease = true
	} else {
		buf = buf[:0]
	}

	buf = append(buf, byte(KeyTypeTable))
	buf = appendMemComparableInt64(buf, tenantID)
	buf = append(buf, byte(KeyTypeIndex))
	buf = appendMemComparableInt64(buf, tableID)
	buf = appendMemComparableInt64(buf, indexID)

	// Encode each index value
	for _, value := range indexValues {
		valueBytes, err := c.encodeMemComparableValue(value)
		if err != nil {
			// If we acquired from pool, release it back
			if shouldRelease {
				c.keyBufferPools.ReleaseCompositeIndexKeyBuffer(buf)
			}
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "EncodeCompositeIndexKeyBuffer", "failed to encode index value")
		}
		buf = append(buf, valueBytes...)
	}

	buf = appendMemComparableInt64(buf, rowID)

	// If we acquired from pool but the caller provided their own buffer, release back to pool
	if shouldRelease && cap(buf) != 128 {
		c.keyBufferPools.ReleaseToSizeTieredPool(buf)
	}

	return buf, nil
}

// ReleaseTableKey releases a table key buffer back to the pool
func (c *memcodec) ReleaseTableKey(buf []byte) {
	c.keyBufferPools.ReleaseTableKeyBuffer(buf)
}

// ReleaseIndexKey releases an index key buffer back to the pool
func (c *memcodec) ReleaseIndexKey(buf []byte) {
	c.keyBufferPools.ReleaseIndexKeyBuffer(buf)
}

// ReleaseCompositeIndexKey releases a composite index key buffer back to the pool
func (c *memcodec) ReleaseCompositeIndexKey(buf []byte) {
	c.keyBufferPools.ReleaseCompositeIndexKeyBuffer(buf)
}

// ReleasePKKey releases a primary key buffer back to the pool
func (c *memcodec) ReleasePKKey(buf []byte) {
	c.keyBufferPools.ReleasePKKeyBuffer(buf)
}

// ReleaseMetaKey releases a metadata key buffer back to the pool
func (c *memcodec) ReleaseMetaKey(buf []byte) {
	c.keyBufferPools.ReleaseMetaKeyBuffer(buf)
}

// ReleaseSequenceKey releases a sequence key buffer back to the pool
func (c *memcodec) ReleaseSequenceKey(buf []byte) {
	c.keyBufferPools.ReleaseSequenceKeyBuffer(buf)
}

// ReleaseIndexScanKey releases an index scan key buffer back to the pool
func (c *memcodec) ReleaseIndexScanKey(buf []byte) {
	c.keyBufferPools.ReleaseIndexScanKeyBuffer(buf)
}