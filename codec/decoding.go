package codec

import (
	"time"

	"github.com/guileen/pglitedb/engine/errors"
	"github.com/guileen/pglitedb/types"
)

// =============================================================================
// DECODING METHODS
// =============================================================================

func (c *memcodec) DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []types.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		err = errors.New(errors.ErrCodeCodec, "invalid table key prefix")
		return
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypeIndex {
		err = errors.New(errors.ErrCodeCodec, "invalid index key marker")
		return
	}
	offset++

	tableID, n = readMemComparableInt64(key[offset:])
	offset += n

	indexID, n = readMemComparableInt64(key[offset:])
	offset += n

	// Parse index values according to schema
	indexValues = make([]interface{}, 0, len(indexColumnTypes))

	for i, colType := range indexColumnTypes {
		if offset >= len(key) {
			err = errors.Errorf(errors.ErrCodeCodec, "unexpected end of key at index value %d", i)
			return
		}

		// Determine value length based on type
		var valueEnd int

		switch colType {
		case types.ColumnTypeString, types.ColumnTypeText, types.ColumnTypeUUID, types.ColumnTypeBinary:
			// Find the end of the string/bytes value (terminated by 0x00 0x00)
			valueEnd = offset + 1
			for valueEnd < len(key)-1 {
				if key[valueEnd] == 0x00 && key[valueEnd+1] == 0x00 {
					valueEnd += 2
					break
				}
				valueEnd++
			}
			if valueEnd >= len(key) {
				err = errors.Errorf(errors.ErrCodeCodec, "invalid string/bytes encoding at index value %d", i)
				return
			}
		case types.ColumnTypeNumber, types.ColumnTypeTimestamp, types.ColumnTypeDate:
			// Fixed size: flag byte + 8 bytes for numeric values
			valueEnd = offset + 9
		case types.ColumnTypeBoolean:
			// Fixed size: 1 byte
			valueEnd = offset + 1
		default:
			// For other types, assume fixed 9 bytes (flag + 8 bytes)
			valueEnd = offset + 9
		}

		if valueEnd > len(key) {
			err = errors.Errorf(errors.ErrCodeCodec, "index key too short for value %d", i)
			return
		}

		value, decodeErr := c.DecodeValue(key[offset:valueEnd], colType)
		if decodeErr != nil {
			err = errors.Wrapf(decodeErr, errors.ErrCodeCodec, "DecodeIndexKeyWithSchema", "failed to decode index value %d", i)
			return
		}

		indexValues = append(indexValues, value)
		offset = valueEnd
	}

	// The remaining bytes should be the rowID (last 8 bytes)
	if len(key)-offset >= 8 {
		rowID, _ = readMemComparableInt64(key[len(key)-8:])
	} else {
		err = errors.New(errors.ErrCodeCodec, "missing rowID in index key")
		return
	}

	return tenantID, tableID, indexID, indexValues, rowID, nil
}

func (c *memcodec) DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		return 0, 0, 0, errors.New(errors.ErrCodeCodec, "invalid table key prefix")
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypeRow {
		return 0, 0, 0, errors.New(errors.ErrCodeCodec, "invalid row key marker")
	}
	offset++

	tableID, n = readMemComparableInt64(key[offset:])
	offset += n
	
	if offset >= len(key) {
		return 0, 0, 0, errors.New(errors.ErrCodeCodec, "incomplete table key: missing rowID")
	}

	rowID, _ = readMemComparableInt64(key[offset:])

	return tenantID, tableID, rowID, nil
}

func (c *memcodec) DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		err = errors.New(errors.ErrCodeCodec, "invalid table key prefix")
		return
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypeIndex {
		err = errors.New(errors.ErrCodeCodec, "invalid index key marker")
		return
	}
	offset++

	tableID, n = readMemComparableInt64(key[offset:])
	offset += n
	
	if offset > len(key) {
		err = errors.New(errors.ErrCodeCodec, "incomplete index key: missing indexID")
		return
	}

	indexID, n = readMemComparableInt64(key[offset:])
	offset += n

	// Parse index values (could be composite)
	indexValues = make([]interface{}, 0)

	// Skip to the end to find rowID
	// In a real implementation, we would parse the index values properly
	// For now, we'll assume the rowID is at the end (last 8 bytes)
	if len(key) >= 8 {
		rowID, _ = readMemComparableInt64(key[len(key)-8:])
	}

	return tenantID, tableID, indexID, indexValues, rowID, nil
}

// ExtractRowIDFromIndexKey efficiently extracts just the rowID from an index key
// This avoids the overhead of full key decoding when only the rowID is needed
func (c *memcodec) ExtractRowIDFromIndexKey(key []byte) (int64, error) {
	if len(key) < 8 {
		return 0, errors.New(errors.ErrCodeCodec, "key too short to contain rowID")
	}
	
	// RowID is stored as the last 8 bytes of the key in memcomparable format
	rowID, _ := readMemComparableInt64(key[len(key)-8:])
	return rowID, nil
}

func (c *memcodec) DecodePKKey(key []byte) (tenantID, tableID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		return 0, 0, errors.New(errors.ErrCodeCodec, "invalid table key prefix")
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypePK {
		return 0, 0, errors.New(errors.ErrCodeCodec, "invalid pk key marker")
	}
	offset++

	tableID, n = readMemComparableInt64(key[offset:])
	offset += n
	
	if offset >= len(key) {
		return 0, 0, errors.New(errors.ErrCodeCodec, "incomplete pk key: missing tableID")
	}

	return tenantID, tableID, nil
}

func (c *memcodec) DecodeRow(data []byte, schemaDef *types.TableDefinition) (*types.Record, error) {
	encoded, err := decodeEncodedRow(data)
	if err != nil {
		return nil, errors.Wrapf(err, errors.ErrCodeCodec, "DecodeRow", "failed to decode encoded row")
	}

	// Reuse record from pool if available
	record := AcquireDecodedRecord()
	record.Table = schemaDef.Name
	// Clear the existing data and pre-allocate with known size to avoid reallocations
	for k := range record.Data {
		delete(record.Data, k)
	}
	record.CreatedAt = time.Unix(encoded.CreatedAt, 0)
	record.UpdatedAt = time.Unix(encoded.UpdatedAt, 0)
	record.Version = encoded.Version

	for _, col := range schemaDef.Columns {
		if valueBytes, ok := encoded.Columns[col.Name]; ok {
			value, err := c.DecodeValue(valueBytes, col.Type)
			if err != nil {
				ReleaseDecodedRecord(record) // Return to pool on error
				return nil, errors.Wrapf(err, errors.ErrCodeCodec, "DecodeRow", "failed to decode column %s", col.Name)
			}
			// Reuse value from pool if available
			recordValue := AcquireValue()
			recordValue.Data = value
			recordValue.Type = col.Type
			record.Data[col.Name] = recordValue
		}
	}

	// Ensure we release the encoded row back to the pool
	ReleaseEncodedRow(encoded)
	
	return record, nil
}

func (c *memcodec) DecodeValue(data []byte, colType types.ColumnType) (interface{}, error) {
	if len(data) == 0 {
		return nil, errors.New(errors.ErrCodeCodec, "empty data")
	}

	if data[0] == nilFlag {
		return nil, nil
	}

	switch colType {
	case types.ColumnTypeString, types.ColumnTypeText:
		return decodeString(data)
	case types.ColumnTypeNumber, types.ColumnTypeSmallInt, types.ColumnTypeInteger, types.ColumnTypeBigInt:
		return decodeNumber(data)
	case types.ColumnTypeBoolean:
		return decodeBoolean(data)
	case types.ColumnTypeTimestamp, types.ColumnTypeDate:
		return decodeTimestamp(data)
	case types.ColumnTypeJSON, types.ColumnTypeJSONB:
		return decodeJSON(data)
	case types.ColumnTypeUUID:
		return decodeUUID(data)
	case types.ColumnTypeBinary:
		return decodeBytes(data)
	default:
		return nil, errors.Errorf(errors.ErrCodeCodec, "unsupported column type: %s", colType)
	}
}

func (c *memcodec) DecodeCompositeKey(data []byte, colTypes []types.ColumnType) ([]interface{}, error) {
	values := make([]interface{}, 0, len(colTypes))
	offset := 0

	for i := 0; i < len(colTypes) && offset < len(data); i++ {
		colType := colTypes[i]
		var endIdx int

		flag := data[offset]
		if flag == nilFlag {
			values = append(values, nil)
			offset += 1
			continue
		}

		switch colType {
		case types.ColumnTypeString, types.ColumnTypeText, types.ColumnTypeUUID, types.ColumnTypeBinary:
			endIdx = offset + 1
			for endIdx < len(data) {
				if data[endIdx] == 0x00 {
					if endIdx+1 < len(data) && data[endIdx+1] == 0x00 {
						endIdx += 2
						break
					} else if endIdx+1 < len(data) && data[endIdx+1] == 0xFF {
						endIdx += 2
						continue
					}
				}
				endIdx++
			}
		case types.ColumnTypeNumber, types.ColumnTypeSmallInt, types.ColumnTypeInteger, types.ColumnTypeBigInt, types.ColumnTypeTimestamp, types.ColumnTypeDate:
			endIdx = offset + 9
		case types.ColumnTypeBoolean:
			endIdx = offset + 1
		default:
			return nil, errors.Errorf(errors.ErrCodeCodec, "unsupported column type: %s", colType)
		}

		if endIdx > len(data) {
			return nil, errors.Errorf(errors.ErrCodeCodec, "unexpected end of data at value %d", i)
		}

		value, err := c.DecodeValue(data[offset:endIdx], colType)
		if err != nil {
			return nil, errors.Wrapf(err, errors.ErrCodeCodec, "DecodeCompositeKey", "failed to decode value %d", i)
		}
		values = append(values, value)
		offset = endIdx
	}

	return values, nil
}