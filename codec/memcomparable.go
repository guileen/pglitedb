package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/guileen/pglitedb/types"
)

const (
	nilFlag      byte = 0xFF
	bytesFlag    byte = 0x00
	compactFlag  byte = 0x01
	intFlag      byte = 0x02
	uintFlag     byte = 0x03
	floatFlag    byte = 0x04
	decimalFlag  byte = 0x05
	durationFlag byte = 0x06
	varintFlag   byte = 0x07
	uvarintFlag  byte = 0x08
	jsonFlag     byte = 0x09
	maxFlag      byte = 0xFA
)

type memcodec struct{}

func NewMemComparableCodec() Codec {
	return &memcodec{}
}

// =============================================================================
// ENCODING METHODS
// =============================================================================

func (c *memcodec) EncodeTableKey(tenantID, tableID, rowID int64) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeTable))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteByte(byte(KeyTypeRow))
	writeMemComparableInt64(buf, tableID)
	writeMemComparableInt64(buf, rowID)
	return buf.Bytes()
}

func (c *memcodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeTable))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteByte(byte(KeyTypeIndex))
	writeMemComparableInt64(buf, tableID)
	writeMemComparableInt64(buf, indexID)

	valueBytes, err := c.encodeMemComparableValue(indexValue)
	if err != nil {
		return nil, fmt.Errorf("encode index value: %w", err)
	}
	buf.Write(valueBytes)
	writeMemComparableInt64(buf, rowID)

	return buf.Bytes(), nil
}

func (c *memcodec) EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error) {
	if len(indexValues) == 0 {
		return nil, fmt.Errorf("index values cannot be empty")
	}

	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeTable))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteByte(byte(KeyTypeIndex))
	writeMemComparableInt64(buf, tableID)
	writeMemComparableInt64(buf, indexID)

	// Encode each index value
	for _, value := range indexValues {
		valueBytes, err := c.encodeMemComparableValue(value)
		if err != nil {
			return nil, fmt.Errorf("encode index value: %w", err)
		}
		buf.Write(valueBytes)
	}

	writeMemComparableInt64(buf, rowID)
	return buf.Bytes(), nil
}

// =============================================================================
// DECODING METHODS
// =============================================================================

func (c *memcodec) DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []types.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		err = fmt.Errorf("invalid table key prefix")
		return
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypeIndex {
		err = fmt.Errorf("invalid index key marker")
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
			err = fmt.Errorf("unexpected end of key at index value %d", i)
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
				err = fmt.Errorf("invalid string/bytes encoding at index value %d", i)
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
			err = fmt.Errorf("index key too short for value %d", i)
			return
		}

		value, decodeErr := c.DecodeValue(key[offset:valueEnd], colType)
		if decodeErr != nil {
			err = fmt.Errorf("decode index value %d: %w", i, decodeErr)
			return
		}

		indexValues = append(indexValues, value)
		offset = valueEnd
	}

	// The remaining bytes should be the rowID (last 8 bytes)
	if len(key)-offset >= 8 {
		rowID, _ = readMemComparableInt64(key[len(key)-8:])
	} else {
		err = fmt.Errorf("missing rowID in index key")
		return
	}

	return tenantID, tableID, indexID, indexValues, rowID, nil
}

func (c *memcodec) EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeTable))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteByte(byte(KeyTypePK))
	writeMemComparableInt64(buf, tableID)

	valueBytes, err := c.encodeMemComparableValue(pkValue)
	if err != nil {
		return nil, fmt.Errorf("encode pk value: %w", err)
	}
	buf.Write(valueBytes)

	return buf.Bytes(), nil
}

func (c *memcodec) EncodeMetaKey(tenantID int64, metaType string, key string) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeMeta))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteString(metaType)
	buf.WriteByte(':')
	buf.WriteString(key)
	return buf.Bytes()
}

func (c *memcodec) EncodeSequenceKey(tenantID int64, seqName string) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeSequence))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteString(seqName)
	return buf.Bytes()
}

func (c *memcodec) DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		return 0, 0, 0, fmt.Errorf("invalid table key prefix")
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypeRow {
		return 0, 0, 0, fmt.Errorf("invalid row key marker")
	}
	offset++

	tableID, n = readMemComparableInt64(key[offset:])
	offset += n

	rowID, _ = readMemComparableInt64(key[offset:])

	return tenantID, tableID, rowID, nil
}

func (c *memcodec) DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		err = fmt.Errorf("invalid table key prefix")
		return
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypeIndex {
		err = fmt.Errorf("invalid index key marker")
		return
	}
	offset++

	tableID, n = readMemComparableInt64(key[offset:])
	offset += n

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

func (c *memcodec) DecodePKKey(key []byte) (tenantID, tableID int64, err error) {
	if len(key) < 1 || KeyType(key[0]) != KeyTypeTable {
		return 0, 0, fmt.Errorf("invalid table key prefix")
	}

	offset := 1
	tenantID, n := readMemComparableInt64(key[offset:])
	offset += n

	if offset >= len(key) || KeyType(key[offset]) != KeyTypePK {
		return 0, 0, fmt.Errorf("invalid pk key marker")
	}
	offset++

	tableID, _ = readMemComparableInt64(key[offset:])

	return tenantID, tableID, nil
}

func (c *memcodec) EncodeRow(row *types.Record, schemaDef *types.TableDefinition) ([]byte, error) {
	encoded := &EncodedRow{
		SchemaVersion: uint32(schemaDef.Version),
		Columns:       make(map[string][]byte),
		CreatedAt:     row.CreatedAt.Unix(),
		UpdatedAt:     row.UpdatedAt.Unix(),
		Version:       row.Version,
		// Transaction metadata will be set by the transaction implementation
		TxnID:        0,
		TxnTimestamp: 0,
	}

	for colName, value := range row.Data {
		var colType types.ColumnType
		for _, col := range schemaDef.Columns {
			if col.Name == colName {
				colType = col.Type
				break
			}
		}

		if colType == "" {
			return nil, fmt.Errorf("column %s not found in schema", colName)
		}

		valueBytes, err := c.EncodeValue(value.Data, colType)
		if err != nil {
			return nil, fmt.Errorf("encode column %s: %w", colName, err)
		}
		encoded.Columns[colName] = valueBytes
	}

	return encodeEncodedRow(encoded)
}

func (c *memcodec) DecodeRow(data []byte, schemaDef *types.TableDefinition) (*types.Record, error) {
	encoded, err := decodeEncodedRow(data)
	if err != nil {
		return nil, err
	}

	record := &types.Record{
		ID:        "",
		Table:     schemaDef.Name,
		Data:      make(map[string]*types.Value),
		CreatedAt: time.Unix(encoded.CreatedAt, 0),
		UpdatedAt: time.Unix(encoded.UpdatedAt, 0),
		Version:   encoded.Version,
	}

	for _, col := range schemaDef.Columns {
		if valueBytes, ok := encoded.Columns[col.Name]; ok {
			value, err := c.DecodeValue(valueBytes, col.Type)
			if err != nil {
				return nil, fmt.Errorf("decode column %s: %w", col.Name, err)
			}
			record.Data[col.Name] = &types.Value{
				Data: value,
				Type: col.Type,
			}
		}
	}

	return record, nil
}

func (c *memcodec) EncodeValue(value interface{}, colType types.ColumnType) ([]byte, error) {
	if value == nil {
		return []byte{nilFlag}, nil
	}

	switch colType {
	case types.ColumnTypeString, types.ColumnTypeText:
		return encodeString(value)
	case types.ColumnTypeNumber:
		return encodeNumber(value)
	case types.ColumnTypeBoolean:
		return encodeBoolean(value)
	case types.ColumnTypeTimestamp, types.ColumnTypeDate:
		return encodeTimestamp(value)
	case types.ColumnTypeJSON:
		return encodeJSON(value)
	case types.ColumnTypeUUID:
		return encodeUUID(value)
	case types.ColumnTypeBinary:
		return encodeBytes(value)
	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (c *memcodec) DecodeValue(data []byte, colType types.ColumnType) (interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if data[0] == nilFlag {
		return nil, nil
	}

	switch colType {
	case types.ColumnTypeString, types.ColumnTypeText:
		return decodeString(data)
	case types.ColumnTypeNumber:
		return decodeNumber(data)
	case types.ColumnTypeBoolean:
		return decodeBoolean(data)
	case types.ColumnTypeTimestamp, types.ColumnTypeDate:
		return decodeTimestamp(data)
	case types.ColumnTypeJSON:
		return decodeJSON(data)
	case types.ColumnTypeUUID:
		return decodeUUID(data)
	case types.ColumnTypeBinary:
		return decodeBytes(data)
	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (c *memcodec) EncodeCompositeKey(values []interface{}, types []types.ColumnType) ([]byte, error) {
	if len(values) != len(types) {
		return nil, fmt.Errorf("values and types length mismatch")
	}

	buf := &bytes.Buffer{}
	for i, value := range values {
		valueBytes, err := c.EncodeValue(value, types[i])
		if err != nil {
			return nil, fmt.Errorf("encode value %d: %w", i, err)
		}
		buf.Write(valueBytes)
	}

	return buf.Bytes(), nil
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
		case types.ColumnTypeNumber, types.ColumnTypeTimestamp, types.ColumnTypeDate:
			endIdx = offset + 9
		case types.ColumnTypeBoolean:
			endIdx = offset + 1
		default:
			return nil, fmt.Errorf("unsupported column type: %s", colType)
		}

		if endIdx > len(data) {
			return nil, fmt.Errorf("unexpected end of data at value %d", i)
		}

		value, err := c.DecodeValue(data[offset:endIdx], colType)
		if err != nil {
			return nil, fmt.Errorf("decode value %d: %w", i, err)
		}
		values = append(values, value)
		offset = endIdx
	}

	return values, nil
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
			return nil, fmt.Errorf("unsupported value type: %T", value)
		}
		buf := &bytes.Buffer{}
		buf.WriteByte(jsonFlag)
		writeMemComparableBytes(buf, jsonBytes)
		return buf.Bytes(), nil
	}
}



// EncodeIndexScanStartKey creates a start key for index scanning
func (c *memcodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeTable))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteByte(byte(KeyTypeIndex))
	writeMemComparableInt64(buf, tableID)
	writeMemComparableInt64(buf, indexID)
	return buf.Bytes()
}

// EncodeIndexScanEndKey creates an end key for index scanning
func (c *memcodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(KeyTypeTable))
	writeMemComparableInt64(buf, tenantID)
	buf.WriteByte(byte(KeyTypeIndex))
	writeMemComparableInt64(buf, tableID)
	writeMemComparableInt64(buf, indexID)
	
	// Add a max byte to ensure we scan all indexes for this table+index
	buf.WriteByte(maxFlag)
	return buf.Bytes()
}