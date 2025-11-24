package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/guileen/pglitedb/internal/table"
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

func (c *memcodec) DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []table.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
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
		case table.ColumnTypeString, table.ColumnTypeText, table.ColumnTypeUUID, table.ColumnTypeBinary:
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
		case table.ColumnTypeNumber, table.ColumnTypeTimestamp, table.ColumnTypeDate:
			// Fixed size: flag byte + 8 bytes for numeric values
			valueEnd = offset + 9
		case table.ColumnTypeBoolean:
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

	// The remaining bytes should be the rowID
	if offset+8 <= len(key) {
		rowID, _ = readMemComparableInt64(key[offset:])
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

func (c *memcodec) EncodeRow(row *table.Record, schemaDef *table.TableDefinition) ([]byte, error) {
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
		var colType table.ColumnType
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

func (c *memcodec) DecodeRow(data []byte, schemaDef *table.TableDefinition) (*table.Record, error) {
	encoded, err := decodeEncodedRow(data)
	if err != nil {
		return nil, err
	}

	record := &table.Record{
		ID:        "",
		Table:     schemaDef.Name,
		Data:      make(map[string]*table.Value),
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
			record.Data[col.Name] = &table.Value{
				Data: value,
				Type: col.Type,
			}
		}
	}

	return record, nil
}

func (c *memcodec) EncodeValue(value interface{}, colType table.ColumnType) ([]byte, error) {
	if value == nil {
		return []byte{nilFlag}, nil
	}

	switch colType {
	case table.ColumnTypeString, table.ColumnTypeText:
		return encodeString(value)
	case table.ColumnTypeNumber:
		return encodeNumber(value)
	case table.ColumnTypeBoolean:
		return encodeBoolean(value)
	case table.ColumnTypeTimestamp, table.ColumnTypeDate:
		return encodeTimestamp(value)
	case table.ColumnTypeJSON:
		return encodeJSON(value)
	case table.ColumnTypeUUID:
		return encodeUUID(value)
	case table.ColumnTypeBinary:
		return encodeBytes(value)
	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (c *memcodec) DecodeValue(data []byte, colType table.ColumnType) (interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if data[0] == nilFlag {
		return nil, nil
	}

	switch colType {
	case table.ColumnTypeString, table.ColumnTypeText:
		return decodeString(data)
	case table.ColumnTypeNumber:
		return decodeNumber(data)
	case table.ColumnTypeBoolean:
		return decodeBoolean(data)
	case table.ColumnTypeTimestamp, table.ColumnTypeDate:
		return decodeTimestamp(data)
	case table.ColumnTypeJSON:
		return decodeJSON(data)
	case table.ColumnTypeUUID:
		return decodeUUID(data)
	case table.ColumnTypeBinary:
		return decodeBytes(data)
	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (c *memcodec) EncodeCompositeKey(values []interface{}, types []table.ColumnType) ([]byte, error) {
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

func (c *memcodec) DecodeCompositeKey(data []byte, types []table.ColumnType) ([]interface{}, error) {
	values := make([]interface{}, 0, len(types))
	offset := 0

	for i := 0; i < len(types) && offset < len(data); i++ {
		colType := types[i]
		var endIdx int

		flag := data[offset]
		if flag == nilFlag {
			values = append(values, nil)
			offset += 1
			continue
		}

		switch colType {
		case table.ColumnTypeString, table.ColumnTypeText, table.ColumnTypeUUID, table.ColumnTypeBinary:
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
		case table.ColumnTypeNumber, table.ColumnTypeTimestamp, table.ColumnTypeDate:
			endIdx = offset + 9
		case table.ColumnTypeBoolean:
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

func writeMemComparableInt64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	u := uint64(v)
	u ^= 0x8000000000000000
	binary.BigEndian.PutUint64(tmp[:], u)
	buf.Write(tmp[:])
}

func readMemComparableInt64(data []byte) (int64, int) {
	if len(data) < 8 {
		return 0, 0
	}
	u := binary.BigEndian.Uint64(data[:8])
	u ^= 0x8000000000000000
	return int64(u), 8
}

func writeMemComparableUint64(buf *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	buf.Write(tmp[:])
}

func writeMemComparableFloat64(buf *bytes.Buffer, f float64) {
	u := math.Float64bits(f)
	if f >= 0 {
		u |= 0x8000000000000000
	} else {
		u = ^u
	}
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], u)
	buf.Write(tmp[:])
}

func writeMemComparableString(buf *bytes.Buffer, s string) {
	writeMemComparableBytes(buf, []byte(s))
}

func writeMemComparableBytes(buf *bytes.Buffer, b []byte) {
	for _, ch := range b {
		buf.WriteByte(ch)
		if ch == 0x00 {
			buf.WriteByte(0xFF)
		}
	}
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)
}

func encodeString(value interface{}) ([]byte, error) {
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}
	buf := &bytes.Buffer{}
	buf.WriteByte(bytesFlag)
	writeMemComparableString(buf, s)
	return buf.Bytes(), nil
}

func decodeString(data []byte) (interface{}, error) {
	if len(data) < 1 || data[0] != bytesFlag {
		return nil, fmt.Errorf("invalid string encoding")
	}
	result := &bytes.Buffer{}
	i := 1
	for i < len(data) {
		if data[i] == 0x00 {
			if i+1 < len(data) && data[i+1] == 0xFF {
				result.WriteByte(0x00)
				i += 2
			} else if i+1 < len(data) && data[i+1] == 0x00 {
				break
			} else {
				i++
			}
		} else {
			result.WriteByte(data[i])
			i++
		}
	}
	return result.String(), nil
}

func encodeNumber(value interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		buf.WriteByte(intFlag)
		writeMemComparableInt64(buf, toInt64(v))
	case uint, uint8, uint16, uint32, uint64:
		buf.WriteByte(uintFlag)
		writeMemComparableUint64(buf, toUint64(v))
	case float32, float64:
		buf.WriteByte(floatFlag)
		writeMemComparableFloat64(buf, toFloat64(v))
	default:
		return nil, fmt.Errorf("unsupported number type: %T", value)
	}
	return buf.Bytes(), nil
}

func decodeNumber(data []byte) (interface{}, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("invalid number encoding")
	}
	flag := data[0]
	switch flag {
	case intFlag:
		v, _ := readMemComparableInt64(data[1:])
		return v, nil
	case uintFlag:
		return binary.BigEndian.Uint64(data[1:9]), nil
	case floatFlag:
		u := binary.BigEndian.Uint64(data[1:9])
		if u&0x8000000000000000 != 0 {
			u &^= 0x8000000000000000
		} else {
			u = ^u
		}
		return math.Float64frombits(u), nil
	default:
		return nil, fmt.Errorf("invalid number flag: %d", flag)
	}
}

func encodeBoolean(value interface{}) ([]byte, error) {
	b, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("expected bool, got %T", value)
	}
	if b {
		return []byte{0x01}, nil
	}
	return []byte{0x00}, nil
}

func decodeBoolean(data []byte) (interface{}, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("invalid boolean encoding")
	}
	return data[0] != 0x00, nil
}

func encodeTimestamp(value interface{}) ([]byte, error) {
	var t time.Time
	switch v := value.(type) {
	case time.Time:
		t = v
	case int64:
		t = time.Unix(v, 0)
	default:
		return nil, fmt.Errorf("expected time.Time or int64, got %T", value)
	}
	buf := &bytes.Buffer{}
	buf.WriteByte(intFlag)
	writeMemComparableInt64(buf, t.UnixNano())
	return buf.Bytes(), nil
}

func decodeTimestamp(data []byte) (interface{}, error) {
	if len(data) < 9 || data[0] != intFlag {
		return nil, fmt.Errorf("invalid timestamp encoding")
	}
	nanos, _ := readMemComparableInt64(data[1:])
	return time.Unix(0, nanos), nil
}

func encodeJSON(value interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(jsonFlag)

	// Convert the value to JSON bytes
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Use the existing string encoding for JSON bytes
	writeMemComparableBytes(buf, jsonBytes)
	return buf.Bytes(), nil
}

func decodeJSON(data []byte) (interface{}, error) {
	if len(data) < 1 || data[0] != jsonFlag {
		return nil, fmt.Errorf("invalid JSON encoding")
	}

	// Extract the JSON bytes using the existing string decoding logic
	result := &bytes.Buffer{}
	i := 1
	for i < len(data) {
		if data[i] == 0x00 {
			if i+1 < len(data) && data[i+1] == 0xFF {
				result.WriteByte(0x00)
				i += 2
			} else if i+1 < len(data) && data[i+1] == 0x00 {
				break
			} else {
				i++
			}
		} else {
			result.WriteByte(data[i])
			i++
		}
	}

	// Unmarshal the JSON bytes back to interface{}
	var value interface{}
	if err := json.Unmarshal(result.Bytes(), &value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return value, nil
}

func encodeBytes(value interface{}) ([]byte, error) {
	b, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte, got %T", value)
	}
	buf := &bytes.Buffer{}
	buf.WriteByte(bytesFlag)
	writeMemComparableBytes(buf, b)
	return buf.Bytes(), nil
}

func decodeBytes(data []byte) (interface{}, error) {
	if len(data) < 1 || data[0] != bytesFlag {
		return nil, fmt.Errorf("invalid bytes encoding")
	}
	result := &bytes.Buffer{}
	i := 1
	for i < len(data) {
		if data[i] == 0x00 {
			if i+1 < len(data) && data[i+1] == 0xFF {
				result.WriteByte(0x00)
				i += 2
			} else if i+1 < len(data) && data[i+1] == 0x00 {
				break
			} else {
				i++
			}
		} else {
			result.WriteByte(data[i])
			i++
		}
	}
	return result.Bytes(), nil
}

func encodeUUID(value interface{}) ([]byte, error) {
	var u uuid.UUID
	var err error

	switch v := value.(type) {
	case uuid.UUID:
		u = v
	case string:
		u, err = uuid.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID string: %w", err)
		}
	case []byte:
		if len(v) != 16 {
			return nil, fmt.Errorf("invalid UUID byte slice length: %d", len(v))
		}
		copy(u[:], v)
	default:
		return nil, fmt.Errorf("expected uuid.UUID, string, or []byte, got %T", value)
	}

	buf := &bytes.Buffer{}
	buf.WriteByte(bytesFlag) // UUID is encoded as bytes
	writeMemComparableBytes(buf, u[:])
	return buf.Bytes(), nil
}

func decodeUUID(data []byte) (interface{}, error) {
	if len(data) < 1 || data[0] != bytesFlag {
		return nil, fmt.Errorf("invalid UUID encoding")
	}

	result := &bytes.Buffer{}
	i := 1
	for i < len(data) {
		if data[i] == 0x00 {
			if i+1 < len(data) && data[i+1] == 0xFF {
				result.WriteByte(0x00)
				i += 2
			} else if i+1 < len(data) && data[i+1] == 0x00 {
				break
			} else {
				i++
			}
		} else {
			result.WriteByte(data[i])
			i++
		}
	}

	// Convert bytes to UUID
	if len(result.Bytes()) != 16 {
		return nil, fmt.Errorf("invalid UUID byte length: %d", len(result.Bytes()))
	}

	var u uuid.UUID
	copy(u[:], result.Bytes())
	return u, nil
}

func encodeEncodedRow(row *EncodedRow) ([]byte, error) {
	buf := &bytes.Buffer{}

	var tmp [8]byte
	binary.BigEndian.PutUint32(tmp[:4], row.SchemaVersion)
	buf.Write(tmp[:4])

	writeMemComparableInt64(buf, row.CreatedAt)
	writeMemComparableInt64(buf, row.UpdatedAt)

	binary.BigEndian.PutUint32(tmp[:4], uint32(row.Version))
	buf.Write(tmp[:4])

	// Write transaction metadata
	binary.BigEndian.PutUint64(tmp[:], row.TxnID)
	buf.Write(tmp[:])

	binary.BigEndian.PutUint64(tmp[:], uint64(row.TxnTimestamp))
	buf.Write(tmp[:])

	binary.BigEndian.PutUint32(tmp[:4], uint32(len(row.Columns)))
	buf.Write(tmp[:4])

	for name, value := range row.Columns {
		nameBytes := []byte(name)
		binary.BigEndian.PutUint32(tmp[:4], uint32(len(nameBytes)))
		buf.Write(tmp[:4])
		buf.Write(nameBytes)

		binary.BigEndian.PutUint32(tmp[:4], uint32(len(value)))
		buf.Write(tmp[:4])
		buf.Write(value)
	}

	return buf.Bytes(), nil
}

func decodeEncodedRow(data []byte) (*EncodedRow, error) {
	if len(data) < 44 { // Increased minimum size to account for TxnID and TxnTimestamp
		return nil, fmt.Errorf("data too short")
	}

	row := &EncodedRow{
		Columns: make(map[string][]byte),
	}

	offset := 0
	row.SchemaVersion = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	row.CreatedAt, _ = readMemComparableInt64(data[offset:])
	offset += 8

	row.UpdatedAt, _ = readMemComparableInt64(data[offset:])
	offset += 8

	row.Version = int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Read transaction metadata
	row.TxnID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	row.TxnTimestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	numColumns := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	for i := uint32(0); i < numColumns; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("unexpected end of data")
		}

		nameLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(nameLen) > len(data) {
			return nil, fmt.Errorf("unexpected end of data")
		}
		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		if offset+4 > len(data) {
			return nil, fmt.Errorf("unexpected end of data")
		}
		valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(valueLen) > len(data) {
			return nil, fmt.Errorf("unexpected end of data")
		}
		value := make([]byte, valueLen)
		copy(value, data[offset:offset+int(valueLen)])
		offset += int(valueLen)

		row.Columns[name] = value
	}

	return row, nil
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	default:
		return 0
	}
}

func toUint64(v interface{}) uint64 {
	switch val := v.(type) {
	case uint:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint64:
		return val
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return 0
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
