package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

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