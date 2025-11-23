package codec

import (
	"bytes"
	"testing"
	"time"

	"github.com/guileen/pglitedb/table"
)

func TestMemComparableInt64Encoding(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{
			name:   "ascending order",
			values: []int64{-1000, -100, -10, -1, 0, 1, 10, 100, 1000},
		},
		{
			name:   "min and max",
			values: []int64{-9223372036854775808, -1, 0, 1, 9223372036854775807},
		},
		{
			name:   "random values",
			values: []int64{42, -42, 12345, -12345, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := make([][]byte, len(tt.values))
			for i, v := range tt.values {
				buf := &bytes.Buffer{}
				writeMemComparableInt64(buf, v)
				encoded[i] = buf.Bytes()

				decoded, n := readMemComparableInt64(encoded[i])
				if decoded != v {
					t.Errorf("round trip failed: encoded %d, decoded %d", v, decoded)
				}
				if n != 8 {
					t.Errorf("expected to read 8 bytes, read %d", n)
				}
			}

			for i := 0; i < len(encoded)-1; i++ {
				cmp := bytes.Compare(encoded[i], encoded[i+1])
				if tt.values[i] < tt.values[i+1] && cmp >= 0 {
					t.Errorf("encoding not memcomparable: %d vs %d, bytes: %d",
						tt.values[i], tt.values[i+1], cmp)
				}
				if tt.values[i] > tt.values[i+1] && cmp <= 0 {
					t.Errorf("encoding not memcomparable: %d vs %d, bytes: %d",
						tt.values[i], tt.values[i+1], cmp)
				}
			}
		})
	}
}

func TestTableKeyEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	tests := []struct {
		name     string
		tenantID int64
		tableID  int64
		rowID    int64
	}{
		{"basic", 1, 1, 1},
		{"large ids", 999999, 888888, 777777},
		{"negative ids", -1, -2, -3},
		{"mixed", 1, -2, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := codec.EncodeTableKey(tt.tenantID, tt.tableID, tt.rowID)

			if len(key) == 0 {
				t.Fatal("encoded key is empty")
			}

			if KeyType(key[0]) != KeyTypeTable {
				t.Errorf("expected table key prefix, got %v", key[0])
			}

			tenantID, tableID, rowID, err := codec.DecodeTableKey(key)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if tenantID != tt.tenantID {
				t.Errorf("tenantID: expected %d, got %d", tt.tenantID, tenantID)
			}
			if tableID != tt.tableID {
				t.Errorf("tableID: expected %d, got %d", tt.tableID, tableID)
			}
			if rowID != tt.rowID {
				t.Errorf("rowID: expected %d, got %d", tt.rowID, rowID)
			}
		})
	}
}

func TestIndexKeyEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	tests := []struct {
		name       string
		tenantID   int64
		tableID    int64
		indexID    int64
		indexValue interface{}
		rowID      int64
	}{
		{"string index", 1, 1, 1, "test", 100},
		{"int index", 1, 1, 2, int64(42), 101},
		{"float index", 1, 1, 3, float64(3.14), 102},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := codec.EncodeIndexKey(tt.tenantID, tt.tableID, tt.indexID, tt.indexValue, tt.rowID)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			if len(key) == 0 {
				t.Fatal("encoded key is empty")
			}

			tenantID, tableID, indexID, _, _, err := codec.DecodeIndexKey(key)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if tenantID != tt.tenantID {
				t.Errorf("tenantID: expected %d, got %d", tt.tenantID, tenantID)
			}
			if tableID != tt.tableID {
				t.Errorf("tableID: expected %d, got %d", tt.tableID, tableID)
			}
			if indexID != tt.indexID {
				t.Errorf("indexID: expected %d, got %d", tt.indexID, indexID)
			}
		})
	}
}

func TestIndexKeyOrdering(t *testing.T) {
	codec := NewMemComparableCodec()

	tenantID := int64(1)
	tableID := int64(1)
	indexID := int64(1)

	stringKeys := []struct {
		value string
		rowID int64
	}{
		{"alice", 1},
		{"bob", 2},
		{"charlie", 3},
		{"david", 4},
	}

	encodedKeys := make([][]byte, len(stringKeys))
	for i, sk := range stringKeys {
		key, err := codec.EncodeIndexKey(tenantID, tableID, indexID, sk.value, sk.rowID)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		encodedKeys[i] = key
	}

	for i := 0; i < len(encodedKeys)-1; i++ {
		if bytes.Compare(encodedKeys[i], encodedKeys[i+1]) >= 0 {
			t.Errorf("keys not in order: %s should be less than %s",
				stringKeys[i].value, stringKeys[i+1].value)
		}
	}
}

func TestCompositeIndexKeyEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	tests := []struct {
		name        string
		tenantID    int64
		tableID     int64
		indexID     int64
		indexValues []interface{}
		rowID       int64
	}{
		{"string and int", 1, 1, 1, []interface{}{"test", int64(42)}, 100},
		{"multiple strings", 1, 1, 2, []interface{}{"alice", "bob", "charlie"}, 101},
		{"mixed types", 1, 1, 3, []interface{}{"test", int64(42), true, float64(3.14)}, 102},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := codec.EncodeCompositeIndexKey(tt.tenantID, tt.tableID, tt.indexID, tt.indexValues, tt.rowID)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			if len(key) == 0 {
				t.Fatal("encoded key is empty")
			}

			// Basic decode test
			tenantID, tableID, indexID, _, _, err := codec.DecodeIndexKey(key)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if tenantID != tt.tenantID {
				t.Errorf("tenantID: expected %d, got %d", tt.tenantID, tenantID)
			}
			if tableID != tt.tableID {
				t.Errorf("tableID: expected %d, got %d", tt.tableID, tableID)
			}
			if indexID != tt.indexID {
				t.Errorf("indexID: expected %d, got %d", tt.indexID, indexID)
			}
		})
	}
}

func TestCompositeIndexKeyOrdering(t *testing.T) {
	codec := NewMemComparableCodec()

	tenantID := int64(1)
	tableID := int64(1)
	indexID := int64(1)

	// Test composite key ordering
	compositeKeys := []struct {
		values []interface{}
		rowID  int64
	}{
		{[]interface{}{"alice", int64(25)}, 1},
		{[]interface{}{"alice", int64(30)}, 2},
		{[]interface{}{"bob", int64(25)}, 3},
		{[]interface{}{"bob", int64(30)}, 4},
	}

	encodedKeys := make([][]byte, len(compositeKeys))
	for i, ck := range compositeKeys {
		key, err := codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, ck.values, ck.rowID)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		encodedKeys[i] = key
	}

	for i := 0; i < len(encodedKeys)-1; i++ {
		if bytes.Compare(encodedKeys[i], encodedKeys[i+1]) >= 0 {
			t.Errorf("composite keys not in order: %+v should be less than %+v",
				compositeKeys[i].values, compositeKeys[i+1].values)
		}
	}
}

func TestValueEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	tests := []struct {
		name    string
		value   interface{}
		colType table.ColumnType
	}{
		{"nil", nil, table.ColumnTypeString},
		{"string", "hello world", table.ColumnTypeString},
		{"empty string", "", table.ColumnTypeString},
		{"int", int64(42), table.ColumnTypeNumber},
		{"negative int", int64(-42), table.ColumnTypeNumber},
		{"zero", int64(0), table.ColumnTypeNumber},
		{"float", float64(3.14), table.ColumnTypeNumber},
		{"bool true", true, table.ColumnTypeBoolean},
		{"bool false", false, table.ColumnTypeBoolean},
		{"timestamp", time.Now(), table.ColumnTypeTimestamp},
		{"bytes", []byte{1, 2, 3, 4}, table.ColumnTypeBinary},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := codec.EncodeValue(tt.value, tt.colType)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			decoded, err := codec.DecodeValue(encoded, tt.colType)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if tt.value == nil {
				if decoded != nil {
					t.Errorf("expected nil, got %v", decoded)
				}
				return
			}

			switch tt.colType {
			case table.ColumnTypeString, table.ColumnTypeText:
				if decoded.(string) != tt.value.(string) {
					t.Errorf("expected %v, got %v", tt.value, decoded)
				}
			case table.ColumnTypeNumber:
				switch v := tt.value.(type) {
				case int64:
					if decoded.(int64) != v {
						t.Errorf("expected %v, got %v", tt.value, decoded)
					}
				case float64:
					if decoded.(float64) != v {
						t.Errorf("expected %v, got %v", tt.value, decoded)
					}
				}
			case table.ColumnTypeBoolean:
				if decoded.(bool) != tt.value.(bool) {
					t.Errorf("expected %v, got %v", tt.value, decoded)
				}
			case table.ColumnTypeTimestamp:
				origTime := tt.value.(time.Time)
				decodedTime := decoded.(time.Time)
				if !origTime.Equal(decodedTime) {
					t.Errorf("expected %v, got %v", origTime, decodedTime)
				}
			case table.ColumnTypeBinary:
				if !bytes.Equal(decoded.([]byte), tt.value.([]byte)) {
					t.Errorf("expected %v, got %v", tt.value, decoded)
				}
			}
		})
	}
}

func TestStringValueOrdering(t *testing.T) {
	codec := NewMemComparableCodec()

	values := []string{"", "a", "aa", "ab", "b", "ba", "bb", "z"}

	encodedValues := make([][]byte, len(values))
	for i, v := range values {
		encoded, err := codec.EncodeValue(v, table.ColumnTypeString)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		encodedValues[i] = encoded
	}

	for i := 0; i < len(encodedValues)-1; i++ {
		cmp := bytes.Compare(encodedValues[i], encodedValues[i+1])
		if cmp >= 0 {
			t.Errorf("string values not properly ordered: %q should be < %q, but bytes compare = %d",
				values[i], values[i+1], cmp)
		}
	}
}

func TestNumberValueOrdering(t *testing.T) {
	codec := NewMemComparableCodec()

	intValues := []int64{-1000, -100, -10, -1, 0, 1, 10, 100, 1000}

	encodedInts := make([][]byte, len(intValues))
	for i, v := range intValues {
		encoded, err := codec.EncodeValue(v, table.ColumnTypeNumber)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		encodedInts[i] = encoded
	}

	for i := 0; i < len(encodedInts)-1; i++ {
		cmp := bytes.Compare(encodedInts[i], encodedInts[i+1])
		if cmp >= 0 {
			t.Errorf("int values not properly ordered: %d should be < %d, but bytes compare = %d",
				intValues[i], intValues[i+1], cmp)
		}
	}

	floatValues := []float64{-100.5, -10.5, -1.5, -0.5, 0, 0.5, 1.5, 10.5, 100.5}

	encodedFloats := make([][]byte, len(floatValues))
	for i, v := range floatValues {
		encoded, err := codec.EncodeValue(v, table.ColumnTypeNumber)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		encodedFloats[i] = encoded
	}

	for i := 0; i < len(encodedFloats)-1; i++ {
		cmp := bytes.Compare(encodedFloats[i], encodedFloats[i+1])
		if cmp >= 0 {
			t.Errorf("float values not properly ordered: %f should be < %f, but bytes compare = %d",
				floatValues[i], floatValues[i+1], cmp)
		}
	}
}

func TestRowEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	schemaDef := &table.TableDefinition{
		Name:    "users",
		Version: 1,
		Columns: []table.ColumnDefinition{
			{Name: "id", Type: table.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: table.ColumnTypeString},
			{Name: "email", Type: table.ColumnTypeString},
			{Name: "age", Type: table.ColumnTypeNumber},
			{Name: "active", Type: table.ColumnTypeBoolean},
		},
	}

	now := time.Now()
	record := &table.Record{
		ID:    "1",
		Table: "users",
		Data: map[string]*table.Value{
			"id":     {Data: int64(1), Type: table.ColumnTypeNumber},
			"name":   {Data: "Alice", Type: table.ColumnTypeString},
			"email":  {Data: "alice@example.com", Type: table.ColumnTypeString},
			"age":    {Data: int64(30), Type: table.ColumnTypeNumber},
			"active": {Data: true, Type: table.ColumnTypeBoolean},
		},
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}

	encoded, err := codec.EncodeRow(record, schemaDef)
	if err != nil {
		t.Fatalf("encode row failed: %v", err)
	}

	if len(encoded) == 0 {
		t.Fatal("encoded row is empty")
	}

	decoded, err := codec.DecodeRow(encoded, schemaDef)
	if err != nil {
		t.Fatalf("decode row failed: %v", err)
	}

	if decoded.Table != record.Table {
		t.Errorf("table: expected %s, got %s", record.Table, decoded.Table)
	}

	if decoded.Version != record.Version {
		t.Errorf("version: expected %d, got %d", record.Version, decoded.Version)
	}

	if decoded.CreatedAt.Unix() != record.CreatedAt.Unix() {
		t.Errorf("createdAt: expected %v, got %v", record.CreatedAt.Unix(), decoded.CreatedAt.Unix())
	}

	for colName, origValue := range record.Data {
		decodedValue, ok := decoded.Data[colName]
		if !ok {
			t.Errorf("column %s missing in decoded row", colName)
			continue
		}

		if decodedValue.Type != origValue.Type {
			t.Errorf("column %s type: expected %s, got %s",
				colName, origValue.Type, decodedValue.Type)
		}

		switch origValue.Type {
		case table.ColumnTypeString:
			if decodedValue.Data.(string) != origValue.Data.(string) {
				t.Errorf("column %s: expected %v, got %v",
					colName, origValue.Data, decodedValue.Data)
			}
		case table.ColumnTypeNumber:
			if decodedValue.Data.(int64) != origValue.Data.(int64) {
				t.Errorf("column %s: expected %v, got %v",
					colName, origValue.Data, decodedValue.Data)
			}
		case table.ColumnTypeBoolean:
			if decodedValue.Data.(bool) != origValue.Data.(bool) {
				t.Errorf("column %s: expected %v, got %v",
					colName, origValue.Data, decodedValue.Data)
			}
		}
	}
}

func TestCompositeKeyEncoding(t *testing.T) {
	codec := NewMemComparableCodec()

	values := []interface{}{"alice", int64(30), true}
	types := []table.ColumnType{
		table.ColumnTypeString,
		table.ColumnTypeNumber,
		table.ColumnTypeBoolean,
	}

	encoded, err := codec.EncodeCompositeKey(values, types)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := codec.DecodeCompositeKey(encoded, types)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("expected %d values, got %d", len(values), len(decoded))
	}

	if decoded[0].(string) != values[0].(string) {
		t.Errorf("value 0: expected %v, got %v", values[0], decoded[0])
	}
	if decoded[1].(int64) != values[1].(int64) {
		t.Errorf("value 1: expected %v, got %v", values[1], decoded[1])
	}
	if decoded[2].(bool) != values[2].(bool) {
		t.Errorf("value 2: expected %v, got %v", values[2], decoded[2])
	}
}

func TestCompositeKeyOrdering(t *testing.T) {
	codec := NewMemComparableCodec()

	keys := []struct {
		name   string
		age    int64
		active bool
	}{
		{"alice", 25, false},
		{"alice", 25, true},
		{"alice", 30, false},
		{"bob", 20, false},
		{"bob", 30, true},
	}

	types := []table.ColumnType{
		table.ColumnTypeString,
		table.ColumnTypeNumber,
		table.ColumnTypeBoolean,
	}

	encodedKeys := make([][]byte, len(keys))
	for i, k := range keys {
		values := []interface{}{k.name, k.age, k.active}
		encoded, err := codec.EncodeCompositeKey(values, types)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		encodedKeys[i] = encoded
	}

	for i := 0; i < len(encodedKeys)-1; i++ {
		cmp := bytes.Compare(encodedKeys[i], encodedKeys[i+1])
		if cmp >= 0 {
			t.Errorf("composite keys not properly ordered at index %d: %+v should be < %+v",
				i, keys[i], keys[i+1])
		}
	}
}

func BenchmarkEncodeTableKey(b *testing.B) {
	codec := NewMemComparableCodec()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.EncodeTableKey(1, 1, int64(i))
	}
}

func BenchmarkDecodeTableKey(b *testing.B) {
	codec := NewMemComparableCodec()
	key := codec.EncodeTableKey(1, 1, 12345)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.DecodeTableKey(key)
	}
}

func BenchmarkEncodeValue(b *testing.B) {
	codec := NewMemComparableCodec()
	value := "benchmark test string"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.EncodeValue(value, table.ColumnTypeString)
	}
}

func BenchmarkEncodeRow(b *testing.B) {
	codec := NewMemComparableCodec()

	schemaDef := &table.TableDefinition{
		Name:    "users",
		Version: 1,
		Columns: []table.ColumnDefinition{
			{Name: "id", Type: table.ColumnTypeNumber},
			{Name: "name", Type: table.ColumnTypeString},
			{Name: "email", Type: table.ColumnTypeString},
			{Name: "age", Type: table.ColumnTypeNumber},
			{Name: "active", Type: table.ColumnTypeBoolean},
		},
	}

	now := time.Now()
	record := &table.Record{
		ID:    "1",
		Table: "users",
		Data: map[string]*table.Value{
			"id":     {Data: int64(1), Type: table.ColumnTypeNumber},
			"name":   {Data: "Alice", Type: table.ColumnTypeString},
			"email":  {Data: "alice@example.com", Type: table.ColumnTypeString},
			"age":    {Data: int64(30), Type: table.ColumnTypeNumber},
			"active": {Data: true, Type: table.ColumnTypeBoolean},
		},
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.EncodeRow(record, schemaDef)
	}
}
