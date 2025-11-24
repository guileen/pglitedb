package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func TestComplexFilterExpression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-filter-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatal(err)
	}

	c := codec.NewMemComparableCodec()
	eng := NewPebbleEngine(kvStore, c)
	defer eng.Close()

	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(1)

	schema := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber},
			{Name: "age", Type: types.ColumnTypeNumber},
			{Name: "city", Type: types.ColumnTypeString},
			{Name: "active", Type: types.ColumnTypeBoolean},
		},
		Indexes: []types.IndexDefinition{
			{Columns: []string{"age"}},
			{Columns: []string{"city", "age"}},
		},
	}

	// Insert test data
	testRows := []*types.Record{
		{Data: map[string]*types.Value{
			"id":     {Type: types.ColumnTypeNumber, Data: int64(1)},
			"age":    {Type: types.ColumnTypeNumber, Data: int64(25)},
			"city":   {Type: types.ColumnTypeString, Data: "Beijing"},
			"active": {Type: types.ColumnTypeBoolean, Data: true},
		}},
		{Data: map[string]*types.Value{
			"id":     {Type: types.ColumnTypeNumber, Data: int64(2)},
			"age":    {Type: types.ColumnTypeNumber, Data: int64(30)},
			"city":   {Type: types.ColumnTypeString, Data: "Shanghai"},
			"active": {Type: types.ColumnTypeBoolean, Data: true},
		}},
		{Data: map[string]*types.Value{
			"id":     {Type: types.ColumnTypeNumber, Data: int64(3)},
			"age":    {Type: types.ColumnTypeNumber, Data: int64(20)},
			"city":   {Type: types.ColumnTypeString, Data: "Beijing"},
			"active": {Type: types.ColumnTypeBoolean, Data: false},
		}},
		{Data: map[string]*types.Value{
			"id":     {Type: types.ColumnTypeNumber, Data: int64(4)},
			"age":    {Type: types.ColumnTypeNumber, Data: int64(35)},
			"city":   {Type: types.ColumnTypeString, Data: "Shenzhen"},
			"active": {Type: types.ColumnTypeBoolean, Data: true},
		}},
	}

	for _, row := range testRows {
		_, err := eng.InsertRow(ctx, tenantID, tableID, row, schema)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	t.Run("AND filter - age > 20 AND city = Beijing", func(t *testing.T) {
		filter := &FilterExpression{
			Type: "and",
			Children: []*FilterExpression{
				{Type: "simple", Column: "age", Operator: ">", Value: int64(20)},
				{Type: "simple", Column: "city", Operator: "=", Value: "Beijing"},
			},
		}

		opts := &ScanOptions{Filter: filter}
		// Use full table scan to test AND filter logic
		iter, err := eng.ScanRows(ctx, tenantID, tableID, schema, opts)
		if err != nil {
			t.Fatalf("ScanRows failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			row := iter.Row()
			age := row.Data["age"].Data.(int64)
			city := row.Data["city"].Data.(string)
			if age <= 20 || city != "Beijing" {
				t.Errorf("Filter failed: age=%d, city=%s", age, city)
			}
			count++
		}

		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	t.Run("OR filter - age < 22 OR age > 33", func(t *testing.T) {
		filter := &FilterExpression{
			Type: "or",
			Children: []*FilterExpression{
				{Type: "simple", Column: "age", Operator: "<", Value: int64(22)},
				{Type: "simple", Column: "age", Operator: ">", Value: int64(33)},
			},
		}

		opts := &ScanOptions{Filter: filter}
		iter, err := eng.ScanRows(ctx, tenantID, tableID, schema, opts)
		if err != nil {
			t.Fatalf("ScanRows failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			row := iter.Row()
			age := row.Data["age"].Data.(int64)
			if !(age < 22 || age > 33) {
				t.Errorf("Filter failed: age=%d", age)
			}
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})

	t.Run("NOT filter - NOT (active = true)", func(t *testing.T) {
		filter := &FilterExpression{
			Type: "not",
			Children: []*FilterExpression{
				{Type: "simple", Column: "active", Operator: "=", Value: true},
			},
		}

		opts := &ScanOptions{Filter: filter}
		iter, err := eng.ScanRows(ctx, tenantID, tableID, schema, opts)
		if err != nil {
			t.Fatalf("ScanRows failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			row := iter.Row()
			active := row.Data["active"].Data.(bool)
			if active {
				t.Errorf("Filter failed: active should be false")
			}
			count++
		}

		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	t.Run("IN operator - city IN (Beijing, Shanghai)", func(t *testing.T) {
		filter := &FilterExpression{
			Type:     "simple",
			Column:   "city",
			Operator: "IN",
			Values:   []interface{}{"Beijing", "Shanghai"},
		}

		opts := &ScanOptions{Filter: filter}
		iter, err := eng.ScanRows(ctx, tenantID, tableID, schema, opts)
		if err != nil {
			t.Fatalf("ScanRows failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			row := iter.Row()
			city := row.Data["city"].Data.(string)
			if city != "Beijing" && city != "Shanghai" {
				t.Errorf("Filter failed: city=%s", city)
			}
			count++
		}

		if count != 3 {
			t.Errorf("Expected 3 rows, got %d", count)
		}
	})

	t.Run("BETWEEN operator - age BETWEEN 25 AND 32", func(t *testing.T) {
		filter := &FilterExpression{
			Type:     "simple",
			Column:   "age",
			Operator: "BETWEEN",
			Values:   []interface{}{int64(25), int64(32)},
		}

		opts := &ScanOptions{Filter: filter}
		iter, err := eng.ScanRows(ctx, tenantID, tableID, schema, opts)
		if err != nil {
			t.Fatalf("ScanRows failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			row := iter.Row()
			age := row.Data["age"].Data.(int64)
			if age < 25 || age > 32 {
				t.Errorf("Filter failed: age=%d", age)
			}
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})
}
