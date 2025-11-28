package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func TestIndexOnlyScan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-index-only-test-*")
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
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(1)

	schema := &types.TableDefinition{
		Name: "products",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber},
			{Name: "name", Type: types.ColumnTypeString},
			{Name: "price", Type: types.ColumnTypeNumber},
			{Name: "category", Type: types.ColumnTypeString},
			{Name: "stock", Type: types.ColumnTypeNumber},
		},
		Indexes: []types.IndexDefinition{
			{Columns: []string{"category", "price", "name"}}, // Covering index for some queries
			{Columns: []string{"price"}},
		},
	}

	// Insert test data
	testRows := []*types.Record{
		{Data: map[string]*types.Value{
			"id":       {Type: types.ColumnTypeNumber, Data: int64(1)},
			"name":     {Type: types.ColumnTypeString, Data: "Laptop"},
			"price":    {Type: types.ColumnTypeNumber, Data: int64(1200)},
			"category": {Type: types.ColumnTypeString, Data: "Electronics"},
			"stock":    {Type: types.ColumnTypeNumber, Data: int64(50)},
		}},
		{Data: map[string]*types.Value{
			"id":       {Type: types.ColumnTypeNumber, Data: int64(2)},
			"name":     {Type: types.ColumnTypeString, Data: "Mouse"},
			"price":    {Type: types.ColumnTypeNumber, Data: int64(25)},
			"category": {Type: types.ColumnTypeString, Data: "Electronics"},
			"stock":    {Type: types.ColumnTypeNumber, Data: int64(200)},
		}},
		{Data: map[string]*types.Value{
			"id":       {Type: types.ColumnTypeNumber, Data: int64(3)},
			"name":     {Type: types.ColumnTypeString, Data: "Desk"},
			"price":    {Type: types.ColumnTypeNumber, Data: int64(300)},
			"category": {Type: types.ColumnTypeString, Data: "Furniture"},
			"stock":    {Type: types.ColumnTypeNumber, Data: int64(30)},
		}},
		{Data: map[string]*types.Value{
			"id":       {Type: types.ColumnTypeNumber, Data: int64(4)},
			"name":     {Type: types.ColumnTypeString, Data: "Chair"},
			"price":    {Type: types.ColumnTypeNumber, Data: int64(150)},
			"category": {Type: types.ColumnTypeString, Data: "Furniture"},
			"stock":    {Type: types.ColumnTypeNumber, Data: int64(100)},
		}},
	}

	for _, row := range testRows {
		_, err := eng.InsertRow(ctx, tenantID, tableID, row, schema)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	t.Run("Covering index scan - category, price, name", func(t *testing.T) {
		// Index (category, price, name) covers projection (category, price)
		opts := &engineTypes.ScanOptions{
			Projection: []string{"category", "price", "name"},
		}

		iter, err := eng.ScanIndex(ctx, tenantID, tableID, 1, schema, opts)
		if err != nil {
			t.Fatalf("ScanIndex failed: %v", err)
		}
		defer iter.Close()

		// Should use indexOnlyIterator
		if !pebble.IsIndexOnlyIterator(iter) {
			t.Error("Should use indexOnlyIterator for covering index")
		}

		// Verify all rows are returned with correct values
		expectedRows := []struct {
			category string
			price    int64
			name     string
		}{
			{"Electronics", 25, "Mouse"},
			{"Electronics", 1200, "Laptop"},
			{"Furniture", 150, "Chair"},
			{"Furniture", 300, "Desk"},
		}

		count := 0
		for iter.Next() {
			row := iter.Row()

			if count >= len(expectedRows) {
				t.Errorf("More rows than expected")
				break
			}

			expected := expectedRows[count]
			
			category, ok := row.Data["category"]
			if !ok || category.Data != expected.category {
				t.Errorf("Row %d: expected category %s, got %v", count, expected.category, category)
			}

			price, ok := row.Data["price"]
			if !ok || price.Data != expected.price {
				t.Errorf("Row %d: expected price %d, got %v", count, expected.price, price)
			}

			name, ok := row.Data["name"]
			if !ok || name.Data != expected.name {
				t.Errorf("Row %d: expected name %s, got %v", count, expected.name, name)
			}

			count++
		}

		if count != len(expectedRows) {
			t.Errorf("Expected %d rows, got %d", len(expectedRows), count)
		}

		if err := iter.Error(); err != nil {
			t.Errorf("Iterator error: %v", err)
		}
	})

	t.Run("Non-covering index scan - need table access", func(t *testing.T) {
		// Projection includes 'stock' which is not in index - must use regular scan with table access
		opts := &engineTypes.ScanOptions{
			Projection: []string{"category", "price", "stock"},
		}

		iter, err := eng.ScanIndex(ctx, tenantID, tableID, 1, schema, opts)
		if err != nil {
			t.Fatalf("ScanIndex failed: %v", err)
		}
		defer iter.Close()

		// Should NOT be index-only iterator
		if pebble.IsIndexOnlyIterator(iter) {
			t.Error("Should not use indexOnlyIterator when index doesn't cover projection")
		}

		count := 0
		for iter.Next() {
			row := iter.Row()
			
			// Should have all projected columns including stock
			if _, ok := row.Data["stock"]; !ok {
				t.Error("Missing stock column")
			}
			
			count++
		}

		if count != 4 {
			t.Errorf("Expected 4 rows, got %d", count)
		}
	})

	t.Run("Covering index with filter", func(t *testing.T) {
		// Test covering index with complex filter
		opts := &engineTypes.ScanOptions{
			Projection: []string{"category", "price"},
			Filter: &engineTypes.FilterExpression{
				Type:     "and",
				Children: []*engineTypes.FilterExpression{
					{
						Type:     "simple",
						Column:   "category",
						Operator: "=",
						Value:    "Electronics",
					},
					{
						Type:     "simple",
						Column:   "price",
						Operator: ">",
						Value:    int64(100),
					},
				},
			},
		}

		iter, err := eng.ScanIndex(ctx, tenantID, tableID, 1, schema, opts)
		if err != nil {
			t.Fatalf("ScanIndex failed: %v", err)
		}
		defer iter.Close()

		// Should use indexOnlyIterator
		if !pebble.IsIndexOnlyIterator(iter) {
			t.Error("Should use indexOnlyIterator for covering index")
		}

		count := 0
		for iter.Next() {
			row := iter.Row()
			
			category := row.Data["category"].Data.(string)
			price := row.Data["price"].Data.(int64)

			if category != "Electronics" {
				t.Errorf("Expected category Electronics, got %s", category)
			}

			if price <= 100 {
				t.Errorf("Expected price > 100, got %d", price)
			}

			count++
		}

		// Only Laptop (Electronics, 1200) should match
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})
}