package catalog

import (
	"fmt"
	"testing"
	"time"

	"github.com/guileen/pglitedb/types"
)

func BenchmarkQuerySorting(b *testing.B) {
	// Create test data
	records := make([]*types.Record, 10000)
	for i := 0; i < 10000; i++ {
		records[i] = &types.Record{
			Data: map[string]*types.Value{
				"id":    {Data: int64(i), Type: types.ColumnTypeNumber},
				"name":  {Data: fmt.Sprintf("User%d", 10000-i), Type: types.ColumnTypeString},
				"score": {Data: float64(10000 - i), Type: types.ColumnTypeNumber},
				"active": {Data: i%2 == 0, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	// Create a mock query manager
	qm := &queryManager{}

	b.Run("SortByID", func(b *testing.B) {
		orderBy := []string{"id"}
		schema := &types.TableDefinition{
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeNumber},
				{Name: "name", Type: types.ColumnTypeString},
				{Name: "score", Type: types.ColumnTypeNumber},
				{Name: "active", Type: types.ColumnTypeBoolean},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sorted := qm.sortRecords(records, orderBy, schema)
			if len(sorted) != len(records) {
				b.Fatalf("Expected %d records, got %d", len(records), len(sorted))
			}
		}
	})

	b.Run("SortByName", func(b *testing.B) {
		orderBy := []string{"name"}
		schema := &types.TableDefinition{
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeNumber},
				{Name: "name", Type: types.ColumnTypeString},
				{Name: "score", Type: types.ColumnTypeNumber},
				{Name: "active", Type: types.ColumnTypeBoolean},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sorted := qm.sortRecords(records, orderBy, schema)
			if len(sorted) != len(records) {
				b.Fatalf("Expected %d records, got %d", len(records), len(sorted))
			}
		}
	})

	b.Run("SortByMultipleFields", func(b *testing.B) {
		orderBy := []string{"active", "score DESC"}
		schema := &types.TableDefinition{
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeNumber},
				{Name: "name", Type: types.ColumnTypeString},
				{Name: "score", Type: types.ColumnTypeNumber},
				{Name: "active", Type: types.ColumnTypeBoolean},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sorted := qm.sortRecords(records, orderBy, schema)
			if len(sorted) != len(records) {
				b.Fatalf("Expected %d records, got %d", len(records), len(sorted))
			}
		}
	})
}

func BenchmarkQuerySortingWithIndex(b *testing.B) {
	// This would test the performance when using index-based sorting
	// Since we don't have a full database setup, we'll simulate the concept
	
	// In a real implementation, index-based sorting would be significantly faster
	// because it leverages the ordered nature of the index itself
	b.Skip("Skipping index-based sorting benchmark - requires full database setup")
}