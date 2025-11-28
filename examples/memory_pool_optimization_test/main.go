package main

import (
	"fmt"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Test acquiring and releasing records
	fmt.Println("Testing memory pool optimization...")
	
	// Acquire a record
	record := types.AcquireRecord()
	fmt.Printf("Acquired record: %+v\n", record)
	
	// Modify the record
	record.ID = "test-id"
	record.Table = "test-table"
	record.Data["key"] = &types.Value{Data: "value", Type: types.ColumnTypeString}
	
	// Release the record back to the pool
	types.ReleaseRecord(record)
	fmt.Println("Released record back to pool")
	
	// Acquire another record to see if it's reset
	record2 := types.AcquireRecord()
	fmt.Printf("Acquired record from pool: ID=%s, Table=%s, DataLength=%d\n", 
		record2.ID, record2.Table, len(record2.Data))
	
	// Test result set pooling
	rs := types.AcquireResultSet()
	rs.Columns = append(rs.Columns, types.ColumnInfo{Name: "id", Type: types.ColumnTypeString})
	rs.Rows = append(rs.Rows, []interface{}{"test-value"})
	fmt.Printf("Created result set with %d columns and %d rows\n", len(rs.Columns), len(rs.Rows))
	
	types.ReleaseResultSet(rs)
	
	rs2 := types.AcquireResultSet()
	fmt.Printf("Acquired result set from pool: Columns=%d, Rows=%d\n", len(rs2.Columns), len(rs2.Rows))
	
	// Show metrics
	metrics := types.GetGlobalMemoryPoolMetrics()
	fmt.Println("Memory pool metrics:")
	for poolType, metric := range metrics {
		fmt.Printf("  %s: Gets=%d, Puts=%d, Hits=%d, Misses=%d\n", 
			poolType, metric.Gets, metric.Puts, metric.Hits, metric.Misses)
	}
	
	fmt.Println("Memory pool optimization test completed successfully!")
}