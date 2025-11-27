package main

import (
	"fmt"
	"github.com/guileen/pglitedb/types"
)

func main() {
	// Test record pooling
	fmt.Println("Testing record pooling...")
	
	// Acquire a record
	record := types.AcquireRecord()
	fmt.Printf("Acquired record: %+v\n", record)
	
	// Modify the record
	record.ID = "test-id"
	record.Table = "test-table"
	record.Data["name"] = &types.Value{Data: "test-name"}
	
	fmt.Printf("Modified record: %+v\n", record)
	
	// Release the record
	types.ReleaseRecord(record)
	fmt.Println("Released record")
	
	// Acquire another record to see if it's reused
	record2 := types.AcquireRecord()
	fmt.Printf("Acquired another record: %+v\n", record2)
	
	// It should be reset
	if record2.ID == "" && record2.Table == "" && len(record2.Data) == 0 {
		fmt.Println("Record was properly reset!")
	} else {
		fmt.Println("Record was not properly reset")
	}
	
	types.ReleaseRecord(record2)
	
	// Test result set pooling
	fmt.Println("\nTesting result set pooling...")
	
	resultSet := types.AcquireResultSet()
	resultSet.Columns = append(resultSet.Columns, types.ColumnInfo{Name: "id", Type: types.ColumnTypeString})
	resultSet.Columns = append(resultSet.Columns, types.ColumnInfo{Name: "name", Type: types.ColumnTypeString})
	resultSet.Rows = append(resultSet.Rows, []interface{}{"1", "Alice"})
	resultSet.Rows = append(resultSet.Rows, []interface{}{"2", "Bob"})
	resultSet.Count = 2
	
	fmt.Printf("ResultSet: %+v\n", resultSet)
	
	types.ReleaseResultSet(resultSet)
	
	// Acquire another result set
	resultSet2 := types.AcquireResultSet()
	if len(resultSet2.Columns) == 0 && len(resultSet2.Rows) == 0 && resultSet2.Count == 0 {
		fmt.Println("ResultSet was properly reset!")
	} else {
		fmt.Println("ResultSet was not properly reset")
	}
	
	types.ReleaseResultSet(resultSet2)
	
	fmt.Println("Memory pooling test completed")
}