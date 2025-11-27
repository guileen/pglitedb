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
	
	fmt.Println("Memory pooling test completed")
}