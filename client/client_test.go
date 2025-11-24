package client

import (
	"testing"
	"github.com/guileen/pglitedb/types"
)

func TestClientCreation(t *testing.T) {
	// This test just verifies that the client can be created without panicking
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Client creation panicked: %v", r)
		}
	}()
	
	client := NewClient("/tmp/pglitedb-test")
	if client == nil {
		t.Error("NewClient returned nil")
	}
}

func TestConvertFunctions(t *testing.T) {
	// Test convertExternalToInternalOptions
	externalOptions := &types.QueryOptions{
		Columns: []string{"id", "name"},
		Where: map[string]interface{}{
			"age": 30,
		},
		OrderBy: []string{"name ASC", "age DESC"},
		Limit:   intPtr(10),
		Offset:  intPtr(0),
	}
	
	internalOptions := convertExternalToInternalOptions(externalOptions)
	
	if len(internalOptions.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(internalOptions.Columns))
	}
	
	if internalOptions.Where == nil {
		t.Error("Where map should not be nil")
	}
	
	if len(internalOptions.OrderBy) != 2 {
		t.Errorf("Expected 2 order by clauses, got %d", len(internalOptions.OrderBy))
	}
	
	if internalOptions.Limit == nil {
		t.Error("Limit should not be nil")
	} else if *internalOptions.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", *internalOptions.Limit)
	}
}

func intPtr(i int) *int {
	return &i
}