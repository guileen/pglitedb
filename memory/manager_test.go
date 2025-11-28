package memory

import (
	"testing"
)

func TestMemoryContext(t *testing.T) {
	// Test creating a root context
	root := NewMemoryContext(nil, 1000)
	if root == nil {
		t.Fatal("Failed to create root memory context")
	}
	
	if root.GetLimit() != 1000 {
		t.Errorf("Expected limit 1000, got %d", root.GetLimit())
	}
	
	// Test allocation
	err := root.Allocate(100)
	if err != nil {
		t.Errorf("Unexpected error during allocation: %v", err)
	}
	
	if root.GetCurrentUsage() != 100 {
		t.Errorf("Expected usage 100, got %d", root.GetCurrentUsage())
	}
	
	// Test deallocation
	root.Deallocate(50)
	if root.GetCurrentUsage() != 50 {
		t.Errorf("Expected usage 50 after deallocation, got %d", root.GetCurrentUsage())
	}
	
	// Test limit enforcement
	err = root.Allocate(1000)
	if err == nil {
		t.Error("Expected error when exceeding limit, but got none")
	}
	
	// Test child context
	child := NewMemoryContext(root, 500)
	if child == nil {
		t.Fatal("Failed to create child memory context")
	}
	
	if child.GetLimit() != 500 {
		t.Errorf("Expected child limit 500, got %d", child.GetLimit())
	}
	
	// Test allocation propagation to parent
	err = child.Allocate(50)
	if err != nil {
		t.Errorf("Unexpected error during child allocation: %v", err)
	}
	
	// Both parent and child should show the allocation
	if root.GetCurrentUsage() != 100 { // 50 from previous + 50 from child
		t.Errorf("Expected root usage 100, got %d", root.GetCurrentUsage())
	}
	
	if child.GetCurrentUsage() != 50 {
		t.Errorf("Expected child usage 50, got %d", child.GetCurrentUsage())
	}
	
	// Test child limit enforcement
	err = child.Allocate(500)
	if err == nil {
		t.Error("Expected error when child exceeds its limit, but got none")
	}
}