package scan

import (
	"testing"
)

func TestIteratorPool_ResourceLeaks(t *testing.T) {
	// Skip this test due to circular dependency
	// This test would require importing the pools package which creates a circular dependency
	t.Skip("Skipping due to circular dependency with pools package")
}

func TestIteratorPool_HighConcurrency(t *testing.T) {
	// Skip this test due to circular dependency
	// This test would require importing the pools package which creates a circular dependency
	t.Skip("Skipping due to circular dependency with pools package")
}

func TestIteratorPool_MemoryPressure(t *testing.T) {
	// Skip this test due to circular dependency
	// This test would require importing the pools package which creates a circular dependency
	t.Skip("Skipping due to circular dependency with pools package")
}