package scan

import (
	"testing"
)

func TestIndexOnlyIterator_Reset(t *testing.T) {
	// This test is no longer needed as Reset method has been removed
	// The iterator now uses direct allocation instead of pooling
	t.Skip("Reset method removed - iterator now uses direct allocation")
}

func TestIndexIterator_Reset(t *testing.T) {
	// This test is no longer needed as Reset method has been removed
	// The iterator now uses direct allocation instead of pooling
	t.Skip("Reset method removed - iterator now uses direct allocation")
}

func TestRowIterator_Reset(t *testing.T) {
	// This test is no longer needed as Reset method has been removed
	// The iterator now uses direct allocation instead of pooling
	t.Skip("Reset method removed - iterator now uses direct allocation")
}