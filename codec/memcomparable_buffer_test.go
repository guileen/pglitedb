package codec

import (
	"testing"
)

func TestEncodeTableKeyBuffer(t *testing.T) {
	c := NewMemComparableCodec()
	
	// Test basic encoding
	buf := make([]byte, 0, 32)
	result, err := c.EncodeTableKeyBuffer(1, 2, 3, buf)
	if err != nil {
		t.Fatalf("EncodeTableKeyBuffer failed: %v", err)
	}
	
	// Verify we can decode it back
	tenantID, tableID, rowID, err := c.DecodeTableKey(result)
	if err != nil {
		t.Fatalf("DecodeTableKey failed: %v", err)
	}
	
	if tenantID != 1 || tableID != 2 || rowID != 3 {
		t.Errorf("Expected (1,2,3), got (%d,%d,%d)", tenantID, tableID, rowID)
	}
	
	// Test with nil buffer
	result2, err := c.EncodeTableKeyBuffer(4, 5, 6, nil)
	if err != nil {
		t.Fatalf("EncodeTableKeyBuffer with nil buffer failed: %v", err)
	}
	
	// Verify we can decode it back
	tenantID2, tableID2, rowID2, err := c.DecodeTableKey(result2)
	if err != nil {
		t.Fatalf("DecodeTableKey failed: %v", err)
	}
	
	if tenantID2 != 4 || tableID2 != 5 || rowID2 != 6 {
		t.Errorf("Expected (4,5,6), got (%d,%d,%d)", tenantID2, tableID2, rowID2)
	}
	
	// Test that the results are identical to the original EncodeTableKey
	original := c.EncodeTableKey(1, 2, 3)
	if len(original) != len(result) {
		t.Errorf("Length mismatch: original=%d, buffered=%d", len(original), len(result))
	}
	
	for i := range original {
		if original[i] != result[i] {
			t.Errorf("Byte mismatch at position %d: original=%d, buffered=%d", i, original[i], result[i])
		}
	}
}