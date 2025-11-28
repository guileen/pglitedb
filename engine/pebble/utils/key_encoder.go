package utils

import (
	"fmt"
)

// KeyEncoder provides key encoding utilities
type KeyEncoder struct{}

// NewKeyEncoder creates a new key encoder
func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{}
}

// EncodeTableKey encodes a table key
func (ke *KeyEncoder) EncodeTableKey(tenantID, tableID, rowID int64) []byte {
	// This is a placeholder implementation
	// In a real implementation, this would properly encode the key
	key := fmt.Sprintf("t:%d:%d:%d", tenantID, tableID, rowID)
	return []byte(key)
}

// EncodeIndexKey encodes an index key
func (ke *KeyEncoder) EncodeIndexKey(tenantID, tableID, indexID int64, value interface{}, rowID int64) ([]byte, error) {
	// This is a placeholder implementation
	// In a real implementation, this would properly encode the key
	key := fmt.Sprintf("i:%d:%d:%d:%v:%d", tenantID, tableID, indexID, value, rowID)
	return []byte(key), nil
}

// EncodeIndexScanStartKey encodes an index scan start key
func (ke *KeyEncoder) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	// This is a placeholder implementation
	key := fmt.Sprintf("i:%d:%d:%d:", tenantID, tableID, indexID)
	return []byte(key)
}

// EncodeIndexScanEndKey encodes an index scan end key
func (ke *KeyEncoder) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	// This is a placeholder implementation
	key := fmt.Sprintf("i:%d:%d:%d:\xff", tenantID, tableID, indexID)
	return []byte(key)
}