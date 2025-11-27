package storage

import (
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

func TestMVCCStorage_EncodeDecodeKey(t *testing.T) {
	mvcc := &MVCCStorage{}
	
	key := []byte("testkey")
	timestamp := int64(1234567890)
	
	encoded := mvcc.encodeKey(key, timestamp)
	decodedKey, decodedTS, err := mvcc.decodeKey(encoded)
	
	if err != nil {
		t.Fatalf("Failed to decode key: %v", err)
	}
	
	if string(decodedKey) != string(key) {
		t.Errorf("Expected key '%s', got '%s'", string(key), string(decodedKey))
	}
	
	if decodedTS != timestamp {
		t.Errorf("Expected timestamp %d, got %d", timestamp, decodedTS)
	}
}

func TestMVCCStorage_VersionManagement(t *testing.T) {
	// This would require a real KV store for testing
	// For now, we'll just test the basic structure
	mvcc := &MVCCStorage{
		timestamp: time.Now().UnixNano(),
	}
	
	if mvcc.allocateTimestamp() <= mvcc.timestamp {
		t.Error("Allocated timestamp should be greater than initial timestamp")
	}
}