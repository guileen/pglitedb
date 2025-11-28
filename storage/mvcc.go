// Package storage provides MVCC (Multi-Version Concurrency Control) implementation
package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

// MVCCVersion represents a version of a record with timestamp metadata
type MVCCVersion struct {
	Key       []byte
	Value     []byte
	StartTS   int64
	CommitTS  int64
	Aborted   bool
	Deleted   bool
}

// MVCCRecord represents a record with all its versions
type MVCCRecord struct {
	Key      []byte
	Versions []*MVCCVersion
}

// MVCCStorage implements MVCC on top of the KV store
type MVCCStorage struct {
	kv        shared.KV
	mu        sync.RWMutex
	timestamp int64
}

// NewMVCCStorage creates a new MVCC storage wrapper
func NewMVCCStorage(kv shared.KV) *MVCCStorage {
	return &MVCCStorage{
		kv:        kv,
		timestamp: time.Now().UnixNano(),
	}
}

// allocateTimestamp generates a new unique timestamp
func (m *MVCCStorage) allocateTimestamp() int64 {
	return m.timestamp + 1
}

// encodeKey encodes a key with timestamp for MVCC
func (m *MVCCStorage) encodeKey(key []byte, timestamp int64) []byte {
	// Format: [original_key][timestamp]
	buf := make([]byte, len(key)+8)
	copy(buf, key)
	binary.BigEndian.PutUint64(buf[len(key):], uint64(timestamp))
	return buf
}

// decodeKey decodes a key to extract original key and timestamp
func (m *MVCCStorage) decodeKey(encodedKey []byte) ([]byte, int64, error) {
	if len(encodedKey) < 8 {
		return nil, 0, fmt.Errorf("invalid encoded key length")
	}
	
	keyLen := len(encodedKey) - 8
	originalKey := make([]byte, keyLen)
	copy(originalKey, encodedKey[:keyLen])
	
	timestamp := int64(binary.BigEndian.Uint64(encodedKey[keyLen:]))
	return originalKey, timestamp, nil
}

// Get retrieves the value for a key at the specified timestamp
func (m *MVCCStorage) Get(key []byte, timestamp int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Scan backwards from the timestamp to find the most recent committed version
	// We start from timestamp and go backward to 0
	for ts := timestamp; ts >= 0; ts-- {
		encodedKey := m.encodeKey(key, ts)
		value, err := m.kv.Get(context.Background(), encodedKey)
		if err != nil {
			if err == shared.ErrNotFound {
				continue // Try previous timestamp
			}
			return nil, err // Actual error
		}
		
		// Decode the value to get version info
		version := &MVCCVersion{}
		if err := m.decodeValue(value, version); err != nil {
			continue
		}
		
		// Check if this version is visible at the given timestamp
		if version.StartTS <= timestamp && 
		   (version.CommitTS > 0 && version.CommitTS <= timestamp) &&
		   !version.Aborted && !version.Deleted {
			if version.Value == nil {
				return nil, shared.ErrNotFound
			}
			foundValue := make([]byte, len(version.Value))
			copy(foundValue, version.Value)
			return foundValue, nil
		}
	}
	
	return nil, shared.ErrNotFound
}

// Put stores a new version of a key-value pair
func (m *MVCCStorage) Put(key, value []byte, startTS int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	version := &MVCCVersion{
		Key:     key,
		Value:   value,
		StartTS: startTS,
	}
	
	encodedKey := m.encodeKey(key, startTS)
	encodedValue, err := m.encodeValue(version)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	
	return m.kv.Set(context.Background(), encodedKey, encodedValue)
}

// Delete marks a key as deleted at the specified timestamp
func (m *MVCCStorage) Delete(key []byte, startTS int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	version := &MVCCVersion{
		Key:     key,
		Value:   nil,
		StartTS: startTS,
		Deleted: true,
	}
	
	encodedKey := m.encodeKey(key, startTS)
	encodedValue, err := m.encodeValue(version)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	
	return m.kv.Set(context.Background(), encodedKey, encodedValue)
}

// Commit commits a version by setting its commit timestamp
func (m *MVCCStorage) Commit(key []byte, startTS, commitTS int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	encodedKey := m.encodeKey(key, startTS)
	
	// Get the existing version
	value, err := m.kv.Get(context.Background(), encodedKey)
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	
	version := &MVCCVersion{}
	if err := m.decodeValue(value, version); err != nil {
		return fmt.Errorf("decode version: %w", err)
	}
	
	// Update commit timestamp
	version.CommitTS = commitTS
	
	// Store updated version
	encodedValue, err := m.encodeValue(version)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	
	return m.kv.Set(context.Background(), encodedKey, encodedValue)
}

// Abort aborts a version by marking it as aborted
func (m *MVCCStorage) Abort(key []byte, startTS int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	encodedKey := m.encodeKey(key, startTS)
	
	// Get the existing version
	value, err := m.kv.Get(context.Background(), encodedKey)
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	
	version := &MVCCVersion{}
	if err := m.decodeValue(value, version); err != nil {
		return fmt.Errorf("decode version: %w", err)
	}
	
	// Mark as aborted
	version.Aborted = true
	
	// Store updated version
	encodedValue, err := m.encodeValue(version)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	
	return m.kv.Set(context.Background(), encodedKey, encodedValue)
}

// encodeValue encodes an MVCCVersion to bytes
func (m *MVCCStorage) encodeValue(version *MVCCVersion) ([]byte, error) {
	// Simple encoding: [value_len][value][start_ts][commit_ts][flags]
	// Flags: bit 0 = aborted, bit 1 = deleted
	
	var buf bytes.Buffer
	
	// Value length and value
	if version.Value != nil {
		valueLen := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLen, uint32(len(version.Value)))
		buf.Write(valueLen)
		buf.Write(version.Value)
	} else {
		valueLen := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLen, 0)
		buf.Write(valueLen)
	}
	
	// Timestamps
	startTSBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(startTSBytes, uint64(version.StartTS))
	buf.Write(startTSBytes)
	
	commitTSBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(commitTSBytes, uint64(version.CommitTS))
	buf.Write(commitTSBytes)
	
	// Flags
	var flags byte
	if version.Aborted {
		flags |= 1
	}
	if version.Deleted {
		flags |= 2
	}
	buf.WriteByte(flags)
	
	return buf.Bytes(), nil
}

// decodeValue decodes bytes to an MVCCVersion
func (m *MVCCStorage) decodeValue(data []byte, version *MVCCVersion) error {
	if len(data) < 21 { // minimum size: 4 + 0 + 8 + 8 + 1
		return fmt.Errorf("invalid data length")
	}
	
	offset := 0
	
	// Value length
	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	// Value
	if valueLen > 0 {
		if len(data) < offset+int(valueLen) {
			return fmt.Errorf("invalid data length for value")
		}
		version.Value = make([]byte, valueLen)
		copy(version.Value, data[offset:offset+int(valueLen)])
		offset += int(valueLen)
	}
	
	// Start timestamp
	if len(data) < offset+8 {
		return fmt.Errorf("invalid data length for start timestamp")
	}
	version.StartTS = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	
	// Commit timestamp
	if len(data) < offset+8 {
		return fmt.Errorf("invalid data length for commit timestamp")
	}
	version.CommitTS = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	
	// Flags
	if len(data) < offset+1 {
		return fmt.Errorf("invalid data length for flags")
	}
	flags := data[offset]
	version.Aborted = (flags & 1) != 0
	version.Deleted = (flags & 2) != 0
	
	return nil
}

// GarbageCollect removes old versions that are no longer needed
func (m *MVCCStorage) GarbageCollect(minTimestamp int64) error {
	// This would implement garbage collection logic
	// For now, we'll leave it as a placeholder
	return nil
}

// GetVisibleVersions returns all visible versions of a key at a given timestamp
func (m *MVCCStorage) GetVisibleVersions(key []byte, timestamp int64) ([]*MVCCVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var versions []*MVCCVersion
	
	// Create iterator to scan versions
	opts := &shared.IteratorOptions{
		LowerBound: m.encodeKey(key, 0),
		UpperBound: m.encodeKey(key, timestamp+1),
	}
	
	iter := m.kv.NewIterator(opts)
	defer iter.Close()
	
	for iter.First(); iter.Valid(); iter.Next() {
		origKey, _, err := m.decodeKey(iter.Key())
		if err != nil {
			continue
		}
		
		// Check if this is the same key
		if bytes.Equal(origKey, key) {
			version := &MVCCVersion{}
			if err := m.decodeValue(iter.Value(), version); err != nil {
				continue
			}
			
			// Check if this version is visible
			if version.StartTS <= timestamp && 
			   (version.CommitTS > 0 && version.CommitTS <= timestamp) &&
			   !version.Aborted {
				versions = append(versions, version)
			}
		}
	}
	
	if iter.Error() != nil {
		return nil, iter.Error()
	}
	
	return versions, nil
}