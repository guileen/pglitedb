// Package transaction provides transaction logging and recovery mechanisms
package transaction

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
	"github.com/cockroachdb/pebble"
)

// LogRecordType represents the type of log record
type LogRecordType uint8

const (
	LogRecordBegin LogRecordType = iota
	LogRecordCommit
	LogRecordAbort
	LogRecordWrite
	LogRecordCheckpoint
)

// LogRecord represents a single log record
type LogRecord struct {
	Type        LogRecordType
	TxnID       TransactionID
	Key         []byte
	Value       []byte
	Timestamp   int64
	PrevLSN     uint64 // Previous Log Sequence Number
	LSN         uint64 // Log Sequence Number
	Checksum    uint32
}

// LogManager manages transaction logs with WAL (Write-Ahead Logging)
type LogManager struct {
	db           *pebble.DB
	nextLSN      uint64
	mu           sync.RWMutex
	flushPending bool
}

// NewLogManager creates a new log manager
func NewLogManager(dbPath string) (*LogManager, error) {
	// Open Pebble DB for logs
	opts := &pebble.Options{
		Levels:                make([]pebble.LevelOptions, 7),
		MemTableSize:          64 << 20, // 64MB
		MemTableStopWritesThreshold: 4,
	}
	
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("open log db: %w", err)
	}
	
	lm := &LogManager{
		db:      db,
		nextLSN: 1,
	}
	
	return lm, nil
}

// WriteLog writes a log record to the log
func (lm *LogManager) WriteLog(record *LogRecord) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// Assign LSN
	record.LSN = lm.nextLSN
	lm.nextLSN++
	
	// Encode the record
	encoded, err := lm.encodeLogRecord(record)
	if err != nil {
		return fmt.Errorf("encode log record: %w", err)
	}
	
	// Calculate checksum
	record.Checksum = crc32.ChecksumIEEE(encoded)
	
	// Re-encode with checksum
	encoded, err = lm.encodeLogRecord(record)
	if err != nil {
		return fmt.Errorf("encode log record with checksum: %w", err)
	}
	
	// Write to log
	key := lm.encodeLogKey(record.LSN)
	if err := lm.db.Set(key, encoded, pebble.Sync); err != nil {
		return fmt.Errorf("write log record: %w", err)
	}
	
	lm.flushPending = true
	return nil
}

// ReadLog reads a log record by LSN
func (lm *LogManager) ReadLog(lsn uint64) (*LogRecord, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	key := lm.encodeLogKey(lsn)
	value, closer, err := lm.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, shared.ErrNotFound
		}
		return nil, fmt.Errorf("read log record: %w", err)
	}
	defer closer.Close()
	
	record, err := lm.decodeLogRecord(value)
	if err != nil {
		return nil, fmt.Errorf("decode log record: %w", err)
	}
	
	// Verify checksum
	expectedChecksum := record.Checksum
	record.Checksum = 0
	encoded, _ := lm.encodeLogRecord(record)
	calculatedChecksum := crc32.ChecksumIEEE(encoded)
	
	if expectedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("log record checksum mismatch")
	}
	
	return record, nil
}

// encodeLogKey encodes an LSN to a key for storage
func (lm *LogManager) encodeLogKey(lsn uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, lsn)
	return key
}

// encodeLogRecord encodes a log record to bytes
func (lm *LogManager) encodeLogRecord(record *LogRecord) ([]byte, error) {
	var buf bytes.Buffer
	
	// Record type (1 byte)
	buf.WriteByte(uint8(record.Type))
	
	// Transaction ID (8 bytes)
	txnIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(txnIDBytes, uint64(record.TxnID))
	buf.Write(txnIDBytes)
	
	// Timestamp (8 bytes)
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(record.Timestamp))
	buf.Write(tsBytes)
	
	// Previous LSN (8 bytes)
	prevLSNBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(prevLSNBytes, record.PrevLSN)
	buf.Write(prevLSNBytes)
	
	// LSN (8 bytes)
	lsnBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBytes, record.LSN)
	buf.Write(lsnBytes)
	
	// Checksum (4 bytes)
	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, record.Checksum)
	buf.Write(checksumBytes)
	
	// Key length and key
	if record.Key != nil {
		keyLenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLenBytes, uint32(len(record.Key)))
		buf.Write(keyLenBytes)
		buf.Write(record.Key)
	} else {
		keyLenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLenBytes, 0)
		buf.Write(keyLenBytes)
	}
	
	// Value length and value
	if record.Value != nil {
		valueLenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLenBytes, uint32(len(record.Value)))
		buf.Write(valueLenBytes)
		buf.Write(record.Value)
	} else {
		valueLenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLenBytes, 0)
		buf.Write(valueLenBytes)
	}
	
	return buf.Bytes(), nil
}

// decodeLogRecord decodes bytes to a log record
func (lm *LogManager) decodeLogRecord(data []byte) (*LogRecord, error) {
	if len(data) < 37 { // Minimum size: 1 + 8 + 8 + 8 + 8 + 4
		return nil, fmt.Errorf("invalid log record size")
	}
	
	record := &LogRecord{}
	offset := 0
	
	// Record type
	record.Type = LogRecordType(data[offset])
	offset++
	
	// Transaction ID
	record.TxnID = TransactionID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	
	// Timestamp
	record.Timestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	
	// Previous LSN
	record.PrevLSN = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	// LSN
	record.LSN = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	// Checksum
	record.Checksum = binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	// Key
	if len(data) < offset+4 {
		return nil, fmt.Errorf("invalid log record size for key length")
	}
	keyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	if keyLen > 0 {
		if len(data) < offset+int(keyLen) {
			return nil, fmt.Errorf("invalid log record size for key")
		}
		record.Key = make([]byte, keyLen)
		copy(record.Key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)
	}
	
	// Value
	if len(data) < offset+4 {
		return nil, fmt.Errorf("invalid log record size for value length")
	}
	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	if valueLen > 0 {
		if len(data) < offset+int(valueLen) {
			return nil, fmt.Errorf("invalid log record size for value")
		}
		record.Value = make([]byte, valueLen)
		copy(record.Value, data[offset:offset+int(valueLen)])
	}
	
	return record, nil
}

// LogBegin logs the beginning of a transaction
func (lm *LogManager) LogBegin(txnID TransactionID) error {
	record := &LogRecord{
		Type:      LogRecordBegin,
		TxnID:     txnID,
		Timestamp: time.Now().UnixNano(),
		PrevLSN:   0, // First record for this transaction
	}
	
	return lm.WriteLog(record)
}

// LogCommit logs the commit of a transaction
func (lm *LogManager) LogCommit(txnID TransactionID) error {
	record := &LogRecord{
		Type:      LogRecordCommit,
		TxnID:     txnID,
		Timestamp: time.Now().UnixNano(),
	}
	
	return lm.WriteLog(record)
}

// LogAbort logs the abort of a transaction
func (lm *LogManager) LogAbort(txnID TransactionID) error {
	record := &LogRecord{
		Type:      LogRecordAbort,
		TxnID:     txnID,
		Timestamp: time.Now().UnixNano(),
	}
	
	return lm.WriteLog(record)
}

// LogWrite logs a write operation within a transaction
func (lm *LogManager) LogWrite(txnID TransactionID, key, value []byte) error {
	record := &LogRecord{
		Type:      LogRecordWrite,
		TxnID:     txnID,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}
	
	return lm.WriteLog(record)
}

// LogCheckpoint logs a checkpoint
func (lm *LogManager) LogCheckpoint() error {
	record := &LogRecord{
		Type:      LogRecordCheckpoint,
		Timestamp: time.Now().UnixNano(),
	}
	
	return lm.WriteLog(record)
}

// GetLatestLSN returns the latest LSN
func (lm *LogManager) GetLatestLSN() uint64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.nextLSN - 1
}

// Recover performs recovery by replaying log records
func (lm *LogManager) Recover(kv shared.KV) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// Find the last checkpoint
	lastCheckpointLSN, err := lm.findLastCheckpoint()
	if err != nil {
		return fmt.Errorf("find last checkpoint: %w", err)
	}
	
	// Replay log records from last checkpoint
	startLSN := lastCheckpointLSN + 1
	if lastCheckpointLSN == 0 {
		startLSN = 1
	}
	
	for lsn := startLSN; lsn < lm.nextLSN; lsn++ {
		record, err := lm.ReadLog(lsn)
		if err != nil {
			if err == shared.ErrNotFound {
				continue
			}
			return fmt.Errorf("read log record %d: %w", lsn, err)
		}
		
		// Apply the log record
		if err := lm.applyLogRecord(record, kv); err != nil {
			return fmt.Errorf("apply log record %d: %w", lsn, err)
		}
	}
	
	return nil
}

// findLastCheckpoint finds the LSN of the last checkpoint
func (lm *LogManager) findLastCheckpoint() (uint64, error) {
	var lastCheckpointLSN uint64
	
	// Iterate backwards from the latest LSN to find the last checkpoint
	for lsn := lm.nextLSN - 1; lsn > 0; lsn-- {
		record, err := lm.ReadLog(lsn)
		if err != nil {
			if err == shared.ErrNotFound {
				continue
			}
			return 0, err
		}
		
		if record.Type == LogRecordCheckpoint {
			lastCheckpointLSN = record.LSN
			break
		}
	}
	
	return lastCheckpointLSN, nil
}

// applyLogRecord applies a log record to the database
func (lm *LogManager) applyLogRecord(record *LogRecord, kv shared.KV) error {
	ctx := context.Background()
	
	switch record.Type {
	case LogRecordWrite:
		// Apply write operation
		return kv.Set(ctx, record.Key, record.Value)
	case LogRecordCommit:
		// Commit is handled at transaction level, nothing to do here
		return nil
	case LogRecordAbort:
		// Abort is handled at transaction level, nothing to do here
		return nil
	case LogRecordBegin:
		// Begin is handled at transaction level, nothing to do here
		return nil
	case LogRecordCheckpoint:
		// Checkpoint marker, nothing to do
		return nil
	default:
		return fmt.Errorf("unknown log record type: %d", record.Type)
	}
}

// Flush forces a flush of pending log writes
func (lm *LogManager) Flush() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	if !lm.flushPending {
		return nil
	}
	
	if err := lm.db.Flush(); err != nil {
		return fmt.Errorf("flush log: %w", err)
	}
	
	lm.flushPending = false
	return nil
}

// Close closes the log manager
func (lm *LogManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	if err := lm.db.Close(); err != nil {
		return fmt.Errorf("close log db: %w", err)
	}
	
	return nil
}

// GetLogStats returns statistics about the log
func (lm *LogManager) GetLogStats() (map[string]interface{}, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	metrics := lm.db.Metrics()
	
	stats := map[string]interface{}{
		"latest_lsn":        lm.nextLSN - 1,
		"log_size_bytes":    metrics.DiskSpaceUsage(),
		"memtable_size":     metrics.MemTable.Size,
		"flush_count":       metrics.Flush.Count,
		"compaction_count":  metrics.Compact.Count,
	}
	
	return stats, nil
}