package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/guileen/pglitedb/storage/shared"
)

type PebbleKV struct {
	db            *pebble.DB
	dbPath        string
	closed        bool
	mu            sync.RWMutex
	pendingWrites int64
	flushTicker   *time.Ticker
	flushDone     chan struct{}

	globalTS          atomic.Int64
	keyTimestamps     sync.Map
	activeTransactions *sync.Map // Changed from map[uint64]*PebbleTransaction to *sync.Map
	nextTxnID          uint64

	// Compaction monitor for performance tracking
	compactionMonitor *CompactionMonitor
	
	// Cache reference for proper cleanup
	cache *pebble.Cache
}

func NewPebbleKV(config *PebbleConfig) (*PebbleKV, error) {
	cache := pebble.NewCache(config.CacheSize)
	// Note: We don't defer cache.Unref() here because we need to unreference the cache
	// when the database is closed, not when this function returns

	// Configure compression levels
	compression := make([]pebble.Compression, 3)
	if config.CompressionEnabled {
		// Enable Snappy compression for better space efficiency
		compression[0] = pebble.SnappyCompression
		compression[1] = pebble.SnappyCompression
		compression[2] = pebble.SnappyCompression
	} else {
		// Disable compression
		compression[0] = pebble.NoCompression
		compression[1] = pebble.NoCompression
		compression[2] = pebble.NoCompression
	}

	// Custom comparer for PostgreSQL-like key ordering
	postgresComparer := &pebble.Comparer{
		Name: "pglitedb-postgres",
		Compare: func(a, b []byte) int {
			// Handle empty keys
			if len(a) == 0 && len(b) == 0 {
				return 0
			}
			if len(a) == 0 {
				return -1
			}
			if len(b) == 0 {
				return 1
			}

			// Keys are structured as: KeyType + tenantID + tableID + [rowID/index components]
			// For better locality, we want to group by tenantID and tableID first

			// Compare key types first
			if a[0] != b[0] {
				return int(a[0]) - int(b[0])
			}

			// For table keys: 't' + tenantID + 'r' + tableID + rowID
			// For index keys: 't' + tenantID + 'i' + tableID + indexID + [index values] + rowID

			// Extract and compare tenantID (starts at byte 1)
			aOffset := 1
			bOffset := 1

			// Compare tenantID (8 bytes each)
			if aOffset+8 <= len(a) && bOffset+8 <= len(b) {
				tenantA := int64(binary.BigEndian.Uint64(a[aOffset : aOffset+8]))
				tenantB := int64(binary.BigEndian.Uint64(b[bOffset : bOffset+8]))
				if tenantA != tenantB {
					if tenantA < tenantB {
						return -1
					}
					return 1
				}
				aOffset += 8
				bOffset += 8
			}

			// Move past intermediate key type ('r' or 'i')
			if aOffset < len(a) && bOffset < len(b) {
				aOffset++
				bOffset++
			}

			// Compare tableID (8 bytes each)
			if aOffset+8 <= len(a) && bOffset+8 <= len(b) {
				tableA := int64(binary.BigEndian.Uint64(a[aOffset : aOffset+8]))
				tableB := int64(binary.BigEndian.Uint64(b[bOffset : bOffset+8]))
				if tableA != tableB {
					if tableA < tableB {
						return -1
					}
					return 1
				}
				aOffset += 8
				bOffset += 8
			}

			// For remaining parts, use lexicographic comparison for better performance
			remainingA := a[aOffset:]
			remainingB := b[bOffset:]

			return bytes.Compare(remainingA, remainingB)
		},
		AbbreviatedKey: func(key []byte) uint64 {
			// Use first 8 bytes for abbreviated key comparison
			if len(key) >= 8 {
				return binary.BigEndian.Uint64(key[:8])
			}
			// Pad with zeros if key is shorter
			var padded [8]byte
			copy(padded[:], key)
			return binary.BigEndian.Uint64(padded[:])
		},
		Equal: bytes.Equal,
		Separator: func(dst, a, b []byte) []byte {
			// Use default separator logic
			return nil
		},
		Successor: func(dst, a []byte) []byte {
			// Use default successor logic
			return nil
		},
		ImmediateSuccessor: func(dst, a []byte) []byte {
			// Use default immediate successor logic
			return nil
		},
	}

	opts := &pebble.Options{
		Cache: cache,
		MaxOpenFiles:   config.MaxOpenFiles,
		MemTableSize:   uint64(config.MemTableSize),
		MemTableStopWritesThreshold: 8, // Increased threshold to reduce write stalls
		L0CompactionThreshold: config.L0CompactionThreshold,    // Configurable L0 compaction threshold to reduce write amplification
		L0StopWritesThreshold: config.L0StopWritesThreshold,    // Configurable L0 stop writes threshold to reduce write stalls
		LBaseMaxBytes:         config.LBaseMaxBytes,            // Configurable LBase max bytes
		MaxConcurrentCompactions: func() int { return config.CompactionConcurrency },
		Levels: []pebble.LevelOptions{
			{TargetFileSize: config.TargetFileSize, BlockSize: config.BlockSize, Compression: compression[0]},   // Configurable target file size
			{TargetFileSize: config.TargetFileSize * 4, BlockSize: config.BlockSize, Compression: compression[1]},  // 4x target file size
			{TargetFileSize: config.TargetFileSize * 16, BlockSize: config.BlockSize, Compression: compression[2]}, // 16x target file size
		},
		MaxManifestFileSize: config.MaxManifestFileSize, // Configurable max manifest file size
		Comparer:            postgresComparer,           // Custom comparer for better key ordering
	}
	
	// Use custom filesystem if provided (useful for testing with in-memory FS)
	// This helps avoid disk I/O and background goroutines that can cause test hangs
	if config.FS != nil {
		opts.FS = config.FS
	}
	
	// Configure WAL and compaction options for testing
	if config.DisableWAL {
		opts.DisableWAL = true
	}
	if config.DisableAutoCompaction {
		opts.DisableAutomaticCompactions = true
	}

	// Configure rate limiter if enabled
	// Note: In Pebble v1.1.5, rate limiting is handled via TargetByteDeletionRate
	if config.EnableRateLimiting && config.RateLimitBytesPerSec > 0 {
		opts.TargetByteDeletionRate = int(config.RateLimitBytesPerSec)
	}

	// Configure bloom filters if enabled
	if config.EnableBloomFilter {
		for i := range opts.Levels {
			opts.Levels[i].FilterPolicy = bloom.FilterPolicy(config.BloomFilterBitsPerKey)
			opts.Levels[i].FilterType = pebble.TableFilter
		}
	}

	// Rate limiting is handled via TargetByteDeletionRate
	// No need to set a separate rate limiter object
	if config.EnableRateLimiting && config.RateLimitBytesPerSec > 0 {
		opts.TargetByteDeletionRate = int(config.RateLimitBytesPerSec)
	}

	db, err := pebble.Open(config.Path, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	pkv := &PebbleKV{
		db:                 db,
		dbPath:             config.Path,
		closed:             false,
		flushTicker:        time.NewTicker(config.FlushInterval),
		flushDone:          make(chan struct{}),
		activeTransactions: &sync.Map{},
		nextTxnID:          1, // Start from 1 to avoid issues with ID 0
		cache:              cache, // Store cache reference for proper cleanup
	}

	// Initialize compaction monitor for performance tracking
	pkv.compactionMonitor = NewCompactionMonitor(pkv, 5*time.Second)
	pkv.compactionMonitor.Start()

	go pkv.backgroundFlush()

	return pkv, nil
}

func (p *PebbleKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, shared.ErrClosed
	}

	value, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, shared.ErrNotFound
		}
		return nil, fmt.Errorf("pebble get: %w", err)
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (p *PebbleKV) Set(ctx context.Context, key, value []byte) error {
	return p.SetWithOptions(ctx, key, value, nil)
}

func (p *PebbleKV) SetWithOptions(ctx context.Context, key, value []byte, opts *shared.WriteOptions) error {
	// Check if closed with read lock first
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return shared.ErrClosed
	}
	p.mu.RUnlock()

	// For async writes (Sync: false), increment pending counter and don't decrement immediately
	// For sync writes (Sync: true), increment and decrement immediately
	isAsync := opts == nil || !opts.Sync
	
	if isAsync {
		atomic.AddInt64(&p.pendingWrites, 1)
		// Note: Counter is decremented by Flush operations, not immediately
	} else {
		atomic.AddInt64(&p.pendingWrites, 1)
		defer atomic.AddInt64(&p.pendingWrites, -1)
	}

	var pebbleOpts *pebble.WriteOptions
	if opts != nil {
		pebbleOpts = &pebble.WriteOptions{
			Sync: opts.Sync,
		}
	}

	// Only lock for the actual database operation, and use read lock for checking closed state
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		atomic.AddInt64(&p.pendingWrites, -1) // Decrement if closed
		return shared.ErrClosed
	}
	
	// Clone key and value to avoid holding references
	keyClone := make([]byte, len(key))
	copy(keyClone, key)
	valueClone := make([]byte, len(value))
	copy(valueClone, value)
	
	err := p.db.Set(keyClone, valueClone, pebbleOpts)
	p.mu.RUnlock()
	
	if err != nil {
		atomic.AddInt64(&p.pendingWrites, -1) // Decrement on error
		return fmt.Errorf("pebble set: %w", err)
	}

	// Update timestamp for MVCC (no lock needed for sync.Map)
	ts := p.allocateTimestamp()
	p.setKeyTimestamp(key, ts)

	return nil
}

func (p *PebbleKV) Delete(ctx context.Context, key []byte) error {
	return p.DeleteWithOptions(ctx, key, nil)
}

func (p *PebbleKV) DeleteWithOptions(ctx context.Context, key []byte, opts *shared.WriteOptions) error {
	// Check if closed with read lock first
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return shared.ErrClosed
	}
	p.mu.RUnlock()

	// For async writes (Sync: false), increment pending counter and don't decrement immediately
	// For sync writes (Sync: true), increment and decrement immediately
	isAsync := opts == nil || !opts.Sync
	
	if isAsync {
		atomic.AddInt64(&p.pendingWrites, 1)
		// Note: Counter is decremented by Flush operations, not immediately
	} else {
		atomic.AddInt64(&p.pendingWrites, 1)
		defer atomic.AddInt64(&p.pendingWrites, -1)
	}

	var pebbleOpts *pebble.WriteOptions
	if opts != nil {
		pebbleOpts = &pebble.WriteOptions{
			Sync: opts.Sync,
		}
	}

	// Only lock for the actual database operation, and use read lock for checking closed state
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		atomic.AddInt64(&p.pendingWrites, -1) // Decrement if closed
		return shared.ErrClosed
	}
	
	// Clone key to avoid holding references
	keyClone := make([]byte, len(key))
	copy(keyClone, key)
	
	err := p.db.Delete(keyClone, pebbleOpts)
	p.mu.RUnlock()
	
	if err != nil {
		atomic.AddInt64(&p.pendingWrites, -1) // Decrement on error
		return fmt.Errorf("pebble delete: %w", err)
	}

	// Update timestamp for MVCC (no lock needed for sync.Map)
	ts := p.allocateTimestamp()
	p.setKeyTimestamp(key, ts)

	return nil
}

func (p *PebbleKV) NewBatch() shared.Batch {
	return &PebbleBatch{
		batch: p.db.NewBatch(),
		kv:    p,
	}
}

func (p *PebbleKV) Commit(ctx context.Context, batch shared.Batch) error {
	return p.CommitBatchWithOptions(ctx, batch, nil)
}

func (p *PebbleKV) CommitBatch(ctx context.Context, batch shared.Batch) error {
	return p.CommitBatchWithOptions(ctx, batch, nil)
}

func (p *PebbleKV) CommitBatchWithOptions(ctx context.Context, batch shared.Batch, opts *shared.WriteOptions) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return shared.ErrClosed
	}

	pebbleBatch, ok := batch.(*PebbleBatch)
	if !ok {
		return fmt.Errorf("invalid batch type")
	}

	atomic.AddInt64(&p.pendingWrites, 1)
	defer atomic.AddInt64(&p.pendingWrites, -1)

	var pebbleOpts *pebble.WriteOptions
	if opts != nil {
		pebbleOpts = &pebble.WriteOptions{
			Sync: opts.Sync,
		}
	}

	if err := p.db.Apply(pebbleBatch.batch, pebbleOpts); err != nil {
		return fmt.Errorf("pebble apply batch: %w", err)
	}

	// Update timestamps for all keys in the batch
	// Note: This is a simplified approach. In a real implementation, you might want to
	// track individual key timestamps more precisely.
	// For simplicity, we're not updating individual key timestamps here
	// A full implementation would parse the batch operations and update timestamps accordingly

	return nil
}

func (p *PebbleKV) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil
	}

	var pebbleOpts *pebble.IterOptions
	if opts != nil {
		pebbleOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	iter, err := p.db.NewIter(pebbleOpts)
	if err != nil {
		return nil
	}
	return &PebbleIterator{iter: iter}
}

func (p *PebbleKV) NewSnapshot() (shared.Snapshot, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, shared.ErrClosed
	}

	snapshot := p.db.NewSnapshot()
	return &PebbleSnapshot{snapshot: snapshot}, nil
}

func (p *PebbleKV) NewTransaction(ctx context.Context) (shared.Transaction, error) {
	// Use a read lock instead of write lock for transaction creation
	// since we're only reading the closed state and atomically incrementing nextTxnID
	p.mu.RLock()
	
	if p.closed {
		p.mu.RUnlock()
		return nil, shared.ErrClosed
	}
	
	// Release the read lock before creating the batch to avoid blocking other operations
	p.mu.RUnlock()

	txnID := atomic.AddUint64(&p.nextTxnID, 1)
	txn := &PebbleTransaction{
		db:         p.db,
		batch:      p.db.NewBatch(),
		kv:         p,
		isolation:  shared.ReadCommitted,
		txnID:      txnID,
		writeKeys:  make(map[string]bool),
		readKeys:   make(map[string][]byte),
		startTS:    time.Now().UnixNano(),
		readSet:    make(map[string]int64),
	}
	p.activeTransactions.Store(txnID, txn)
	return txn, nil
}

func (p *PebbleKV) allocateTimestamp() int64 {
	return p.globalTS.Add(1)
}

func (p *PebbleKV) getKeyTimestamp(key []byte) int64 {
	if ts, ok := p.keyTimestamps.Load(string(key)); ok {
		return ts.(int64)
	}
	return 0
}

func (p *PebbleKV) setKeyTimestamp(key []byte, ts int64) {
	p.keyTimestamps.Store(string(key), ts)
}

// GetActiveTransaction retrieves an active transaction by its ID
func (p *PebbleKV) GetActiveTransaction(txnID uint64) (*PebbleTransaction, bool) {
	if txn, ok := p.activeTransactions.Load(txnID); ok {
		if pebbleTxn, ok := txn.(*PebbleTransaction); ok {
			return pebbleTxn, true
		}
	}
	return nil, false
}

func (p *PebbleKV) Stats() shared.KVStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return shared.KVStats{}
	}

	metrics := p.db.Metrics()
	
	// Calculate read amplification (number of levels)
	readAmp := 0
	var totalBytesFlushed uint64
	var totalBytesCompacted uint64
	
	for _, level := range metrics.Levels {
		if level.NumFiles > 0 {
			readAmp++
		}
		totalBytesFlushed += level.BytesFlushed
		totalBytesCompacted += level.BytesCompacted
	}
	
	// Calculate space amplification
	var spaceAmp float64
	if metrics.Total().Size > 0 {
		spaceAmp = float64(metrics.DiskSpaceUsage()) / float64(metrics.Total().Size)
	}
	
	// Calculate write amplification
	var writeAmp float64
	if metrics.Total().Size > 0 {
		writeAmp = float64(totalBytesFlushed+totalBytesCompacted) / float64(metrics.Total().Size)
	}
	
	return shared.KVStats{
		ReadAmplification:  int64(readAmp),
		WriteAmplification: writeAmp,
		SpaceAmplification: spaceAmp,
		KeyCount:           0, // Key count is not directly available in Pebble metrics
		ApproximateSize:    int64(metrics.DiskSpaceUsage()),
		MemTableSize:       int64(metrics.MemTable.Size),
		FlushCount:         int64(metrics.Flush.Count),
		CompactionCount:    int64(metrics.Compact.Count),
		PendingWrites:      atomic.LoadInt64(&p.pendingWrites),
		// Compaction-specific statistics
		L0FileCount:            int64(metrics.Levels[0].NumFiles),
		L1FileCount:            int64(metrics.Levels[1].NumFiles),
		L2FileCount:            int64(metrics.Levels[2].NumFiles),
		CompactionBytesWritten: int64(totalBytesCompacted),
	}
}

func (p *PebbleKV) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return shared.ErrClosed
	}

	// Reset pending writes counter when flushing (process all pending writes)
	atomic.StoreInt64(&p.pendingWrites, 0)

	return p.db.Flush()
}

func (p *PebbleKV) backgroundFlush() {
	defer func() {
		// Ensure any cleanup happens here if needed
	}()
	
	for {
		select {
		case <-p.flushTicker.C:
			// Check if we should exit first
			p.mu.RLock()
			if p.closed {
				p.mu.RUnlock()
				return
			}
			
			// Only flush if there are pending writes
			if atomic.LoadInt64(&p.pendingWrites) > 0 {
				p.mu.RUnlock()
				
				// Perform flush without holding the lock
				_ = p.db.Flush()
				
				// Reset pending writes counter when background flushing (process all pending writes)
				atomic.StoreInt64(&p.pendingWrites, 0)
			} else {
				p.mu.RUnlock()
			}
		case <-p.flushDone:
			// Received signal to stop
			return
		}
	}
}

func (p *PebbleKV) CheckForConflicts(txn shared.Transaction, key []byte) error {
	// This is a simplified conflict detection mechanism
	// In a real implementation, you would check for write-write conflicts
	// and read-write conflicts based on timestamps and isolation levels
	
	pebbleTxn, ok := txn.(*PebbleTransaction)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	
	// Check if any active transactions have written to this key
	// with a newer timestamp
	currentTS := p.getKeyTimestamp(key)
	
	var conflictErr error
	p.activeTransactions.Range(func(_, value interface{}) bool {
		activeTxn := value.(*PebbleTransaction)
		if activeTxn.txnID != pebbleTxn.txnID && activeTxn.committed {
			// Check if this active transaction wrote to the same key
			if _, written := activeTxn.writeKeys[string(key)]; written {
				activeTSTxn := p.getKeyTimestamp(key)
				if activeTSTxn > currentTS {
					conflictErr = fmt.Errorf("write-write conflict detected")
					return false // Stop iteration
				}
			}
		}
		return true // Continue iteration
	})
	
	return conflictErr
}

func (p *PebbleKV) Close() error {
	// Lock to check and set closed state
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Stop the flush ticker and signal background goroutine to stop
	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	if p.flushDone != nil {
		close(p.flushDone)
	}

	// Stop the compaction monitor
	if p.compactionMonitor != nil {
		p.compactionMonitor.Stop()
	}

	// Wait for pending writes to complete (with timeout)
	// Use a more robust waiting mechanism
	timeout := time.After(1 * time.Second) // Reduced timeout to prevent test hangs
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	// Wait for pending writes to complete or timeout
	waiting := true
	for waiting && atomic.LoadInt64(&p.pendingWrites) > 0 {
		select {
		case <-timeout:
			// Timeout - proceed with close anyway to prevent hanging
			waiting = false
		case <-ticker.C:
			// Continue waiting
		}
	}

	// Force flush any remaining data before closing
	// Use read lock for flush since we only need to check if db is not nil
	p.mu.RLock()
	var flushErr error
	if p.db != nil {
		flushErr = p.db.Flush()
	}
	p.mu.RUnlock()

	// Close the database - this should properly terminate all internal goroutines
	p.mu.Lock()
	var err error
	if p.db != nil {
		err = p.db.Close()
		p.db = nil // Prevent further use
	}

	// Unreference the cache to allow it to be garbage collected
	if p.cache != nil {
		p.cache.Unref()
		p.cache = nil
	}
	p.mu.Unlock()

	// Additional wait to ensure all goroutines have exited
	// This is a workaround for Pebble's internal goroutines that may not exit immediately
	// Use a shorter sleep time to reduce test duration
	time.Sleep(5 * time.Millisecond)

	// Return any error from flush or close operations
	// Prioritize flush errors over close errors
	if flushErr != nil {
		return fmt.Errorf("flush error: %w", flushErr)
	}
	return err
}