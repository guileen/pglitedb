package kv

import (
	"context"
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
	activeTransactions *sync.Map  // Changed from map[uint64]*PebbleTransaction to *sync.Map
	nextTxnID          uint64
	
	// Compaction monitor for performance tracking
	compactionMonitor *CompactionMonitor
}

type PebbleConfig struct {
	Path                  string
	CacheSize             int64
	MemTableSize          int
	MaxOpenFiles          int
	CompactionConcurrency int
	FlushInterval         time.Duration
	BlockSize             int           // Block size for SSTable blocks
	L0CompactionThreshold int           // Number of L0 files to trigger compaction
	L0StopWritesThreshold int           // Number of L0 files to stop writes
	LBaseMaxBytes         int64         // Maximum size of L1 level in bytes
	CompressionEnabled    bool          // Enable Snappy compression
	EnableRateLimiting    bool          // Enable write rate limiting
	RateLimitBytesPerSec  int64         // Rate limit in bytes per second
	EnableBloomFilter     bool          // Enable bloom filters for better read performance
	BloomFilterBitsPerKey float64       // Bloom filter bits per key
	TargetFileSize        int64         // Target file size for SSTables
	MaxManifestFileSize   int64         // Maximum manifest file size
}

// DefaultPebbleConfig creates a default configuration for Pebble KV store optimized for production
func DefaultPebbleConfig(path string) *PebbleConfig {
	return &PebbleConfig{
		Path:                  path,
		CacheSize:             2 * 1024 * 1024 * 1024, // Increase to 2GB for better read performance
		MemTableSize:          128 * 1024 * 1024,       // Increase to 128MB for write-heavy workloads
		MaxOpenFiles:          100000,                  // Increase further
		CompactionConcurrency: 16,                      // Increase for better parallelism
		FlushInterval:         5 * time.Second,         // More aggressive flushing
		BlockSize:             64 << 10,                // Increase to 64KB for better performance
		L0CompactionThreshold: 8,                       // Increase to reduce write amplification
		L0StopWritesThreshold: 32,                      // Increase to prevent write stalls
		LBaseMaxBytes:         128 << 20,               // 128MB for L1, better space efficiency
		CompressionEnabled:    true,
		EnableRateLimiting:    true,                    // Enable rate limiting for consistent performance
		RateLimitBytesPerSec:  100 << 20,               // 100MB/s rate limit to prevent resource exhaustion
		EnableBloomFilter:     true,                    // Enable bloom filters for better read performance
		BloomFilterBitsPerKey: 10,                      // 10 bits per key for good balance
		TargetFileSize:        32 << 20,                // 32MB target file size for better sequential reads
		MaxManifestFileSize:   128 << 20,               // 128MB max manifest file size
	}
}

// TestOptimizedPebbleConfig creates a configuration optimized for testing performance
// Reduces memory usage and disables compression for faster operations
func TestOptimizedPebbleConfig(path string) *PebbleConfig {
	return &PebbleConfig{
		Path:                  path,
		CacheSize:             32 * 1024 * 1024,        // Reduce to 32MB for testing
		MemTableSize:          4 * 1024 * 1024,         // Reduce to 4MB for faster flushes
		MaxOpenFiles:          1000,                    // Reduce for testing
		CompactionConcurrency: 2,                       // Reduce for testing
		FlushInterval:         100 * time.Millisecond,  // Much faster flushing
		BlockSize:             4 << 10,                 // Reduce to 4KB for testing
		L0CompactionThreshold: 2,                       // Reduce to trigger compactions sooner
		L0StopWritesThreshold: 10,                      // Reduce to prevent write stalls
		LBaseMaxBytes:         32 << 20,                // 32MB for L1 in testing
		CompressionEnabled:    false,                   // Disable compression for speed
		EnableRateLimiting:    false,                   // Disable rate limiting for testing
		EnableBloomFilter:     true,                    // Keep bloom filters for testing
		BloomFilterBitsPerKey: 5,                       // Lower bits per key for testing
		TargetFileSize:        8 << 20,                // 8MB target file size for testing
		MaxManifestFileSize:   32 << 20,                // 32MB max manifest file size for testing
	}
}

// SpaceOptimizedPebbleConfig creates a configuration optimized for space efficiency
// Balances read/write performance with minimal space amplification
func SpaceOptimizedPebbleConfig(path string) *PebbleConfig {
	return &PebbleConfig{
		Path:                  path,
		CacheSize:             256 * 1024 * 1024,         // 256MB cache for lower memory usage
		MemTableSize:          16 * 1024 * 1024,          // 16MB memtable for more frequent flushes
		MaxOpenFiles:          10000,                     // Reduced file descriptor usage
		CompactionConcurrency: 2,                         // Lower concurrency for better space efficiency
		FlushInterval:         500 * time.Millisecond,    // More frequent flushing
		BlockSize:             16 << 10,                  // 16KB block size for better compression
		L0CompactionThreshold: 2,                         // Lower threshold for more frequent compactions
		L0StopWritesThreshold: 8,                         // Prevent write stalls
		LBaseMaxBytes:         256 << 20,                 // 256MB for L1 (increased for better space efficiency)
		CompressionEnabled:    true,                      // Enable compression for space efficiency
		EnableRateLimiting:    true,                      // Enable rate limiting for consistent performance
		RateLimitBytesPerSec:  10 << 20,                  // 10MB/s rate limit for better space efficiency
		EnableBloomFilter:     true,                      // Enable bloom filters for better read performance
		BloomFilterBitsPerKey: 12,                        // 12 bits per key for better filtering
		TargetFileSize:        4 << 20,                   // 4MB target file size (smaller for better space efficiency)
		MaxManifestFileSize:   16 << 20,                  // 16MB max manifest file size
	}
}

// PostgreSQLOptimizedPebbleConfig creates a configuration optimized for PostgreSQL-like workloads
// Balances read/write performance with space efficiency
func PostgreSQLOptimizedPebbleConfig(path string) *PebbleConfig {
	return &PebbleConfig{
		Path:                  path,
		CacheSize:             2 * 1024 * 1024 * 1024, // Increase to 2GB for better read performance
		MemTableSize:          128 * 1024 * 1024,       // Increase to 128MB for write-heavy workloads
		MaxOpenFiles:          100000,                  // Increase further for better file handling
		CompactionConcurrency: 16,                      // Increase for better parallelism
		FlushInterval:         1 * time.Second,         // More aggressive flushing for lower latency
		BlockSize:             64 << 10,                // Increase to 64KB for better performance
		L0CompactionThreshold: 8,                       // Increase to reduce write amplification
		L0StopWritesThreshold: 32,                      // Increase to prevent write stalls
		LBaseMaxBytes:         256 << 20,               // 256MB for L1, better space efficiency
		CompressionEnabled:    true,                    // Enable compression for space efficiency
		EnableRateLimiting:    true,                    // Enable rate limiting for consistent performance
		RateLimitBytesPerSec:  100 << 20,               // 100MB/s rate limit to prevent resource exhaustion
		EnableBloomFilter:     true,                    // Enable bloom filters for better read performance
		BloomFilterBitsPerKey: 12,                      // 12 bits per key for better filtering
		TargetFileSize:        32 << 20,                // 32MB target file size for better sequential reads
		MaxManifestFileSize:   128 << 20,               // 128MB max manifest file size
	}
}

func NewPebbleKV(config *PebbleConfig) (*PebbleKV, error) {
	cache := pebble.NewCache(config.CacheSize)
	defer cache.Unref()
	
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
	
	opts := &pebble.Options{
		Cache: cache,
		MaxOpenFiles:   config.MaxOpenFiles,
		MemTableSize:   uint64(config.MemTableSize),
		MemTableStopWritesThreshold: 8,  // Increased threshold to reduce write stalls
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
	return p.SetWithOptions(ctx, key, value, shared.DefaultWriteOptions)
}

func (p *PebbleKV) SetWithOptions(ctx context.Context, key, value []byte, opts *shared.WriteOptions) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return shared.ErrClosed
	}

	writeOpts := pebble.NoSync
	if opts != nil && opts.Sync {
		writeOpts = pebble.Sync
	} else {
		atomic.AddInt64(&p.pendingWrites, 1)
	}

	if err := p.db.Set(key, value, writeOpts); err != nil {
		return fmt.Errorf("pebble set: %w", err)
	}
	return nil
}

func (p *PebbleKV) Delete(ctx context.Context, key []byte) error {
	return p.DeleteWithOptions(ctx, key, shared.DefaultWriteOptions)
}

func (p *PebbleKV) DeleteWithOptions(ctx context.Context, key []byte, opts *shared.WriteOptions) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return shared.ErrClosed
	}

	writeOpts := pebble.NoSync
	if opts != nil && opts.Sync {
		writeOpts = pebble.Sync
	} else {
		atomic.AddInt64(&p.pendingWrites, 1)
	}

	if err := p.db.Delete(key, writeOpts); err != nil {
		return fmt.Errorf("pebble delete: %w", err)
	}
	return nil
}

func (p *PebbleKV) NewBatch() shared.Batch {
	return &PebbleBatch{
		batch: p.db.NewBatch(),
	}
}

func (p *PebbleKV) Commit(ctx context.Context, batch shared.Batch) error {
	return p.CommitBatch(ctx, batch)
}

func (p *PebbleKV) CommitBatch(ctx context.Context, batch shared.Batch) error {
	return p.CommitBatchWithOptions(ctx, batch, shared.DefaultWriteOptions)
}

func (p *PebbleKV) CommitBatchWithOptions(ctx context.Context, batch shared.Batch, opts *shared.WriteOptions) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return shared.ErrClosed
	}

	pb, ok := batch.(*PebbleBatch)
	if !ok {
		return fmt.Errorf("invalid batch type")
	}

	writeOpts := pebble.NoSync
	if opts != nil && (opts.Sync || opts.Durability == shared.DurabilityImmediate) {
		writeOpts = pebble.Sync
	} else {
		atomic.AddInt64(&p.pendingWrites, int64(pb.batch.Count()))
	}

	if err := pb.batch.Commit(writeOpts); err != nil {
		return fmt.Errorf("pebble commit batch: %w", err)
	}
	return nil
}

func (p *PebbleKV) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
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
	if iter == nil {
		return nil
	}
	
	return &PebbleIterator{
		iter:    iter,
		reverse: opts != nil && opts.Reverse,
		err:     err,
	}
}

func (p *PebbleKV) NewSnapshot() (shared.Snapshot, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, shared.ErrClosed
	}

	snapshot := p.db.NewSnapshot()
	return &PebbleSnapshot{
		snapshot: snapshot,
	}, nil
}

func (p *PebbleKV) NewTransaction(ctx context.Context) (shared.Transaction, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, shared.ErrClosed
	}

	txnID := atomic.AddUint64(&p.nextTxnID, 1) - 1

	txn := &PebbleTransaction{
		db:        p.db,
		batch:     p.db.NewBatch(),
		isolation: shared.ReadCommitted,
		txnID:     txnID,
		kv:        p,
		writeKeys: make(map[string]bool),
		startTS:   p.globalTS.Load(),
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

func (p *PebbleKV) Stats() shared.KVStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return shared.KVStats{}
	}

	metrics := p.db.Metrics()
	
	// Calculate total key count approximation
	var keyCount int64
	for _, level := range metrics.Levels {
		keyCount += int64(level.NumFiles)
	}
	
	// Calculate amplification factors
	var readAmp int64 = 0
	var totalFiles int64 = 0
	for i, level := range metrics.Levels {
		if i < len(metrics.Levels)-1 { // Don't count the last level for read amplification
			readAmp += int64(level.Sublevels)
		}
		totalFiles += int64(level.NumFiles)
	}
	
	// Write amplification calculation (approximate)
	var writeAmp float64 = 1.0
	var totalBytesFlushed, totalBytesCompacted uint64
	for _, level := range metrics.Levels {
		totalBytesFlushed += level.BytesFlushed
		totalBytesCompacted += level.BytesCompacted
	}
	if metrics.Flush.Count > 0 || totalBytesFlushed > 0 || totalBytesCompacted > 0 {
		// Simplified write amplification calculation
		writeAmp = float64(totalBytesFlushed+totalBytesCompacted) / float64(totalBytesFlushed+1)
	}
	
	// Space amplification calculation
	var spaceAmp float64 = 1.0
	if metrics.Total().Size > 0 {
		spaceAmp = float64(metrics.DiskSpaceUsage()) / float64(metrics.Total().Size)
	}
	
	return shared.KVStats{
		KeyCount:        keyCount,
		ApproximateSize: int64(metrics.DiskSpaceUsage()),
		MemTableSize:    int64(metrics.MemTable.Size),
		FlushCount:      int64(metrics.Flush.Count),
		CompactionCount: int64(metrics.Compact.Count),
		PendingWrites:   atomic.LoadInt64(&p.pendingWrites),
		
		// Compaction-specific statistics
		L0FileCount:     int64(metrics.Levels[0].NumFiles),
		L1FileCount:     int64(metrics.Levels[1].NumFiles),
		L2FileCount:     int64(metrics.Levels[2].NumFiles),
		ReadAmplification: readAmp,
		WriteAmplification: writeAmp,
		SpaceAmplification: spaceAmp,
		CompactionBytesWritten: int64(totalBytesCompacted),
	}
}

func (p *PebbleKV) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return shared.ErrClosed
	}

	if err := p.db.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}

	atomic.StoreInt64(&p.pendingWrites, 0)
	return nil
}

func (p *PebbleKV) backgroundFlush() {
	for {
		select {
		case <-p.flushTicker.C:
			if atomic.LoadInt64(&p.pendingWrites) > 0 {
				p.Flush()
			}
		case <-p.flushDone:
			return
		}
	}
}

func (p *PebbleKV) CheckForConflicts(txn shared.Transaction, key []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return shared.ErrClosed
	}

	// Get the transaction ID
	ptxn, ok := txn.(*PebbleTransaction)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}

	// For Read Committed isolation, check if there are any uncommitted
	// writes to this key from other transactions
	var conflictErr error
	p.activeTransactions.Range(func(txnID, value interface{}) bool {
		id := txnID.(uint64)
		otherTxn := value.(*PebbleTransaction)
		
		// Skip the current transaction
		if id == ptxn.txnID {
			return true
		}

		// Check if the other transaction has written to this key
		otherTxn.mu.RLock()
		_, written := otherTxn.writeKeys[string(key)]
		otherTxn.mu.RUnlock()

		if written {
			conflictErr = shared.ErrConflict
			return false // Stop iteration
		}
		return true // Continue iteration
	})

	return conflictErr
}

func (p *PebbleKV) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	// Stop the compaction monitor
	if p.compactionMonitor != nil {
		p.compactionMonitor.Stop()
	}

	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	if p.flushDone != nil {
		close(p.flushDone)
	}

	if atomic.LoadInt64(&p.pendingWrites) > 0 {
		p.db.Flush()
	}

	if err := p.db.Close(); err != nil {
		return fmt.Errorf("pebble close: %w", err)
	}
	return nil
}

type PebbleBatch struct {
	batch *pebble.Batch
}

func (b *PebbleBatch) Set(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *PebbleBatch) Delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *PebbleBatch) Count() int {
	return int(b.batch.Count())
}

func (b *PebbleBatch) Reset() {
	b.batch.Reset()
}

func (b *PebbleBatch) Close() error {
	return b.batch.Close()
}

type PebbleIterator struct {
	iter    *pebble.Iterator
	reverse bool
	err     error
}

func (i *PebbleIterator) Valid() bool {
	if i == nil || i.iter == nil {
		return false
	}
	return i.iter.Valid()
}

func (i *PebbleIterator) Next() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Prev()
	}
	return i.iter.Next()
}

func (i *PebbleIterator) Prev() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Next()
	}
	return i.iter.Prev()
}

func (i *PebbleIterator) Key() []byte {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Key()
}

func (i *PebbleIterator) Value() []byte {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Value()
}

func (i *PebbleIterator) Error() error {
	if i == nil {
		return nil
	}
	if i.err != nil {
		return i.err
	}
	if i.iter == nil {
		return nil
	}
	return i.iter.Error()
}

func (i *PebbleIterator) SeekGE(key []byte) bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.SeekLT(key)
	}
	return i.iter.SeekGE(key)
}

func (i *PebbleIterator) SeekLT(key []byte) bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.SeekGE(key)
	}
	return i.iter.SeekLT(key)
}

func (i *PebbleIterator) First() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Last()
	}
	return i.iter.First()
}

func (i *PebbleIterator) Last() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.First()
	}
	return i.iter.Last()
}

func (i *PebbleIterator) Close() error {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Close()
}

type PebbleSnapshot struct {
	snapshot *pebble.Snapshot
}

func (s *PebbleSnapshot) Get(key []byte) ([]byte, error) {
	value, closer, err := s.snapshot.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, shared.ErrNotFound
		}
		return nil, fmt.Errorf("pebble snapshot get: %w", err)
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (s *PebbleSnapshot) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
	var pebbleOpts *pebble.IterOptions
	if opts != nil {
		pebbleOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	iter, err := s.snapshot.NewIter(pebbleOpts)
	if err != nil {
		return nil
	}
	if iter == nil {
		return nil
	}
	
	return &PebbleIterator{
		iter:    iter,
		reverse: opts != nil && opts.Reverse,
		err:     err,
	}
}

func (s *PebbleSnapshot) Close() error {
	return s.snapshot.Close()
}

type PebbleTransaction struct {
	db         *pebble.DB
	batch      *pebble.Batch
	mu         sync.RWMutex
	closed     bool
	readKeys   map[string][]byte
	isolation  shared.IsolationLevel
	txnID      uint64
	kv         *PebbleKV
	writeKeys  map[string]bool
	
	startTS    int64
	commitTS   int64
	readSet    map[string]int64
}

func (t *PebbleTransaction) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, shared.ErrClosed
	}

	// For ReadUncommitted isolation, we can read uncommitted changes
	// First check if this key was written in this transaction
	value, closer, err := t.batch.Get(key)
	if err == nil {
		defer closer.Close()
		result := make([]byte, len(value))
		copy(result, value)
		
		if t.readKeys == nil {
			t.readKeys = make(map[string][]byte)
		}
		t.readKeys[string(key)] = result
		
		return result, nil
	}

	// For all isolation levels, check the database for committed data
	// This ensures visibility of data inserted via engine.InsertRow
	value, closer, err = t.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, shared.ErrNotFound
		}
		return nil, fmt.Errorf("transaction get: %w", err)
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)

	if t.readKeys == nil {
		t.readKeys = make(map[string][]byte)
	}
	t.readKeys[string(key)] = result

	if t.isolation == shared.Serializable {
		if t.readSet == nil {
			t.readSet = make(map[string]int64)
		}
		t.readSet[string(key)] = t.kv.getKeyTimestamp(key)
	}

	return result, nil
}

func (t *PebbleTransaction) Set(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	// Track the written key
	t.writeKeys[string(key)] = true

	return t.batch.Set(key, value, nil)
}

func (t *PebbleTransaction) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	// Track the written key
	t.writeKeys[string(key)] = true

	return t.batch.Delete(key, nil)
}

func (t *PebbleTransaction) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
	var pebbleOpts *pebble.IterOptions
	if opts != nil {
		pebbleOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	// Create an iterator that combines both the batch and the database
	// This is a simplified approach - in a real implementation, we would need
	// to merge the iterators properly to handle conflicts
	
	// For now, if the batch is empty, just use the database iterator
	if t.batch.Count() == 0 {
		iter, err := t.db.NewIter(pebbleOpts)
		if err != nil {
			return nil
		}
		if iter == nil {
			return nil
		}
		
		return &PebbleIterator{
			iter:    iter,
			reverse: opts != nil && opts.Reverse,
			err:     err,
		}
	}

	// If the batch has operations, we need to handle this more carefully
	// For now, we'll use the batch iterator, but note that this might not
	// include all the data that exists in the database
	iter, err := t.batch.NewIter(pebbleOpts)
	if err != nil {
		return nil
	}
	if iter == nil {
		return nil
	}
	
	return &PebbleIterator{
		iter:    iter,
		reverse: opts != nil && opts.Reverse,
		err:     err,
	}
}

func (t *PebbleTransaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	if t.isolation == shared.Serializable {
		for key, readTS := range t.readSet {
			currentTS := t.kv.getKeyTimestamp([]byte(key))
			if currentTS > readTS {
				t.closed = true
				t.kv.activeTransactions.Delete(t.txnID)
				t.batch.Close()
				return shared.ErrConflict
			}
		}
	}

	t.commitTS = t.kv.allocateTimestamp()

	for key := range t.writeKeys {
		t.kv.setKeyTimestamp([]byte(key), t.commitTS)
	}

	t.closed = true
	
	t.kv.activeTransactions.Delete(t.txnID)
	
	if err := t.batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("transaction commit: %w", err)
	}
	return nil
}

func (t *PebbleTransaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true
	
	// Unregister the transaction
	t.kv.activeTransactions.Delete(t.txnID)
	
	return t.batch.Close()
}

func (t *PebbleTransaction) Isolation() shared.IsolationLevel {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.isolation
}

func (t *PebbleTransaction) SetIsolation(level shared.IsolationLevel) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if t.closed {
		return shared.ErrClosed
	}
	
	// Validate the isolation level
	switch level {
	case shared.ReadUncommitted, shared.ReadCommitted, shared.RepeatableRead, shared.SnapshotIsolation, shared.Serializable:
		t.isolation = level
		return nil
	default:
		return fmt.Errorf("invalid isolation level: %d", level)
	}
}

func (t *PebbleTransaction) TxnID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.txnID
}

func (t *PebbleTransaction) Close() error {
	return t.Rollback()
}