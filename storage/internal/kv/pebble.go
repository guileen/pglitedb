package kv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
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
	activeTransactions map[uint64]*PebbleTransaction
	transactionMu      sync.RWMutex
	nextTxnID          uint64
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
	CompressionEnabled    bool          // Enable Snappy compression
}

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
		CompressionEnabled:    true,
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
		LBaseMaxBytes:         128 << 20, // 128 MB for better space efficiency
		MaxConcurrentCompactions: func() int { return config.CompactionConcurrency },
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 8 << 20, BlockSize: config.BlockSize, Compression: compression[0]},   // 8 MB - increased block size for better sequential read performance
			{TargetFileSize: 32 << 20, BlockSize: config.BlockSize, Compression: compression[1]},  // 32 MB
			{TargetFileSize: 128 << 20, BlockSize: config.BlockSize, Compression: compression[2]}, // 128 MB
		},
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
		activeTransactions: make(map[uint64]*PebbleTransaction),
		nextTxnID:          1, // Start from 1 to avoid issues with ID 0
	}

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

	p.transactionMu.Lock()
	txnID := p.nextTxnID
	p.nextTxnID++
	p.transactionMu.Unlock()

	txn := &PebbleTransaction{
		db:        p.db,
		batch:     p.db.NewBatch(),
		isolation: shared.ReadCommitted,
		txnID:     txnID,
		kv:        p,
		writeKeys: make(map[string]bool),
		startTS:   p.globalTS.Load(),
	}

	p.transactionMu.Lock()
	p.activeTransactions[txnID] = txn
	p.transactionMu.Unlock()

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
	
	return shared.KVStats{
		KeyCount:        keyCount,
		ApproximateSize: int64(metrics.DiskSpaceUsage()),
		MemTableSize:    int64(metrics.MemTable.Size),
		FlushCount:      int64(metrics.Flush.Count),
		CompactionCount: int64(metrics.Compact.Count),
		PendingWrites:   atomic.LoadInt64(&p.pendingWrites),
	}
}

func (p *PebbleKV) Flush() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

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
	p.transactionMu.RLock()
	defer p.transactionMu.RUnlock()

	// Check if any other active transaction has written to this key
	for txnID, otherTxn := range p.activeTransactions {
		// Skip the current transaction
		if txnID == ptxn.txnID {
			continue
		}

		// Check if the other transaction has written to this key
		otherTxn.mu.RLock()
		_, written := otherTxn.writeKeys[string(key)]
		otherTxn.mu.RUnlock()

		if written {
			return shared.ErrConflict
		}
	}

	return nil
}

func (p *PebbleKV) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

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
				t.kv.transactionMu.Lock()
				delete(t.kv.activeTransactions, t.txnID)
				t.kv.transactionMu.Unlock()
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
	
	t.kv.transactionMu.Lock()
	delete(t.kv.activeTransactions, t.txnID)
	t.kv.transactionMu.Unlock()
	
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
	t.kv.transactionMu.Lock()
	delete(t.kv.activeTransactions, t.txnID)
	t.kv.transactionMu.Unlock()
	
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