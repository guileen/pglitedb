# PostgreSQL-over-PebbleDB Performance Improvement Plan

## Current Performance Baseline

Based on benchmark results:
- UpdateRows: ~34-38ms/op, ~2.3MB/op, ~42,189 allocs/op
- DeleteRows: ~78-81ms/op, ~0.8MB/op, ~16,600 allocs/op
- IndexIterator: ~1.97ms/op, ~0.8MB/op, ~23,011 allocs/op

Target: 30% improvement (2,499.64 TPS → 3,245 TPS, 4.001ms → 3.2ms latency)

## 1. Problem Diagnosis

### Core Issues Identified:

1. **High Memory Allocations**: All operations show excessive allocations (>15,000 allocs/op)
2. **Inefficient Key Encoding**: Repeated key encoding/decoding without buffer reuse
3. **Suboptimal Resource Pooling**: Iterator and buffer pools not fully utilized
4. **MVCC Overhead**: Timestamp-based versioning adds significant overhead
5. **Transaction Management**: Complex transaction tracking creates bottlenecks

## 2. Optimization Solutions

### A. Key Encoding/Decoding Optimization

**Problem**: Repeated key encoding creates excessive allocations.

**Solution**: Implement buffer reuse for key encoding operations.

```go
// In codec/encoding.go, modify key encoding methods to use buffer pools:
func (c *memcodec) EncodeTableKeyBuffer(tenantID, tableID, rowID int64, buf []byte) ([]byte, error) {
    // Reuse buffer when possible to eliminate allocations
    if buf == nil {
        // Use tiered buffer pool instead of make([]byte, 0, 32)
        buf = c.bufferPool.Acquire(32)
    }
    buf = buf[:0]
    
    // ... encoding logic ...
    return buf, nil
}
```

### B. Resource Pool Enhancement

**Problem**: Current pooling is not fully optimized.

**Solution**: Expand and optimize resource pools:

```go
// In engine/pebble/resources/pools/manager.go
type Manager struct {
    // Add specialized pools for common operations
    tableKeyBufferPool   *sync.Pool  // For table key buffers
    indexKeyBufferPool   *sync.Pool  // For index key buffers
    rowBufferPool        *sync.Pool  // For row encoding/decoding
    filterBufferPool     *sync.Pool  // For filter evaluation
}
```

### C. MVCC Optimization

**Problem**: MVCC timestamp encoding/decoding overhead.

**Solution**: Implement timestamp caching and batch timestamp operations:

```go
// In storage/mvcc.go
type MVCCStorage struct {
    kv           shared.KV
    mu           sync.RWMutex
    timestamp    int64
    tsCache      *sync.Map  // Cache for frequently accessed timestamps
    batchTS      int64      // Batch timestamp for transactions
}
```

### D. Transaction Batch Processing

**Problem**: Individual transaction operations create overhead.

**Solution**: Implement batch processing for transaction operations:

```go
// In engine/pebble/transactions/manager.go
type TransactionBatch struct {
    operations []TransactionOperation
    batchSize  int
}

func (tb *TransactionBatch) AddOperation(op TransactionOperation) {
    tb.operations = append(tb.operations, op)
    if len(tb.operations) >= tb.batchSize {
        tb.Flush()
    }
}
```

### E. Iterator Optimization

**Problem**: Iterator creation and management is inefficient.

**Solution**: Implement iterator reuse with reset patterns:

```go
// In engine/pebble/operations/scan/row_iterator.go
func (ri *RowIterator) Reset(iter storage.Iterator, codec codec.Codec, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) {
    ri.iter = iter
    ri.codec = codec
    ri.schemaDef = schemaDef
    ri.opts = opts
    ri.count = 0
    ri.started = false
    ri.err = nil
    ri.current = nil
    // Retain pool reference for reuse
}
```

## 3. Implementation Roadmap

### Phase 1: Resource Pooling Enhancement (Week 1)
- Implement specialized buffer pools for key encoding
- Optimize existing pool usage patterns
- Add metrics for pool utilization tracking

### Phase 2: Key Encoding Optimization (Week 2)
- Implement buffer reuse for all key encoding operations
- Add benchmark tests for key encoding performance
- Optimize codec.EncodeTableKeyBuffer and related methods

### Phase 3: MVCC and Transaction Optimization (Week 3)
- Implement timestamp caching
- Add batch processing for transactions
- Optimize conflict detection algorithms

### Phase 4: Iterator and Scan Optimization (Week 4)
- Implement iterator reuse patterns
- Optimize filter evaluation
- Add scan result caching

## 4. Expected Performance Improvements

### Memory Allocation Reduction (40-50% reduction)
- Current: 42,189 allocs/op (UpdateRows)
- Target: 21,000-25,000 allocs/op

### Latency Reduction (30% improvement)
- Current: 34-38ms/op (UpdateRows)
- Target: 24-27ms/op

### Throughput Improvement (30% increase)
- Current: ~2,500 TPS
- Target: ~3,250 TPS

## 5. Risk Mitigation

### Data Consistency Risks
- Implement comprehensive regression tests
- Add MVCC consistency verification utilities
- Use atomic operations for timestamp management

### Performance Regression Risks
- Maintain benchmark baselines for all operations
- Implement performance regression testing in CI
- Monitor key metrics: allocations, latency, throughput

### Memory Leak Risks
- Enhance leak detection in resource manager
- Add pool validation and cleanup routines
- Implement reference counting for pooled resources

## 6. Monitoring and Validation

### Key Metrics to Track
1. **Allocations per operation**: Should decrease by 40-50%
2. **Latency percentiles**: 95th and 99th percentiles should improve
3. **Throughput**: TPS should increase by 30%
4. **Memory usage**: Overall memory footprint should stabilize
5. **Garbage collection pressure**: GC frequency should decrease

### Validation Methods
1. **Benchmark testing**: Run existing benchmarks with `-count=10`
2. **Regression testing**: Ensure all 228 tests still pass
3. **Stress testing**: Run high-concurrency workloads
4. **Memory profiling**: Use `go tool pprof` to verify allocation reduction

## 7. Code Changes Summary

### Files to Modify:
1. `codec/encoding.go` - Buffer reuse for key encoding
2. `engine/pebble/resources/pools/manager.go` - Enhanced pooling
3. `storage/mvcc.go` - Timestamp caching and batch operations
4. `engine/pebble/operations/scan/row_iterator.go` - Iterator reuse
5. `engine/pebble/transactions/manager.go` - Batch processing

### New Files to Create:
1. `engine/pebble/resources/pools/key_buffer_pools.go` - Specialized key buffer pools
2. `engine/pebble/transactions/batch.go` - Transaction batching utilities
3. `engine/pebble/metrics/optimization.go` - Optimization-specific metrics

## 8. Testing and Rollout Plan

### Pre-Implementation
- Run full benchmark suite and save results
- Run regression tests to establish baseline
- Profile memory allocations with `go tool pprof`

### Post-Implementation (Each Phase)
- Run benchmark suite and compare results
- Run regression tests to ensure correctness
- Profile memory usage and verify improvements
- Stress test with concurrent workloads

### Production Rollout
- Deploy to staging environment first
- Monitor key metrics for 48 hours
- Gradual rollout to production (25% → 50% → 100%)
- Continuous monitoring during rollout

## 9. Success Criteria

### Performance Metrics
- 30% improvement in TPS (2,500 → 3,250)
- 30% reduction in latency (4ms → 3.2ms)
- 40% reduction in memory allocations
- 25% reduction in GC pressure

### Quality Metrics
- 100% regression test pass rate
- No memory leaks detected
- No data consistency issues
- Improved resource utilization

This plan focuses on systematic optimization while maintaining data consistency and system reliability. Each phase delivers measurable improvements that build upon previous optimizations.