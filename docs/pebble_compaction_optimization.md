# PebbleDB Compaction Optimization for PostgreSQL-over-PebbleDB

## Overview

This document describes the optimization strategies implemented for PebbleDB compaction in the PostgreSQL-over-PebbleDB project. The optimizations focus on reducing write amplification, improving read performance, and maintaining consistent performance under mixed workloads.

## Key Optimizations

### 1. Configuration Parameters

#### Memory Management
- **CacheSize**: Increased to 2GB for production to improve read performance
- **MemTableSize**: Set to 128MB for production to reduce flush frequency
- **MaxOpenFiles**: Increased to 100,000 to handle large databases

#### Compaction Tuning
- **L0CompactionThreshold**: Set to 8 to reduce write amplification
- **L0StopWritesThreshold**: Set to 32 to prevent write stalls
- **LBaseMaxBytes**: Set to 128MB for better space efficiency
- **CompactionConcurrency**: Set to 16 for better parallelism

#### Performance Features
- **Rate Limiting**: Enabled with 100MB/s limit to prevent resource exhaustion
- **Bloom Filters**: Enabled with 10 bits per key for better read performance
- **Compression**: Enabled with Snappy for space efficiency

### 2. PostgreSQL-Optimized Configuration

A specialized configuration (`PostgreSQLOptimizedPebbleConfig`) is provided for PostgreSQL-like workloads:
- Balanced cache size (1GB)
- Moderate memtable size (64MB)
- Conservative rate limiting (50MB/s)
- Optimized file sizes for mixed read/write patterns

### 3. Monitoring and Metrics

Enhanced metrics collection provides insights into:
- Read/Write/Space amplification factors
- Level-specific file counts
- Compaction throughput
- Pending write operations

## Performance Testing

Benchmark tests are included to compare different configurations:
- `BenchmarkPebbleCompaction_DefaultConfig`: Tests with default settings
- `BenchmarkPebbleCompaction_PostgreSQLConfig`: Tests with PostgreSQL-optimized settings

## Expected Improvements

### Write Performance
- **Reduced Write Amplification**: 20-30% reduction through optimized L0 thresholds
- **Consistent Throughput**: Rate limiting prevents performance spikes
- **Lower Latency Variance**: Better compaction scheduling reduces latency outliers

### Read Performance
- **Improved Cache Hit Rates**: Larger cache improves read performance
- **Faster Point Lookups**: Bloom filters reduce disk I/O for non-existent keys
- **Better Sequential Reads**: Optimized block and file sizes

### Space Efficiency
- **Reduced Storage Overhead**: Compression and optimized compaction reduce space amplification
- **Efficient Garbage Collection**: Better compaction scheduling improves space reclamation

## Testing Methodology

### Benchmark Tests
Run the compaction benchmarks:
```bash
go test -v ./storage/internal/kv -run=NONE -bench=BenchmarkPebbleCompaction
```

### Manual Testing
1. Start the compaction monitor to observe real-time performance
2. Run mixed read/write workloads similar to PostgreSQL operations
3. Monitor amplification factors and file count distributions

## Tuning Guidelines

### For Write-Heavy Workloads
- Increase `MemTableSize` to 256MB
- Increase `L0CompactionThreshold` to 12
- Decrease `CompactionConcurrency` to reduce CPU usage

### For Read-Heavy Workloads
- Increase `CacheSize` to 4GB
- Enable more aggressive bloom filters
- Optimize block sizes for sequential reads

### For Mixed Workloads
- Use the `PostgreSQLOptimizedPebbleConfig`
- Monitor amplification factors and adjust thresholds accordingly
- Balance rate limiting based on system capacity

## Monitoring Metrics

Key metrics to monitor:
- **Read Amplification**: Should remain < 10 for good performance
- **Write Amplification**: Should remain < 10 for SSD longevity
- **Space Amplification**: Should remain < 1.5 for space efficiency
- **L0 File Count**: Should stay below `L0StopWritesThreshold`
- **Pending Writes**: Should remain low under normal operation

## Future Enhancements

Planned improvements:
1. Adaptive compaction thresholds based on workload analysis
2. Custom merge operators for PostgreSQL-specific data types
3. Advanced rate limiting with dynamic adjustment
4. Integration with PostgreSQL's vacuum mechanism
5. Enhanced compression strategies for different data types