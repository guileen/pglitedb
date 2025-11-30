package kv

import (
	"time"
)

// PebbleConfig holds configuration options for the Pebble KV store
type PebbleConfig struct {
	Path                  string
	CacheSize             int64
	MemTableSize          int
	MaxOpenFiles          int
	CompactionConcurrency int
	FlushInterval         time.Duration
	BlockSize             int
	L0CompactionThreshold int
	L0StopWritesThreshold int
	LBaseMaxBytes         int64
	CompressionEnabled    bool
	EnableRateLimiting    bool
	RateLimitBytesPerSec  int64
	EnableBloomFilter     bool
	BloomFilterBitsPerKey int
	TargetFileSize        int64
	MaxManifestFileSize   int64
}

// DefaultPebbleConfig creates a default configuration for Pebble KV store optimized for production
func DefaultPebbleConfig(path string) *PebbleConfig {
	return &PebbleConfig{
		Path:                  path,
		CacheSize:             512 * 1024 * 1024,       // 512MB cache for balanced read performance
		MemTableSize:          64 * 1024 * 1024,        // 64MB memtable for balanced write performance
		MaxOpenFiles:          5000,                    // Balanced file handle limit
		CompactionConcurrency: 8,                       // Balanced parallelism
		FlushInterval:         500 * time.Millisecond,   // Balanced flushing interval
		BlockSize:             32 << 10,                // 32KB block size for better performance
		L0CompactionThreshold: 4,                       // Balanced threshold to reduce write amplification
		L0StopWritesThreshold: 12,                      // Balanced threshold to reduce write stalls
		LBaseMaxBytes:         64 << 20,                // 64MB for L1, balanced space efficiency
		CompressionEnabled:    true,
		EnableRateLimiting:    false,                   // Disable rate limiting for maximum performance
		RateLimitBytesPerSec:  50 << 20,                // 50MB/s rate limit if enabled
		EnableBloomFilter:     true,                    // Enable bloom filters for better read performance
		BloomFilterBitsPerKey: 10,                      // 10 bits per key for better filtering
		TargetFileSize:        16 << 20,                // 16MB target file size for better sequential reads
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
		MaxOpenFiles:          2000,                      // Further reduced file descriptor usage for safety
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
		CacheSize:             512 * 1024 * 1024,       // 512MB cache for balanced read performance
		MemTableSize:          64 * 1024 * 1024,        // 64MB memtable for balanced write performance
		MaxOpenFiles:          5000,                    // Balanced file handle limit
		CompactionConcurrency: 8,                       // Balanced parallelism
		FlushInterval:         500 * time.Millisecond,   // Balanced flushing interval
		BlockSize:             32 << 10,                // 32KB block size for better performance
		L0CompactionThreshold: 4,                       // Balanced threshold to reduce write amplification
		L0StopWritesThreshold: 12,                      // Balanced threshold to reduce write stalls
		LBaseMaxBytes:         64 << 20,                // 64MB for L1, balanced space efficiency
		CompressionEnabled:    true,
		EnableRateLimiting:    false,                   // Disable rate limiting for maximum performance
		RateLimitBytesPerSec:  50 << 20,                // 50MB/s rate limit if enabled
		EnableBloomFilter:     true,                    // Enable bloom filters for better read performance
		BloomFilterBitsPerKey: 10,                      // 10 bits per key for better filtering
		TargetFileSize:        16 << 20,                // 16MB target file size for better sequential reads
		MaxManifestFileSize:   128 << 20,               // 128MB max manifest file size
	}
}

// HighPerformancePebbleConfig creates a configuration optimized for maximum performance
// Prioritizes throughput and low latency over space efficiency
func HighPerformancePebbleConfig(path string) *PebbleConfig {
	return &PebbleConfig{
		Path:                  path,
		CacheSize:             1 * 1024 * 1024 * 1024, // Reduced to 1GB cache for better resource usage
		MemTableSize:          64 * 1024 * 1024,        // Reduced to 64MB memtable for balanced performance
		MaxOpenFiles:          10000,                   // Reduced file handle limit to prevent resource exhaustion
		CompactionConcurrency: 16,                      // Balanced parallelism for throughput
		FlushInterval:         100 * time.Millisecond,  // More aggressive flushing for lowest latency
		BlockSize:             32 << 10,                // 32KB block size for balanced performance
		L0CompactionThreshold: 4,                       // Balanced threshold for compaction triggering
		L0StopWritesThreshold: 12,                      // Reasonable threshold to prevent write stalls
		LBaseMaxBytes:         64 << 20,                // 64MB for L1, balanced space efficiency
		CompressionEnabled:    true,
		EnableRateLimiting:    false,                   // Disable rate limiting for maximum performance
		RateLimitBytesPerSec:  50 << 20,                // 50MB/s rate limit if enabled
		EnableBloomFilter:     true,                    // Enable bloom filters for better read performance
		BloomFilterBitsPerKey: 10,                      // 10 bits per key for good filtering
		TargetFileSize:        16 << 20,                // 16MB target file size for balanced sequential reads
		MaxManifestFileSize:   128 << 20,               // 128MB max manifest file size
	}
}