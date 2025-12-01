package kv

import (
	"time"

	"github.com/cockroachdb/pebble/vfs"
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
	FS                    vfs.FS // Optional filesystem (useful for testing with in-memory FS)
	DisableWAL            bool   // Disable write-ahead log for testing
	DisableAutoCompaction bool   // Disable automatic compactions for testing
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
// Also minimizes background goroutines to prevent test hangs by using in-memory filesystem when path is empty
//
// When path is empty, uses an in-memory filesystem to avoid disk I/O and background goroutines
// that can cause tests to hang. This is particularly important because Pebble's default
// behavior (when FS is nil) wraps the filesystem with disk health checking, which creates
// background goroutines that continue running even after db.Close() is called.
//
// For benchmarks and production use, provide a non-empty path to use the file system.
func TestOptimizedPebbleConfig(path string) *PebbleConfig {
	config := &PebbleConfig{
		Path:                  path,
		CacheSize:             256 * 1024,              // Reduce to 256KB for testing to minimize memory
		MemTableSize:          64 * 1024,               // Reduce to 64KB for faster flushes
		MaxOpenFiles:          5,                       // Minimal file handles for testing
		CompactionConcurrency: 1,                       // Single-threaded compactions for predictability
		FlushInterval:         100 * time.Millisecond,  // Less aggressive flushing to reduce overhead
		BlockSize:             128,                     // Reduce to 128 bytes for testing
		L0CompactionThreshold: 1,                       // Immediate compaction triggering
		L0StopWritesThreshold: 2,                       // Minimal write stall prevention
		LBaseMaxBytes:         256 * 1024,              // 256KB for L1 in testing
		CompressionEnabled:    false,                   // Disable compression for speed
		EnableRateLimiting:    false,                   // Disable rate limiting for testing
		EnableBloomFilter:     false,                   // Disable bloom filters for testing
		BloomFilterBitsPerKey: 0,                       // Disable bloom filters completely
		TargetFileSize:        64 * 1024,               // 64KB target file size for testing
		MaxManifestFileSize:   256 * 1024,              // 256KB max manifest file size for testing
		DisableWAL:            false,                   // Don't disable WAL as it prevents writes
		DisableAutoCompaction: true,                    // Disable automatic compactions for testing to reduce background goroutines
	}
	
	// Use in-memory filesystem for tests when path is empty to avoid disk I/O and background goroutines
	if path == "" {
		config.FS = vfs.NewMem()
	}
	
	return config
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
		CacheSize:             4 * 1024 * 1024 * 1024, // Increase to 4GB cache for better read performance
		MemTableSize:          256 * 1024 * 1024,       // Increase to 256MB memtable for better batching
		MaxOpenFiles:          50000,                   // Increase file handles
		CompactionConcurrency: 32,                      // Maximize parallelism
		FlushInterval:         10 * time.Millisecond,   // Aggressive flushing
		BlockSize:             128 << 10,               // 128KB blocks
		L0CompactionThreshold: 2,                       // Trigger early compaction
		L0StopWritesThreshold: 40,                      // Prevent write stalls
		LBaseMaxBytes:         1024 << 20,              // 1GB for better space efficiency
		CompressionEnabled:    true,
		EnableRateLimiting:    false,
		EnableBloomFilter:     true,
		BloomFilterBitsPerKey: 15,                      // Better filtering
		TargetFileSize:        64 << 20,                // 64MB files
		MaxManifestFileSize:   1024 << 20,              // 1GB manifest
	}
}