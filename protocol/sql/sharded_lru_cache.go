package sql

import (
	"container/list"
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ShardedLRUCache implements a thread-safe sharded LRU cache with expiration support
// This reduces lock contention by distributing entries across multiple shards
type ShardedLRUCache struct {
	shards     []*shard
	numShards  int
	expiration time.Duration
	
	// Global atomic counters for better performance monitoring
	hits   int64
	misses int64
}

// shard represents a single shard of the cache with its own mutex
type shard struct {
	capacity  int64
	cache     map[string]*list.Element
	evictList *list.List
	mutex     sync.RWMutex
}

// shardedCacheEntry represents a cached item in sharded cache
type shardedCacheEntry struct {
	key       string
	value     interface{}
	timestamp time.Time
}

// NewShardedLRUCache creates a new sharded LRU cache with the specified capacity
// The capacity is distributed evenly across shards
func NewShardedLRUCache(capacity int) *ShardedLRUCache {
	return NewShardedLRUCacheWithExpiration(capacity, 0)
}

// nextPowerOfTwo returns the next power of two for a given number
func nextPowerOfTwo(v int) int {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

// NewShardedLRUCacheWithExpiration creates a new sharded LRU cache with expiration
func NewShardedLRUCacheWithExpiration(capacity int, expiration time.Duration) *ShardedLRUCache {
	// Dynamically determine optimal number of shards based on CPU cores
	numShards := runtime.NumCPU() * 4
	if numShards < 16 {
		numShards = 16
	}
	if numShards > 256 {
		numShards = 256
	}
	
	// Ensure numShards is a power of two for efficient hashing
	numShards = nextPowerOfTwo(numShards)
	
	shardCapacity := capacity / numShards
	if shardCapacity == 0 {
		shardCapacity = 1
	}
	
	shards := make([]*shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &shard{
			capacity:  int64(shardCapacity),
			cache:     make(map[string]*list.Element),
			evictList: list.New(),
		}
	}
	
	return &ShardedLRUCache{
		shards:     shards,
		numShards:  numShards,
		expiration: expiration,
	}
}

// getShard returns the shard responsible for the given key
func (c *ShardedLRUCache) getShard(key string) *shard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()
	return c.shards[hash%uint32(c.numShards)]
}

// Get retrieves a value from the cache
func (c *ShardedLRUCache) Get(key string) (interface{}, bool) {
	shard := c.getShard(key)
	
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	
	if element, exists := shard.cache[key]; exists {
		entry := element.Value.(*shardedCacheEntry)
		
		// Check if entry has expired
		if c.expiration > 0 && time.Since(entry.timestamp) > c.expiration {
			// Entry expired, remove it
			c.removeElement(shard, element)
			atomic.AddInt64(&c.misses, 1)
			return nil, false
		}
		
		// Move to front (most recently used)
		shard.evictList.MoveToFront(element)
		value := entry.value
		atomic.AddInt64(&c.hits, 1)
		return value, true
	}
	
	atomic.AddInt64(&c.misses, 1)
	return nil, false
}

// Put adds a value to the cache
func (c *ShardedLRUCache) Put(key string, value interface{}) {
	shard := c.getShard(key)
	
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	
	// Check if key already exists
	if element, exists := shard.cache[key]; exists {
		// Update existing entry
		shard.evictList.MoveToFront(element)
		entry := element.Value.(*shardedCacheEntry)
		entry.value = value
		entry.timestamp = time.Now()
		return
	}
	
	// Add new entry
	entry := &shardedCacheEntry{
		key:       key,
		value:     value,
		timestamp: time.Now(),
	}
	element := shard.evictList.PushFront(entry)
	shard.cache[key] = element
	
	// Evict oldest if necessary
	if int64(shard.evictList.Len()) > shard.capacity {
		c.evictOldest(shard)
	}
}

// Remove removes a key from the cache
func (c *ShardedLRUCache) Remove(key string) {
	shard := c.getShard(key)
	
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	
	if element, exists := shard.cache[key]; exists {
		c.removeElement(shard, element)
	}
}

// Len returns the number of items in the cache
func (c *ShardedLRUCache) Len() int {
	totalLen := 0
	for _, shard := range c.shards {
		shard.mutex.RLock()
		totalLen += shard.evictList.Len()
		shard.mutex.RUnlock()
	}
	return totalLen
}

// Clear removes all items from the cache
func (c *ShardedLRUCache) Clear() {
	for _, shard := range c.shards {
		shard.mutex.Lock()
		shard.cache = make(map[string]*list.Element)
		shard.evictList.Init()
		shard.mutex.Unlock()
	}
	
	// Reset stats
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Stats returns cache hit/miss statistics
func (c *ShardedLRUCache) Stats() (hits, misses int64) {
	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses)
}

// HitRate returns the cache hit rate as a percentage
func (c *ShardedLRUCache) HitRate() float64 {
	hits, misses := c.Stats()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total) * 100
}

// ResetStats resets the cache statistics
func (c *ShardedLRUCache) ResetStats() {
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// evictOldest removes the oldest entry from the shard
func (c *ShardedLRUCache) evictOldest(s *shard) {
	element := s.evictList.Back()
	if element != nil {
		c.removeElement(s, element)
	}
}

// removeElement removes a specific element from the shard
func (c *ShardedLRUCache) removeElement(s *shard, element *list.Element) {
	s.evictList.Remove(element)
	entry := element.Value.(*shardedCacheEntry)
	delete(s.cache, entry.key)
}