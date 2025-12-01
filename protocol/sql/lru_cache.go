package sql

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// LRUCache implements a thread-safe LRU cache with expiration support
type LRUCache struct {
	capacity   int64
	cache      map[string]*list.Element
	evictList  *list.List
	mutex      sync.RWMutex
	expiration time.Duration
	
	// Atomic counters for better performance monitoring
	hits   int64
	misses int64
	
	// Active expiration
	expirationTicker *time.Ticker
	stopChan         chan struct{}
	activeExpiration bool
}

// cacheEntry represents a cached item
type cacheEntry struct {
	key        string
	value      interface{}
	timestamp  time.Time
	expiration time.Duration // Per-entry expiration, 0 means use default
}

// NewLRUCache creates a new LRU cache with the specified capacity
// Optimized for high-performance parsing scenarios with larger capacity
func NewLRUCache(capacity int) *LRUCache {
	// Ensure minimum capacity for performance, but allow smaller capacities for testing
	if capacity < 1 {
		capacity = 1000
	}
	
	return &LRUCache{
		capacity:  int64(capacity),
		cache:     make(map[string]*list.Element, capacity), // Pre-allocate map with capacity
		evictList: list.New(),
	}
}

// NewLRUCacheWithExpiration creates a new LRU cache with expiration
// Optimized for high-throughput scenarios with lazy expiration
func NewLRUCacheWithExpiration(capacity int, expiration time.Duration) *LRUCache {
	// Ensure minimum capacity for performance, but allow smaller capacities for testing
	if capacity < 1 {
		capacity = 1000
	}
	
	cache := &LRUCache{
		capacity:         int64(capacity),
		cache:            make(map[string]*list.Element, capacity), // Pre-allocate map with capacity
		evictList:        list.New(),
		expiration:       expiration,
		activeExpiration: expiration > 0 && expiration < 10*time.Minute, // Only use active expiration for short durations
	}
	
	// Start active expiration if needed and duration is reasonable
	if cache.activeExpiration {
		cache.startActiveExpiration()
	}
	
	return cache
}

// startActiveExpiration starts the active expiration goroutine
func (c *LRUCache) startActiveExpiration() {
	if c.expiration <= 0 {
		return
	}
	
	c.stopChan = make(chan struct{})
	c.expirationTicker = time.NewTicker(c.expiration / 2) // Check every half expiration period
	
	go func() {
		for {
			select {
			case <-c.expirationTicker.C:
				c.cleanupExpired()
			case <-c.stopChan:
				return
			}
		}
	}()
}

// cleanupExpired removes expired entries from the cache
func (c *LRUCache) cleanupExpired() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	now := time.Now()
	var toRemove []*list.Element
	
	// Collect expired entries
	for element := c.evictList.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*cacheEntry)
		expiration := c.expiration
		if entry.expiration > 0 {
			expiration = entry.expiration
		}
		
		if expiration > 0 && now.Sub(entry.timestamp) > expiration {
			toRemove = append(toRemove, element)
		}
	}
	
	// Remove expired entries
	for _, element := range toRemove {
		c.removeElement(element)
	}
}

// Get retrieves a value from the cache
// Optimized for read-heavy workloads with reduced lock contention
func (c *LRUCache) Get(key string) (interface{}, bool) {
	// Fast path: Check if entry exists without locking for read operations
	c.mutex.RLock()
	element, exists := c.cache[key]
	c.mutex.RUnlock()
	
	if !exists {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	
	// Upgrade to write lock for moving element to front
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// Double-check that element still exists (could have been removed by another goroutine)
	element, exists = c.cache[key]
	if !exists {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	
	entry := element.Value.(*cacheEntry)
	
	// Check if entry has expired
	expiration := c.expiration
	if entry.expiration > 0 {
		expiration = entry.expiration
	}
	
	if expiration > 0 && time.Since(entry.timestamp) > expiration {
		// Entry expired, remove it
		c.removeElement(element)
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	
	// Move to front (most recently used)
	c.evictList.MoveToFront(element)
	value := entry.value
	atomic.AddInt64(&c.hits, 1)
	return value, true
}

// Put adds a value to the cache
func (c *LRUCache) Put(key string, value interface{}) {
	c.PutWithExpiration(key, value, 0)
}

// PutWithExpiration adds a value to the cache with a specific expiration time
func (c *LRUCache) PutWithExpiration(key string, value interface{}, expiration time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Check if key already exists
	if element, exists := c.cache[key]; exists {
		// Update existing entry
		c.evictList.MoveToFront(element)
		entry := element.Value.(*cacheEntry)
		entry.value = value
		entry.timestamp = time.Now()
		entry.expiration = expiration
		return
	}

	// Add new entry
	entry := &cacheEntry{
		key:        key,
		value:      value,
		timestamp:  time.Now(),
		expiration: expiration,
	}
	element := c.evictList.PushFront(entry)
	c.cache[key] = element

	// Evict oldest if necessary
	if int64(c.evictList.Len()) > c.capacity {
		c.evictOldest()
	}
}

// Remove removes a key from the cache
func (c *LRUCache) Remove(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, exists := c.cache[key]; exists {
		c.removeElement(element)
	}
}

// Len returns the number of items in the cache
func (c *LRUCache) Len() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.evictList.Len()
}

// Clear removes all items from the cache
func (c *LRUCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache = make(map[string]*list.Element)
	c.evictList.Init()
	
	// Stop active expiration if running
	if c.stopChan != nil {
		close(c.stopChan)
		c.stopChan = nil
	}
	if c.expirationTicker != nil {
		c.expirationTicker.Stop()
		c.expirationTicker = nil
	}
}

// Stats returns cache hit/miss statistics
func (c *LRUCache) Stats() (hits, misses int64) {
	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses)
}

// HitRate returns the cache hit rate as a percentage
func (c *LRUCache) HitRate() float64 {
	hits, misses := c.Stats()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total) * 100
}

// ResetStats resets the cache statistics
func (c *LRUCache) ResetStats() {
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Close stops the active expiration goroutine and cleans up resources
func (c *LRUCache) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// Stop active expiration if running
	if c.stopChan != nil {
		close(c.stopChan)
		c.stopChan = nil
	}
	if c.expirationTicker != nil {
		c.expirationTicker.Stop()
		c.expirationTicker = nil
	}
}

// evictOldest removes the oldest entry from the cache
func (c *LRUCache) evictOldest() {
	element := c.evictList.Back()
	if element != nil {
		c.removeElement(element)
	}
}

// removeElement removes a specific element from the cache
func (c *LRUCache) removeElement(element *list.Element) {
	c.evictList.Remove(element)
	entry := element.Value.(*cacheEntry)
	delete(c.cache, entry.key)
}