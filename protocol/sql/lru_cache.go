package sql

import (
	"container/list"
	"sync"
	"time"
)

// LRUCache implements a thread-safe LRU cache with expiration support
type LRUCache struct {
	capacity   int
	cache      map[string]*list.Element
	evictList  *list.List
	mutex      sync.RWMutex
	expiration time.Duration
}

// cacheEntry represents a cached item
type cacheEntry struct {
	key       string
	value     interface{}
	timestamp time.Time
}

// NewLRUCache creates a new LRU cache with the specified capacity
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity:  capacity,
		cache:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

// NewLRUCacheWithExpiration creates a new LRU cache with expiration
func NewLRUCacheWithExpiration(capacity int, expiration time.Duration) *LRUCache {
	return &LRUCache{
		capacity:   capacity,
		cache:      make(map[string]*list.Element),
		evictList:  list.New(),
		expiration: expiration,
	}
}

// Get retrieves a value from the cache
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, exists := c.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		
		// Check if entry has expired
		if c.expiration > 0 && time.Since(entry.timestamp) > c.expiration {
			c.removeElement(element)
			return nil, false
		}
		
		// Move to front (most recently used)
		c.evictList.MoveToFront(element)
		return entry.value, true
	}
	
	return nil, false
}

// Put adds a value to the cache
func (c *LRUCache) Put(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Check if key already exists
	if element, exists := c.cache[key]; exists {
		// Update existing entry
		c.evictList.MoveToFront(element)
		entry := element.Value.(*cacheEntry)
		entry.value = value
		entry.timestamp = time.Now()
		return
	}

	// Add new entry
	entry := &cacheEntry{
		key:       key,
		value:     value,
		timestamp: time.Now(),
	}
	element := c.evictList.PushFront(entry)
	c.cache[key] = element

	// Evict oldest if necessary
	if c.evictList.Len() > c.capacity {
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