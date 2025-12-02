package sql

import (
    "container/list"
    "sync"
    "sync/atomic"
    "time"
)

// LFUCache implements a thread-safe LFU cache with expiration support
type LFUCache struct {
    capacity   int64
    cache      map[string]*list.Element
    freqLists  map[int]*list.List // Maps frequency to list of entries with that frequency
    minFreq    int                // Minimum frequency in the cache
    mutex      sync.RWMutex
    expiration time.Duration
    
    // Atomic counters for better performance monitoring
    hits   int64
    misses int64
}

// lfuCacheEntry represents a cached item with frequency tracking
type lfuCacheEntry struct {
    key        string
    value      interface{}
    timestamp  time.Time
    frequency  int
    freqList   *list.List // Reference to the frequency list this entry belongs to
}

// NewLFUCache creates a new LFU cache with the specified capacity
func NewLFUCache(capacity int) *LFUCache {
    if capacity < 1 {
        capacity = 100000 // Default to 100,000 if invalid
    }
    
    return &LFUCache{
        capacity:  int64(capacity),
        cache:     make(map[string]*list.Element),
        freqLists: make(map[int]*list.List),
        minFreq:   0,
    }
}

// NewLFUCacheWithExpiration creates a new LFU cache with expiration
func NewLFUCacheWithExpiration(capacity int, expiration time.Duration) *LFUCache {
    cache := NewLFUCache(capacity)
    cache.expiration = expiration
    return cache
}

// Get retrieves a value from the cache using LFU eviction policy
func (c *LFUCache) Get(key string) (interface{}, bool) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    element, exists := c.cache[key]
    if !exists {
        atomic.AddInt64(&c.misses, 1)
        return nil, false
    }
    
    entry := element.Value.(*lfuCacheEntry)
    
    // Check if entry has expired
    if c.expiration > 0 && time.Since(entry.timestamp) > c.expiration {
        // Entry expired, remove it
        c.removeElement(element)
        atomic.AddInt64(&c.misses, 1)
        return nil, false
    }
    
    // Increment frequency
    c.incrementFrequency(entry, element)
    
    atomic.AddInt64(&c.hits, 1)
    return entry.value, true
}

// incrementFrequency increases the frequency of an entry and moves it to the appropriate frequency list
func (c *LFUCache) incrementFrequency(entry *lfuCacheEntry, element *list.Element) {
    // Remove from current frequency list
    if entry.freqList != nil {
        entry.freqList.Remove(element)
    }
    
    // Increment frequency
    entry.frequency++
    
    // Get or create the frequency list for the new frequency
    freqList, exists := c.freqLists[entry.frequency]
    if !exists {
        freqList = list.New()
        c.freqLists[entry.frequency] = freqList
    }
    
    // Add to the new frequency list
    entry.freqList = freqList
    freqList.PushBack(entry)
    
    // Update minFreq if necessary
    if entry.frequency == 1 {
        c.minFreq = 1
    } else if c.minFreq == entry.frequency-1 {
        // Check if the previous minimum frequency list is now empty
        if prevList, exists := c.freqLists[c.minFreq]; exists && prevList.Len() == 0 {
            c.minFreq = entry.frequency
        }
    }
}

// Put adds a value to the cache using LFU eviction policy
func (c *LFUCache) Put(key string, value interface{}) {
    c.PutWithExpiration(key, value, 0)
}

// PutWithExpiration adds a value to the cache with a specific expiration time
func (c *LFUCache) PutWithExpiration(key string, value interface{}, expiration time.Duration) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    // Check if key already exists
    if element, exists := c.cache[key]; exists {
        // Update existing entry
        entry := element.Value.(*lfuCacheEntry)
        entry.value = value
        entry.timestamp = time.Now()
        if expiration > 0 {
            entry.timestamp = time.Now().Add(expiration - c.expiration)
        }
        
        // Increment frequency
        c.incrementFrequency(entry, element)
        return
    }
    
    // Add new entry
    entry := &lfuCacheEntry{
        key:       key,
        value:     value,
        timestamp: time.Now(),
        frequency: 0, // Will be incremented to 1
    }
    
    // Check capacity and evict if necessary
    if int64(len(c.cache)) >= c.capacity {
        c.evict()
    }
    
    // Add to cache
    freqList, exists := c.freqLists[1]
    if !exists {
        freqList = list.New()
        c.freqLists[1] = freqList
    }
    
    entry.freqList = freqList
    element := freqList.PushBack(entry)
    c.cache[key] = element
    
    // Set minFreq to 1 since we just added a new entry with frequency 1
    c.minFreq = 1
}

// evict removes the least frequently used entry from the cache
func (c *LFUCache) evict() {
    // Find the frequency list with the minimum frequency
    freqList, exists := c.freqLists[c.minFreq]
    if !exists || freqList.Len() == 0 {
        // Fallback: find the actual minimum frequency
        c.minFreq = 0
        for freq := range c.freqLists {
            if c.minFreq == 0 || freq < c.minFreq {
                c.minFreq = freq
            }
        }
        freqList, exists = c.freqLists[c.minFreq]
        if !exists || freqList.Len() == 0 {
            return // Nothing to evict
        }
    }
    
    // Remove the least recently used entry from the minimum frequency list
    element := freqList.Front()
    if element != nil {
        entry := element.Value.(*lfuCacheEntry)
        // Find the key in the cache map
        var keyToRemove string
        for k, v := range c.cache {
            if v.Value == entry {
                keyToRemove = k
                break
            }
        }
        if keyToRemove != "" {
            delete(c.cache, keyToRemove)
        }
        freqList.Remove(element)
        
        // Clean up empty frequency lists
        if freqList.Len() == 0 {
            delete(c.freqLists, c.minFreq)
        }
    }
}

// removeElement removes a specific element from the cache
func (c *LFUCache) removeElement(element *list.Element) {
    entry := element.Value.(*lfuCacheEntry)
    c.removeElementByEntry(entry)
}

// removeElementByEntry removes a specific entry from the cache
func (c *LFUCache) removeElementByEntry(entry *lfuCacheEntry) {
    // Remove from frequency list
    if entry.freqList != nil {
        for e := entry.freqList.Front(); e != nil; e = e.Next() {
            if e.Value == entry {
                entry.freqList.Remove(e)
                break
            }
        }
    }
    
    // Remove from cache
    // We don't have a direct way to remove from the map without the key
    // This will be handled by the caller who has access to the key
    
    // Clean up empty frequency lists
    if entry.freqList != nil && entry.freqList.Len() == 0 {
        // Find the frequency value for this list
        for freq, list := range c.freqLists {
            if list == entry.freqList {
                delete(c.freqLists, freq)
                break
            }
        }
    }
}

// elementForEntry finds the list element for a given entry
// This is a helper function to find the element in the list
func elementForEntry(entry *lfuCacheEntry) *list.Element {
    if entry.freqList == nil {
        return nil
    }
    
    for e := entry.freqList.Front(); e != nil; e = e.Next() {
        if e.Value == entry {
            return e
        }
    }
    return nil
}

// Remove removes a key from the cache
func (c *LFUCache) Remove(key string) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    if element, exists := c.cache[key]; exists {
        c.removeElement(element)
    }
}

// Len returns the number of items in the cache
func (c *LFUCache) Len() int {
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    return len(c.cache)
}

// Clear removes all items from the cache
func (c *LFUCache) Clear() {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    c.cache = make(map[string]*list.Element)
    c.freqLists = make(map[int]*list.List)
    c.minFreq = 0
    
    // Reset stats
    atomic.StoreInt64(&c.hits, 0)
    atomic.StoreInt64(&c.misses, 0)
}

// Stats returns cache hit/miss statistics
func (c *LFUCache) Stats() (hits, misses int64) {
    return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses)
}

// HitRate returns the cache hit rate as a percentage
func (c *LFUCache) HitRate() float64 {
    hits, misses := c.Stats()
    total := hits + misses
    if total == 0 {
        return 0.0
    }
    return float64(hits) / float64(total) * 100
}

// ResetStats resets the cache statistics
func (c *LFUCache) ResetStats() {
    atomic.StoreInt64(&c.hits, 0)
    atomic.StoreInt64(&c.misses, 0)
}