//go:build !pgquery_simple
// +build !pgquery_simple

package sql

import (
	"sync"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// HybridPGParser implements a hybrid PostgreSQL parser that combines
// a fast simple parser with caching and falls back to the full parser when needed
type HybridPGParser struct {
	simpleParser *SimplePGParser
	fullParser   *FullPGParser
	
	// Query cache with LRU eviction
	cache      map[string]*cachedParseResult
	cacheMutex sync.RWMutex
	cacheSize  int
	maxCacheSize int
	
	// Metrics
	statsMutex     sync.RWMutex
	parseAttempts  int64
	simpleSuccess  int64
	cacheHits      int64
	fallbackCount  int64
	totalParseTime time.Duration
}

// cachedParseResult stores cached parsing results
type cachedParseResult struct {
	parsed     *ParsedQuery
	timestamp  time.Time
	lastAccess time.Time
}

// NewHybridPGParser creates a new hybrid PostgreSQL parser instance
// Increased cache size to reduce CGO call overhead based on profiling analysis
func NewHybridPGParser() *HybridPGParser {
	return &HybridPGParser{
		simpleParser:  NewSimplePGParser(),
		fullParser:    NewFullPGParser(), // The full CGO parser
		cache:         make(map[string]*cachedParseResult),
		cacheSize:     0,
		maxCacheSize:  5000, // Increased cache size to 5000 entries to reduce CGO overhead
	}
}

// Parse takes a raw SQL query string and returns a parsed representation
// It tries the simple parser first, uses cache if available, and falls back to full parser
func (p *HybridPGParser) Parse(query string) (*ParsedQuery, error) {
	p.statsMutex.Lock()
	p.parseAttempts++
	p.statsMutex.Unlock()

	// Check cache first
	if cached := p.getCachedResult(query); cached != nil {
		p.statsMutex.Lock()
		p.cacheHits++
		p.statsMutex.Unlock()
		return cached, nil
	}

	// Try simple parser first
	startTime := time.Now()
	parsed, err := p.simpleParser.Parse(query)
	duration := time.Since(startTime)
	
	p.statsMutex.Lock()
	p.totalParseTime += duration
	p.statsMutex.Unlock()

	if err == nil && p.isSimpleParseValid(parsed) {
		// Simple parser succeeded and result is valid
		p.statsMutex.Lock()
		p.simpleSuccess++
		p.statsMutex.Unlock()
		
		// Cache the result
		p.cacheResult(query, parsed)
		return parsed, nil
	}

	// Fall back to full parser
	p.statsMutex.Lock()
	p.fallbackCount++
	p.statsMutex.Unlock()
	
	startTime = time.Now()
	parsed, err = p.fullParser.Parse(query)
	duration = time.Since(startTime)
	
	p.statsMutex.Lock()
	p.totalParseTime += duration
	p.statsMutex.Unlock()
	
	if err != nil {
		return nil, err
	}

	// Cache the result from full parser as well
	p.cacheResult(query, parsed)
	return parsed, nil
}

// getCachedResult retrieves a parsed result from cache if available
func (p *HybridPGParser) getCachedResult(query string) *ParsedQuery {
	p.cacheMutex.RLock()
	defer p.cacheMutex.RUnlock()
	
	if cached, exists := p.cache[query]; exists {
		cached.lastAccess = time.Now()
		return cached.parsed
	}
	return nil
}

// cacheResult stores a parsed result in cache
func (p *HybridPGParser) cacheResult(query string, parsed *ParsedQuery) {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()
	
	// Evict oldest entries if cache is full
	if p.cacheSize >= p.maxCacheSize {
		p.evictOldestEntries()
	}
	
	// Add new entry
	now := time.Now()
	p.cache[query] = &cachedParseResult{
		parsed:     parsed,
		timestamp:  now,
		lastAccess: now,
	}
	p.cacheSize++
}

// evictOldestEntries removes the least recently accessed entries from cache
func (p *HybridPGParser) evictOldestEntries() {
	// Find and remove the oldest entries
	// For simplicity, we'll remove 10% of the cache
	toRemove := p.maxCacheSize / 10
	if toRemove < 1 {
		toRemove = 1
	}
	
	// Simple LRU eviction - remove oldest by last access time
	type cacheEntry struct {
		key       string
		lastAccess time.Time
	}
	
	entries := make([]cacheEntry, 0, len(p.cache))
	for key, entry := range p.cache {
		entries = append(entries, cacheEntry{key: key, lastAccess: entry.lastAccess})
	}
	
	// Sort by last access time (oldest first)
	// Simple bubble sort for small numbers
	for i := 0; i < len(entries)-1; i++ {
		for j := 0; j < len(entries)-i-1; j++ {
			if entries[j].lastAccess.After(entries[j+1].lastAccess) {
				entries[j], entries[j+1] = entries[j+1], entries[j]
			}
		}
	}
	
	// Remove the oldest entries
	for i := 0; i < toRemove && i < len(entries); i++ {
		delete(p.cache, entries[i].key)
		p.cacheSize--
	}
}

// isSimpleParseValid checks if the result from simple parser is valid enough
// This is a heuristic to determine when we can trust the simple parser
func (p *HybridPGParser) isSimpleParseValid(parsed *ParsedQuery) bool {
	// For now, we trust the simple parser for basic CRUD operations
	// In the future, we might add more sophisticated validation
	switch parsed.Type {
	case SelectStatement, InsertStatement, UpdateStatement, DeleteStatement:
		return true
	default:
		return false
	}
}

// Validate checks if a query is syntactically valid
func (p *HybridPGParser) Validate(query string) error {
	// Use simple parser for validation first
	if err := p.simpleParser.Validate(query); err != nil {
		// Fall back to full parser for validation
		return p.fullParser.Validate(query)
	}
	return nil
}

// ParseWithParams parses a query with parameter information
func (p *HybridPGParser) ParseWithParams(query string, paramCount int) (*ParsedQuery, error) {
	// For parameterized queries, we might want to use the full parser
	// as they tend to be more complex, but we still try the simple path first
	return p.Parse(query)
}

// GetStatementType returns the type of the SQL statement
func (p *HybridPGParser) GetStatementType(stmt interface{}) StatementType {
	// Delegate to the appropriate parser based on the statement type
	switch s := stmt.(type) {
	case *pg_query.Node:
		return p.fullParser.GetStatementType(s)
	case string:
		return p.simpleParser.GetStatementType(s)
	default:
		return SelectStatement
	}
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *HybridPGParser) SupportsParameterPlaceholders() bool {
	return true
}

// GetStats returns parsing statistics
func (p *HybridPGParser) GetStats() map[string]int64 {
	p.statsMutex.RLock()
	defer p.statsMutex.RUnlock()
	
	return map[string]int64{
		"parse_attempts": p.parseAttempts,
		"simple_success": p.simpleSuccess,
		"cache_hits":     p.cacheHits,
		"fallback_count": p.fallbackCount,
	}
}

// GetAverageParseTime returns the average parse time in nanoseconds
func (p *HybridPGParser) GetAverageParseTime() time.Duration {
	p.statsMutex.RLock()
	defer p.statsMutex.RUnlock()
	
	if p.parseAttempts > 0 {
		return p.totalParseTime / time.Duration(p.parseAttempts)
	}
	return 0
}