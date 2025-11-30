//go:build !pgquery_simple
// +build !pgquery_simple

package sql

import (
	"fmt"
	"strings"
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
	statsMutex        sync.RWMutex
	parseAttempts     int64
	simpleSuccess     int64
	cacheHits         int64
	fallbackCount     int64
	totalParseTime    time.Duration
	
	// Detailed metrics for performance monitoring
	simpleParseTime   time.Duration
	fullParseTime     time.Duration
	complexQueryCount int64
	simpleQueryCount  int64
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

	// Analyze query complexity to decide which parser to use
	useSimpleParser := p.shouldUseSimpleParser(query)
	
	if useSimpleParser {
		// Track simple query attempts
		p.statsMutex.Lock()
		p.simpleQueryCount++
		p.statsMutex.Unlock()
		
		// Try simple parser first
		startTime := time.Now()
		parsed, err := p.simpleParser.Parse(query)
		duration := time.Since(startTime)
		
		p.statsMutex.Lock()
		p.totalParseTime += duration
		p.simpleParseTime += duration
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
	} else {
		// Track complex query attempts
		p.statsMutex.Lock()
		p.complexQueryCount++
		p.statsMutex.Unlock()
	}

	// Fall back to full parser
	p.statsMutex.Lock()
	p.fallbackCount++
	p.statsMutex.Unlock()
	
	startTime := time.Now()
	parsed, err := p.fullParser.Parse(query)
	duration := time.Since(startTime)
	
	p.statsMutex.Lock()
	p.totalParseTime += duration
	p.fullParseTime += duration
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
	// Normalize the query for better cache hit rates
	normalizedQuery := NormalizeQuery(query)
	
	p.cacheMutex.RLock()
	defer p.cacheMutex.RUnlock()
	
	if cached, exists := p.cache[normalizedQuery]; exists {
		cached.lastAccess = time.Now()
		return cached.parsed
	}
	return nil
}

// cacheResult stores a parsed result in cache
func (p *HybridPGParser) cacheResult(query string, parsed *ParsedQuery) {
	// Normalize the query for better cache hit rates
	normalizedQuery := NormalizeQuery(query)
	
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()
	
	// Evict oldest entries if cache is full
	if p.cacheSize >= p.maxCacheSize {
		p.evictOldestEntries()
	}
	
	// Add new entry
	now := time.Now()
	p.cache[normalizedQuery] = &cachedParseResult{
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
		// For SELECT statements, check if it's a complex query that might need the full parser
		if parsed.Type == SelectStatement {
			return p.isSelectQuerySimpleEnough(parsed)
		}
		return true
	default:
		return false
	}
}

// isSelectQuerySimpleEnough determines if a SELECT query is simple enough for the simple parser
func (p *HybridPGParser) isSelectQuerySimpleEnough(parsed *ParsedQuery) bool {
	// If we have complex features, fall back to full parser
	if len(parsed.OrderBy) > 2 {
		// More than 2 ORDER BY clauses might be complex
		return false
	}
	
	if parsed.Limit != nil && *parsed.Limit > 10000 {
		// Large LIMIT values might indicate complex queries
		return false
	}
	
	// Check for complex conditions
	if len(parsed.Conditions) > 5 {
		// Too many conditions might indicate a complex query
		return false
	}
	
	// Check for complex field selections
	if len(parsed.Fields) > 10 {
		// Too many fields selected
		return false
	}
	
	// Check for special functions or expressions in fields
	for _, field := range parsed.Fields {
		if p.isComplexField(field) {
			return false
		}
	}
	
	// Check for complex conditions
	for _, condition := range parsed.Conditions {
		if p.isComplexCondition(condition) {
			return false
		}
	}
	
	return true
}

// isComplexField checks if a field specification is complex
func (p *HybridPGParser) isComplexField(field string) bool {
	// Check for function calls
	if strings.Contains(field, "(") && strings.Contains(field, ")") {
		return true
	}
	
	// Check for complex expressions
	if strings.Contains(field, " AS ") || strings.Contains(strings.ToUpper(field), " CASE ") {
		return true
	}
	
	// Check for arithmetic operations
	if strings.ContainsAny(field, "+-*/") {
		return true
	}
	
	return false
}

// isComplexCondition checks if a condition is complex
func (p *HybridPGParser) isComplexCondition(condition Condition) bool {
	// Check for subqueries (indicated by parentheses)
	if strings.Contains(fmt.Sprintf("%v", condition.Value), "(") {
		return true
	}
	
	// Check for complex operators
	switch condition.Operator {
	case "IN", "NOT IN", "LIKE", "ILIKE", "SIMILAR TO", "~", "~*", "!~", "!~*":
		// These operators might indicate complex patterns
		return true
	}
	
	// Check for functions in values
	if strings.Contains(fmt.Sprintf("%v", condition.Value), "(") {
		return true
	}
	
	return false
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
		"parse_attempts":     p.parseAttempts,
		"simple_success":     p.simpleSuccess,
		"cache_hits":         p.cacheHits,
		"fallback_count":     p.fallbackCount,
		"complex_query_count": p.complexQueryCount,
		"simple_query_count":  p.simpleQueryCount,
	}
}

// GetDetailedStats returns detailed parsing statistics including timing information
func (p *HybridPGParser) GetDetailedStats() map[string]interface{} {
	p.statsMutex.RLock()
	defer p.statsMutex.RUnlock()
	
	stats := map[string]interface{}{
		"parse_attempts":      p.parseAttempts,
		"simple_success":      p.simpleSuccess,
		"cache_hits":          p.cacheHits,
		"fallback_count":      p.fallbackCount,
		"complex_query_count": p.complexQueryCount,
		"simple_query_count":  p.simpleQueryCount,
		"total_parse_time_ns": p.totalParseTime.Nanoseconds(),
		"simple_parse_time_ns": p.simpleParseTime.Nanoseconds(),
		"full_parse_time_ns":   p.fullParseTime.Nanoseconds(),
	}
	
	// Calculate averages if we have data
	if p.parseAttempts > 0 {
		stats["avg_parse_time_ns"] = p.totalParseTime.Nanoseconds() / p.parseAttempts
	}
	
	if p.simpleSuccess > 0 {
		stats["avg_simple_parse_time_ns"] = p.simpleParseTime.Nanoseconds() / p.simpleSuccess
	}
	
	if p.fallbackCount > 0 {
		stats["avg_full_parse_time_ns"] = p.fullParseTime.Nanoseconds() / p.fallbackCount
	}
	
	// Calculate percentages
	if p.parseAttempts > 0 {
		stats["simple_success_rate"] = float64(p.simpleSuccess) / float64(p.parseAttempts) * 100
		stats["cache_hit_rate"] = float64(p.cacheHits) / float64(p.parseAttempts) * 100
		stats["fallback_rate"] = float64(p.fallbackCount) / float64(p.parseAttempts) * 100
	}
	
	return stats
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

// shouldUseSimpleParser determines if we should attempt to use the simple parser first
func (p *HybridPGParser) shouldUseSimpleParser(query string) bool {
	// Convert to lowercase for easier matching
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	
	// Check for complex SQL features that require the full parser
	complexKeywords := []string{
		"join", "union", "intersect", "except",
		"group by", "having", 
		"window", "over", "partition by",
		"with", "recursive",
		"cast", "convert", "extract",
		"exists", "any", "all", "some",
		"between", "is null", "is not null",
	}
	
	// If any complex keywords are found, use the full parser
	for _, keyword := range complexKeywords {
		if strings.Contains(lowerQuery, keyword) {
			return false
		}
	}
	
	// Check for nested queries
	if strings.Count(lowerQuery, "(") > 3 {
		// Too many parentheses might indicate subqueries
		return false
	}
	
	// Check for complex functions
	complexFunctions := []string{
		"coalesce", "nullif", "case", "when", "then", "else", "end",
		"substring", "trim", "position", "overlay",
		"date_part", "date_trunc", "age", "current_date", "current_time",
	}
	
	for _, function := range complexFunctions {
		if strings.Contains(lowerQuery, function+"(") {
			return false
		}
	}
	
	// For basic CRUD operations, try the simple parser first
	basicOperations := []string{"select", "insert", "update", "delete"}
	for _, op := range basicOperations {
		if strings.HasPrefix(lowerQuery, op) {
			return true
		}
	}
	
	// For other statements, use the full parser
	return false
}