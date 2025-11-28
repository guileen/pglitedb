package idgen

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// IDGenerator handles ID generation for the database engine
type IDGenerator struct {
	rowIDGenerator      RowIDGenerator
	tableIDCounters     map[int64]*int64
	indexIDCounters     map[string]*int64
	tableIDCountersMu   sync.RWMutex
	indexIDCountersMu   sync.RWMutex
}

// RowIDGenerator interface for generating row IDs
type RowIDGenerator interface {
	Next() (int64, error)
}

// snowflakeRowIDGenerator implements RowIDGenerator using Snowflake algorithm
type snowflakeRowIDGenerator struct {
	generator *snowflakeIDGenerator
}

func (s *snowflakeRowIDGenerator) Next() (int64, error) {
	return s.generator.Next()
}

// NewIDGenerator creates a new IDGenerator
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{
		rowIDGenerator:  &snowflakeRowIDGenerator{generator: NewSnowflakeIDGenerator(0)},
		tableIDCounters: make(map[int64]*int64),
		indexIDCounters: make(map[string]*int64),
	}
}

// NextRowID generates a new row ID
func (ig *IDGenerator) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return ig.rowIDGenerator.Next()
}

// NextTableID generates a new table ID
func (ig *IDGenerator) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	ig.tableIDCountersMu.RLock()
	if counter, exists := ig.tableIDCounters[tenantID]; exists {
		ig.tableIDCountersMu.RUnlock()
		return atomic.AddInt64(counter, 1), nil
	}
	ig.tableIDCountersMu.RUnlock()

	ig.tableIDCountersMu.Lock()
	defer ig.tableIDCountersMu.Unlock()
	
	// Double-check after acquiring write lock
	if counter, exists := ig.tableIDCounters[tenantID]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	var startID int64 = 0
	counter := &startID
	ig.tableIDCounters[tenantID] = counter

	return atomic.AddInt64(counter, 1), nil
}

// NextIndexID generates a new index ID
func (ig *IDGenerator) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	key := fmt.Sprintf("%d:%d", tenantID, tableID)

	ig.indexIDCountersMu.RLock()
	if counter, exists := ig.indexIDCounters[key]; exists {
		ig.indexIDCountersMu.RUnlock()
		return atomic.AddInt64(counter, 1), nil
	}
	ig.indexIDCountersMu.RUnlock()

	ig.indexIDCountersMu.Lock()
	defer ig.indexIDCountersMu.Unlock()
	
	// Double-check after acquiring write lock
	if counter, exists := ig.indexIDCounters[key]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	var startID int64 = 0
	counter := &startID
	ig.indexIDCounters[key] = counter

	return atomic.AddInt64(counter, 1), nil
}

// Snowflake ID Generator implementation
type snowflakeIDGenerator struct {
	mu        sync.Mutex
	epoch     int64
	machineID int64
	sequence  int64
	lastTime  int64
}

func NewSnowflakeIDGenerator(machineID int64) *snowflakeIDGenerator {
	if machineID < 0 || machineID > 1023 {
		panic("machineID must be between 0-1023")
	}

	return &snowflakeIDGenerator{
		epoch:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		machineID: machineID,
		sequence:  0,
		lastTime:  0,
	}
}

func (g *snowflakeIDGenerator) Next() (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()

	if now < g.lastTime {
		return 0, fmt.Errorf("clock moved backwards")
	}

	if now == g.lastTime {
		g.sequence = (g.sequence + 1) & 4095
		if g.sequence == 0 {
			for now <= g.lastTime {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTime = now

	id := ((now - g.epoch) << 22) | (g.machineID << 12) | g.sequence
	return id, nil
}