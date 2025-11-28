package memory

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryContext implements hierarchical memory contexts for better tracking
type MemoryContext struct {
	parent     *MemoryContext
	children   []*MemoryContext
	allocated  int64
	limit      int64
	pool       *sync.Pool
	mutex      sync.Mutex
	
	// Enhanced tracking fields
	id         string           // Unique identifier for debugging
	name       string           // Human-readable name
	createdAt  time.Time        // When this context was created
	stats      *MemoryStats     // Detailed statistics
	tracer     *MemoryTracer    // Memory allocation tracing
}

// NewMemoryContext creates a new memory context with the specified parent and limit
func NewMemoryContext(parent *MemoryContext, limit int64, opts ...MemoryContextOption) *MemoryContext {
	ctx := &MemoryContext{
		parent:    parent,
		limit:     limit,
		pool:      &sync.Pool{},
		createdAt: time.Now(),
		stats:     &MemoryStats{},
	}
	
	// Apply options
	for _, opt := range opts {
		opt(ctx)
	}
	
	// Generate ID if not provided
	if ctx.id == "" {
		ctx.id = fmt.Sprintf("mc-%d", time.Now().UnixNano())
	}
	
	// Add this context to parent's children if parent exists
	if parent != nil {
		parent.mutex.Lock()
		parent.children = append(parent.children, ctx)
		parent.mutex.Unlock()
	}
	
	return ctx
}

// Allocate records a memory allocation of the specified size
func (mc *MemoryContext) Allocate(size int64) error {
	// Check if this allocation would exceed the limit
	current := atomic.LoadInt64(&mc.allocated)
	if mc.limit > 0 && current+size > mc.limit {
		return &MemoryLimitExceededError{
			Context: mc,
			Current: current,
			Request: size,
			Limit:   mc.limit,
		}
	}
	
	// Update allocated count
	atomic.AddInt64(&mc.allocated, size)
	
	// Update statistics
	atomic.AddUint64(&mc.stats.AllocationCount, 1)
	
	// Update peak usage if needed
	for {
		currentPeak := atomic.LoadInt64(&mc.stats.PeakUsage)
		newUsage := current + size
		if newUsage <= currentPeak {
			break
		}
		if atomic.CompareAndSwapInt64(&mc.stats.PeakUsage, currentPeak, newUsage) {
			mc.stats.LastPeakTime = time.Now()
			break
		}
	}
	
	// Update average size
	mc.updateAverageSize(size)
	
	// Trace allocation if enabled
	if mc.tracer != nil && mc.tracer.enabled {
		mc.traceAllocation(size)
	}
	
	// Propagate to parent if exists
	if mc.parent != nil {
		return mc.parent.Allocate(size)
	}
	
	return nil
}

// updateAverageSize updates the average allocation size
func (mc *MemoryContext) updateAverageSize(size int64) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	
	count := atomic.LoadUint64(&mc.stats.AllocationCount)
	if count > 0 {
		currentAvg := mc.stats.AverageSize
		newAvg := (currentAvg*float64(count-1) + float64(size)) / float64(count)
		mc.stats.AverageSize = newAvg
	} else {
		mc.stats.AverageSize = float64(size)
	}
}

// traceAllocation records allocation information for debugging
func (mc *MemoryContext) traceAllocation(size int64) {
	if mc.tracer == nil {
		return
	}
	
	mc.tracer.mutex.Lock()
	defer mc.tracer.mutex.Unlock()
	
	// In a real implementation, we would capture the stack trace
	// For now, we'll just record basic information
	info := AllocationInfo{
		Size:      size,
		Timestamp: time.Now(),
		Stack:     "stack trace would be here",
	}
	
	// Use a simple hash of the current time as a pseudo-pointer
	ptr := uintptr(time.Now().UnixNano())
	mc.tracer.allocs[ptr] = info
}

// Deallocate records a memory deallocation of the specified size
func (mc *MemoryContext) Deallocate(size int64) {
	atomic.AddInt64(&mc.allocated, -size)
	atomic.AddUint64(&mc.stats.DeallocationCount, 1)
	
	// Trace deallocation if enabled
	if mc.tracer != nil && mc.tracer.enabled {
		mc.traceDeallocation(size)
	}
	
	// Propagate to parent if exists
	if mc.parent != nil {
		mc.parent.Deallocate(size)
	}
}

// traceDeallocation records deallocation information for debugging
func (mc *MemoryContext) traceDeallocation(size int64) {
	if mc.tracer == nil {
		return
	}
	
	// In a real implementation, we would remove the allocation trace
	// For now, we'll just note that a deallocation occurred
	mc.tracer.mutex.Lock()
	defer mc.tracer.mutex.Unlock()
	
	// Simple tracking - in a real implementation we would match with allocation
}

// GetCurrentUsage returns the current memory usage
func (mc *MemoryContext) GetCurrentUsage() int64 {
	return atomic.LoadInt64(&mc.allocated)
}

// GetLimit returns the memory limit for this context
func (mc *MemoryContext) GetLimit() int64 {
	return mc.limit
}

// GetPool returns the sync.Pool associated with this context
func (mc *MemoryContext) GetPool() *sync.Pool {
	return mc.pool
}

// GetStats returns a copy of the current memory statistics
func (mc *MemoryContext) GetStats() MemoryStats {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	
	return MemoryStats{
		AllocationCount:   atomic.LoadUint64(&mc.stats.AllocationCount),
		DeallocationCount: atomic.LoadUint64(&mc.stats.DeallocationCount),
		PeakUsage:         atomic.LoadInt64(&mc.stats.PeakUsage),
		LastPeakTime:      mc.stats.LastPeakTime,
		AverageSize:       mc.stats.AverageSize,
	}
}

// GetID returns the unique identifier of this context
func (mc *MemoryContext) GetID() string {
	return mc.id
}

// GetName returns the name of this context
func (mc *MemoryContext) GetName() string {
	return mc.name
}

// GetCreatedAt returns when this context was created
func (mc *MemoryContext) GetCreatedAt() time.Time {
	return mc.createdAt
}

// ForceGC forces garbage collection and updates memory statistics
func (mc *MemoryContext) ForceGC() {
	runtime.GC()
	
	// Update statistics after GC
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// In a real implementation, we might update context statistics based on GC results
}

// WithContext creates a new context with this memory context
func (mc *MemoryContext) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, "memoryContext", mc)
}

// FromContext retrieves a memory context from a context
func FromContext(ctx context.Context) *MemoryContext {
	if mc, ok := ctx.Value("memoryContext").(*MemoryContext); ok {
		return mc
	}
	return nil
}

// MemoryLimitExceededError represents an error when memory limit is exceeded
type MemoryLimitExceededError struct {
	Context *MemoryContext
	Current int64
	Request int64
	Limit   int64
}

func (e *MemoryLimitExceededError) Error() string {
	return fmt.Sprintf("memory limit exceeded: current=%d, request=%d, limit=%d", e.Current, e.Request, e.Limit)
}

// MemoryStats tracks detailed memory statistics
type MemoryStats struct {
	AllocationCount uint64    // Number of allocations
	DeallocationCount uint64  // Number of deallocations
	PeakUsage       int64     // Peak memory usage
	LastPeakTime    time.Time // When peak usage occurred
	AverageSize     float64   // Average allocation size
}

// MemoryTracer tracks memory allocations for debugging
type MemoryTracer struct {
	enabled bool
	allocs  map[uintptr]AllocationInfo
	mutex   sync.RWMutex
}

// AllocationInfo contains information about a memory allocation
type AllocationInfo struct {
	Size      int64
	Timestamp time.Time
	Stack     string
}

// MemoryContextOption configures a MemoryContext
type MemoryContextOption func(*MemoryContext)

// WithName sets the name of the memory context
func WithName(name string) MemoryContextOption {
	return func(mc *MemoryContext) {
		mc.name = name
	}
}

// WithID sets the ID of the memory context
func WithID(id string) MemoryContextOption {
	return func(mc *MemoryContext) {
		mc.id = id
	}
}

// WithTracing enables memory allocation tracing
func WithTracing() MemoryContextOption {
	return func(mc *MemoryContext) {
		mc.tracer = &MemoryTracer{
			enabled: true,
			allocs:  make(map[uintptr]AllocationInfo),
		}
	}
}