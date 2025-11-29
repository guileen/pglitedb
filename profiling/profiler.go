// Package profiling provides comprehensive profiling infrastructure for PGLiteDB
package profiling

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

// ProfileType represents different types of profiles
type ProfileType string

const (
	CPUProfile     ProfileType = "cpu"
	MemProfile     ProfileType = "heap"
	AllocProfile   ProfileType = "allocs"
	BlockProfile   ProfileType = "block"
	MutexProfile   ProfileType = "mutex"
	GoroutineProfile ProfileType = "goroutine"
	ThreadProfile  ProfileType = "threadcreate"
)

// Profiler manages profiling for the database engine
type Profiler struct {
	cpuProfile    *os.File
	memProfile    *os.File
	allocProfile  *os.File
	blockProfile  *os.File
	mutexProfile  *os.File
	isCPURunning  bool
	isMemRunning  bool
	
	// Enhanced fields
	profiles      map[ProfileType]*ProfileSession
	activeProfiles map[ProfileType]bool
	stats         *ProfileStats
	mutex         sync.Mutex
}

// ProfileSession represents an active profiling session
type ProfileSession struct {
	File     *os.File
	Started  time.Time
	Duration time.Duration
	Type     ProfileType
}

// ProfileStats tracks profiling statistics
type ProfileStats struct {
	TotalSessions     int64
	SuccessfulSessions int64
	FailedSessions    int64
	TotalDuration     time.Duration
}

// Global profiler instance
var (
	globalProfiler     *Profiler
	globalProfilerOnce sync.Once
)

// GetGlobalProfiler returns the global profiler instance (lazy-loaded)
func GetGlobalProfiler() *Profiler {
	globalProfilerOnce.Do(func() {
		globalProfiler = &Profiler{
			profiles:       make(map[ProfileType]*ProfileSession),
			activeProfiles: make(map[ProfileType]bool),
			stats:          &ProfileStats{},
		}
	})
	return globalProfiler
}

// NewProfiler creates a new profiler instance
func NewProfiler() *Profiler {
	return &Profiler{
		profiles:       make(map[ProfileType]*ProfileSession),
		activeProfiles: make(map[ProfileType]bool),
		stats:          &ProfileStats{},
	}
}

// StartCPUProfile starts CPU profiling
func (p *Profiler) StartCPUProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	// Clean up any previous state
	if p.cpuProfile != nil {
		p.cpuProfile.Close()
		p.cpuProfile = nil
	}
	p.activeProfiles[CPUProfile] = false
	delete(p.profiles, CPUProfile)
	
	if p.activeProfiles[CPUProfile] {
		return fmt.Errorf("CPU profiling is already running")
	}
	
	f, err := os.Create(filename)
	if err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not create CPU profile: %w", err)
	}
	
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		p.recordFailedSession()
		return fmt.Errorf("could not start CPU profile: %w", err)
	}
	
	p.cpuProfile = f
	p.activeProfiles[CPUProfile] = true
	p.profiles[CPUProfile] = &ProfileSession{
		File:    f,
		Started: time.Now(),
		Type:    CPUProfile,
	}
	p.recordStartedSession()
	
	return nil
}

// StopCPUProfile stops CPU profiling
func (p *Profiler) StopCPUProfile() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	// Even if not marked as active, try to stop and close if we have a file
	if p.cpuProfile != nil {
		pprof.StopCPUProfile()
		if err := p.cpuProfile.Close(); err != nil {
			p.cpuProfile = nil
			p.activeProfiles[CPUProfile] = false
			delete(p.profiles, CPUProfile)
			p.recordFailedSession()
			return fmt.Errorf("could not close CPU profile file: %w", err)
		}
		p.cpuProfile = nil
	}
	
	// Update session info if we had one
	if session, exists := p.profiles[CPUProfile]; exists {
		session.Duration = time.Since(session.Started)
		p.recordCompletedSession(session.Duration)
	}
	
	// Always clean up the state
	p.activeProfiles[CPUProfile] = false
	delete(p.profiles, CPUProfile)
	
	return nil
}

// StartMemProfile starts memory profiling
func (p *Profiler) StartMemProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.activeProfiles[MemProfile] {
		return fmt.Errorf("memory profiling is already running")
	}
	
	f, err := os.Create(filename)
	if err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not create memory profile: %w", err)
	}
	
	// Get up-to-date statistics
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		f.Close()
		p.recordFailedSession()
		return fmt.Errorf("could not write memory profile: %w", err)
	}
	
	p.memProfile = f
	p.activeProfiles[MemProfile] = true
	p.profiles[MemProfile] = &ProfileSession{
		File:    f,
		Started: time.Now(),
		Type:    MemProfile,
	}
	p.recordStartedSession()
	
	return nil
}

// StopMemProfile stops memory profiling
func (p *Profiler) StopMemProfile() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.activeProfiles[MemProfile] {
		return fmt.Errorf("memory profiling is not running")
	}
	
	session := p.profiles[MemProfile]
	session.Duration = time.Since(session.Started)
	
	if err := p.memProfile.Close(); err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not close memory profile file: %w", err)
	}
	
	p.memProfile = nil
	p.activeProfiles[MemProfile] = false
	delete(p.profiles, MemProfile)
	p.recordCompletedSession(session.Duration)
	
	return nil
}

// StartAllocProfile starts allocation profiling
func (p *Profiler) StartAllocProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.activeProfiles[AllocProfile] {
		return fmt.Errorf("allocation profiling is already running")
	}
	
	// Enable allocs profiling
	runtime.MemProfileRate = 1
	
	p.activeProfiles[AllocProfile] = true
	p.profiles[AllocProfile] = &ProfileSession{
		Started: time.Now(),
		Type:    AllocProfile,
	}
	p.recordStartedSession()
	
	return nil
}

// StopAllocProfile stops allocation profiling and writes the profile
func (p *Profiler) StopAllocProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.activeProfiles[AllocProfile] {
		return fmt.Errorf("allocation profiling is not running")
	}
	
	session := p.profiles[AllocProfile]
	session.Duration = time.Since(session.Started)
	
	f, err := os.Create(filename)
	if err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not create alloc profile: %w", err)
	}
	defer f.Close()
	
	// Write the profile
	if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not write alloc profile: %w", err)
	}
	
	// Reset to default rate
	runtime.MemProfileRate = 512 * 1024
	
	p.activeProfiles[AllocProfile] = false
	delete(p.profiles, AllocProfile)
	p.recordCompletedSession(session.Duration)
	
	return nil
}

// StartBlockProfile starts block profiling
func (p *Profiler) StartBlockProfile(rate int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.activeProfiles[BlockProfile] {
		return fmt.Errorf("block profiling is already running")
	}
	
	runtime.SetBlockProfileRate(rate)
	
	p.activeProfiles[BlockProfile] = true
	p.profiles[BlockProfile] = &ProfileSession{
		Started: time.Now(),
		Type:    BlockProfile,
	}
	p.recordStartedSession()
	
	return nil
}

// StopBlockProfile stops block profiling and writes the profile
func (p *Profiler) StopBlockProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.activeProfiles[BlockProfile] {
		return fmt.Errorf("block profiling is not running")
	}
	
	session := p.profiles[BlockProfile]
	session.Duration = time.Since(session.Started)
	
	f, err := os.Create(filename)
	if err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not create block profile: %w", err)
	}
	defer f.Close()
	
	// Write the profile
	if err := pprof.Lookup("block").WriteTo(f, 0); err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not write block profile: %w", err)
	}
	
	// Disable block profiling
	runtime.SetBlockProfileRate(0)
	
	p.activeProfiles[BlockProfile] = false
	delete(p.profiles, BlockProfile)
	p.recordCompletedSession(session.Duration)
	
	return nil
}

// StartMutexProfile starts mutex profiling
func (p *Profiler) StartMutexProfile(fraction int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.activeProfiles[MutexProfile] {
		return fmt.Errorf("mutex profiling is already running")
	}
	
	runtime.SetMutexProfileFraction(fraction)
	
	p.activeProfiles[MutexProfile] = true
	p.profiles[MutexProfile] = &ProfileSession{
		Started: time.Now(),
		Type:    MutexProfile,
	}
	p.recordStartedSession()
	
	return nil
}

// StopMutexProfile stops mutex profiling and writes the profile
func (p *Profiler) StopMutexProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.activeProfiles[MutexProfile] {
		return fmt.Errorf("mutex profiling is not running")
	}
	
	session := p.profiles[MutexProfile]
	session.Duration = time.Since(session.Started)
	
	f, err := os.Create(filename)
	if err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not create mutex profile: %w", err)
	}
	defer f.Close()
	
	// Write the profile
	if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
		p.recordFailedSession()
		return fmt.Errorf("could not write mutex profile: %w", err)
	}
	
	// Disable mutex profiling
	runtime.SetMutexProfileFraction(0)
	
	p.activeProfiles[MutexProfile] = false
	delete(p.profiles, MutexProfile)
	p.recordCompletedSession(session.Duration)
	
	return nil
}

// recordStartedSession records a started profiling session
func (p *Profiler) recordStartedSession() {
	p.stats.TotalSessions++
}

// recordCompletedSession records a completed profiling session
func (p *Profiler) recordCompletedSession(duration time.Duration) {
	p.stats.SuccessfulSessions++
	p.stats.TotalDuration += duration
}

// recordFailedSession records a failed profiling session
func (p *Profiler) recordFailedSession() {
	p.stats.FailedSessions++
}

// GetProfileStats returns current profiling statistics
func (p *Profiler) GetProfileStats() ProfileStats {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	return ProfileStats{
		TotalSessions:     p.stats.TotalSessions,
		SuccessfulSessions: p.stats.SuccessfulSessions,
		FailedSessions:    p.stats.FailedSessions,
		TotalDuration:     p.stats.TotalDuration,
	}
}

// IsProfileActive checks if a specific profile type is active
func (p *Profiler) IsProfileActive(profileType ProfileType) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	return p.activeProfiles[profileType]
}

// GetActiveProfiles returns a list of currently active profiles
func (p *Profiler) GetActiveProfiles() []ProfileType {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	active := make([]ProfileType, 0, len(p.activeProfiles))
	for profileType, isActive := range p.activeProfiles {
		if isActive {
			active = append(active, profileType)
		}
	}
	
	return active
}



// StartContinuousProfiling starts continuous profiling in the background
func (p *Profiler) StartContinuousProfiling(ctx context.Context, interval time.Duration, prefix string) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		
		counter := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				counter++
				
				// Take memory profile
				memFilename := fmt.Sprintf("%s_mem_%d.prof", prefix, counter)
				if err := p.StartMemProfile(memFilename); err != nil {
					// Log error but continue
					fmt.Printf("Warning: could not take memory profile: %v\n", err)
				}
				
				// Take CPU profile for 10 seconds
				cpuFilename := fmt.Sprintf("%s_cpu_%d.prof", prefix, counter)
				if err := p.StartCPUProfile(cpuFilename); err != nil {
					fmt.Printf("Warning: could not start CPU profile: %v\n", err)
				} else {
					time.Sleep(10 * time.Second)
					if err := p.StopCPUProfile(); err != nil {
						fmt.Printf("Warning: could not stop CPU profile: %v\n", err)
					}
				}
				
				// Take goroutine profile
				goroutineFilename := fmt.Sprintf("%s_goroutine_%d.prof", prefix, counter)
				if err := p.WriteGoroutineProfile(goroutineFilename); err != nil {
					fmt.Printf("Warning: could not take goroutine profile: %v\n", err)
				}
			}
		}
	}()
}

// ProfileSnapshot captures a snapshot of current profiling data
type ProfileSnapshot struct {
	ActiveProfiles []ProfileType
	Stats          ProfileStats
	Timestamp      time.Time
}

// TakeSnapshot captures a snapshot of the current profiling state
func (p *Profiler) TakeSnapshot() ProfileSnapshot {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	active := make([]ProfileType, 0, len(p.activeProfiles))
	for profileType, isActive := range p.activeProfiles {
		if isActive {
			active = append(active, profileType)
		}
	}
	
	return ProfileSnapshot{
		ActiveProfiles: active,
		Stats: ProfileStats{
			TotalSessions:     p.stats.TotalSessions,
			SuccessfulSessions: p.stats.SuccessfulSessions,
			FailedSessions:    p.stats.FailedSessions,
			TotalDuration:     p.stats.TotalDuration,
		},
		Timestamp: time.Now(),
	}
}

// WriteProfiles writes all current profiles to files
func (p *Profiler) WriteProfiles(prefix string) error {
	// Memory profile
	memFilename := prefix + "_heap.prof"
	if err := p.StartMemProfile(memFilename); err != nil {
		return fmt.Errorf("could not write memory profile: %w", err)
	}
	
	// Goroutine profile
	goroutineFilename := prefix + "_goroutine.prof"
	if err := p.WriteGoroutineProfile(goroutineFilename); err != nil {
		return fmt.Errorf("could not write goroutine profile: %w", err)
	}
	
	// Thread creation profile
	threadFilename := prefix + "_threadcreate.prof"
	if err := p.WriteThreadProfile(threadFilename); err != nil {
		return fmt.Errorf("could not write thread creation profile: %w", err)
	}
	
	return nil
}

// WriteGoroutineProfile writes a goroutine profile to a file
func (p *Profiler) WriteGoroutineProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create goroutine profile: %w", err)
	}
	defer f.Close()
	
	if err := pprof.Lookup("goroutine").WriteTo(f, 0); err != nil {
		return fmt.Errorf("could not write goroutine profile: %w", err)
	}
	
	return nil
}

// WriteThreadProfile writes a thread creation profile to a file
func (p *Profiler) WriteThreadProfile(filename string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create thread creation profile: %w", err)
	}
	defer f.Close()
	
	if err := pprof.Lookup("threadcreate").WriteTo(f, 0); err != nil {
		return fmt.Errorf("could not write thread creation profile: %w", err)
	}
	
	return nil
}

// StartProfile starts profiling for the specified profile type
func (p *Profiler) StartProfile(profileType ProfileType, filename string) error {
	switch profileType {
	case CPUProfile:
		return p.StartCPUProfile(filename)
	case MemProfile:
		return p.StartMemProfile(filename)
	case AllocProfile:
		return p.StartAllocProfile(filename)
	case BlockProfile:
		// For block profile, we need a rate parameter
		return p.StartBlockProfile(1)
	case MutexProfile:
		// For mutex profile, we need a fraction parameter
		return p.StartMutexProfile(1)
	default:
		return fmt.Errorf("unsupported profile type: %s", profileType)
	}
}

// StopProfile stops profiling for the specified profile type
func (p *Profiler) StopProfile(profileType ProfileType, filename string) error {
	switch profileType {
	case CPUProfile:
		return p.StopCPUProfile()
	case MemProfile:
		return p.StopMemProfile()
	case AllocProfile:
		return p.StopAllocProfile(filename)
	case BlockProfile:
		return p.StopBlockProfile(filename)
	case MutexProfile:
		return p.StopMutexProfile(filename)
	default:
		return fmt.Errorf("unsupported profile type: %s", profileType)
	}
}