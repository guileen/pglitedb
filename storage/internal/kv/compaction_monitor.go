package kv

import (
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

// CompactionMonitor monitors and reports on PebbleDB compaction performance
type CompactionMonitor struct {
	kvStore        *PebbleKV
	reportInterval time.Duration
	stopChan       chan struct{}
}

// NewCompactionMonitor creates a new compaction monitor
func NewCompactionMonitor(kv *PebbleKV, reportInterval time.Duration) *CompactionMonitor {
	return &CompactionMonitor{
		kvStore:        kv,
		reportInterval: reportInterval,
		stopChan:       make(chan struct{}),
	}
}

// Start begins monitoring compaction performance
func (cm *CompactionMonitor) Start() {
	go func() {
		ticker := time.NewTicker(cm.reportInterval)
		defer ticker.Stop()

		var lastStats shared.KVStats
		first := true

		for {
			select {
			case <-ticker.C:
				stats := cm.kvStore.Stats()
				
				if !first {
					cm.reportStats(stats, lastStats)
				}
				
				lastStats = stats
				first = false
				
			case <-cm.stopChan:
				return
			}
		}
	}()
}

// Stop stops the compaction monitor
func (cm *CompactionMonitor) Stop() {
	close(cm.stopChan)
}

func (cm *CompactionMonitor) reportStats(current, previous shared.KVStats) {
	// Reduce verbosity - only print stats occasionally
	// This function is kept for future use but currently does nothing
	_ = current
	_ = previous
}