package kv

import (
	"fmt"
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
	// Calculate deltas
	flushDelta := current.FlushCount - previous.FlushCount
	compactionDelta := current.CompactionCount - previous.CompactionCount
	bytesWrittenDelta := current.CompactionBytesWritten - previous.CompactionBytesWritten

	fmt.Printf("Compaction Stats Report:\n")
	fmt.Printf("  L0 Files: %d, L1 Files: %d, L2 Files: %d\n", 
		current.L0FileCount, current.L1FileCount, current.L2FileCount)
	fmt.Printf("  Read Amplification: %d, Write Amplification: %.2f, Space Amplification: %.2f\n",
		current.ReadAmplification, current.WriteAmplification, current.SpaceAmplification)
	fmt.Printf("  Flushes: %d (%d new), Compactions: %d (%d new)\n",
		current.FlushCount, flushDelta, current.CompactionCount, compactionDelta)
	fmt.Printf("  Bytes Written: %d (%d new)\n",
		current.CompactionBytesWritten, bytesWrittenDelta)
	fmt.Printf("  Pending Writes: %d\n", current.PendingWrites)
	fmt.Println("---")
}