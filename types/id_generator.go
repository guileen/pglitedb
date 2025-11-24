package types

import (
	"fmt"
	"sync"
	"time"
)

type SnowflakeIDGenerator struct {
	mu        sync.Mutex
	epoch     int64
	machineID int64
	sequence  int64
	lastTime  int64
}

func NewSnowflakeIDGenerator(machineID int64) *SnowflakeIDGenerator {
	if machineID < 0 || machineID > 1023 {
		panic("machineID must be between 0-1023")
	}

	return &SnowflakeIDGenerator{
		epoch:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		machineID: machineID,
		sequence:  0,
		lastTime:  0,
	}
}

func (g *SnowflakeIDGenerator) Next() (int64, error) {
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
