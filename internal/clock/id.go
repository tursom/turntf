package clock

import (
	"fmt"
	"sync"
	"time"
)

const (
	MaxNodeID   = 1023
	maxSequence = 4095
	epochMs     = int64(1735689600000) // 2025-01-01T00:00:00Z
)

type IDGenerator struct {
	mu       sync.Mutex
	nodeID   uint16
	lastMs   int64
	sequence uint16
}

func NewIDGenerator(nodeID uint16) (*IDGenerator, error) {
	if nodeID > MaxNodeID {
		return nil, fmt.Errorf("node id %d exceeds max %d", nodeID, MaxNodeID)
	}
	return &IDGenerator{nodeID: nodeID}, nil
}

func (g *IDGenerator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	nowMs := time.Now().UTC().UnixMilli() - epochMs
	if nowMs < 0 {
		nowMs = 0
	}
	if nowMs < g.lastMs {
		nowMs = g.lastMs
	}

	if nowMs == g.lastMs {
		g.sequence++
		if g.sequence > maxSequence {
			nowMs = g.waitNextMsLocked(nowMs)
			g.sequence = 0
		}
	} else {
		g.sequence = 0
	}

	g.lastMs = nowMs
	return (nowMs << 22) | (int64(g.nodeID) << 12) | int64(g.sequence)
}

func (g *IDGenerator) waitNextMsLocked(lastMs int64) int64 {
	for {
		nowMs := time.Now().UTC().UnixMilli() - epochMs
		if nowMs > lastMs {
			return nowMs
		}
		time.Sleep(time.Millisecond)
	}
}
