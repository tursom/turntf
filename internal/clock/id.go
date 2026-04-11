package clock

import (
	"crypto/rand"
	"fmt"
	"math/big"
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

func GenerateNodeID() (int64, error) {
	nodeID, err := randomUint16(1, MaxNodeID)
	if err != nil {
		return 0, fmt.Errorf("generate node machine id: %w", err)
	}
	sequence, err := randomUint16(0, maxSequence)
	if err != nil {
		return 0, fmt.Errorf("generate node sequence: %w", err)
	}

	nowMs := time.Now().UTC().UnixMilli() - epochMs
	if nowMs < 0 {
		nowMs = 0
	}
	return (nowMs << 22) | (int64(nodeID) << 12) | int64(sequence), nil
}

func NodeSlotFromID(id int64) (uint16, error) {
	if id <= 0 {
		return 0, fmt.Errorf("node id must be positive")
	}
	slot := uint16((id >> 12) & MaxNodeID)
	if slot == 0 {
		return 0, fmt.Errorf("node id %d has empty slot", id)
	}
	if slot > MaxNodeID {
		return 0, fmt.Errorf("node id %d has slot %d exceeding max %d", id, slot, MaxNodeID)
	}
	return slot, nil
}

func NodeSequenceFromID(id int64) (uint16, error) {
	if _, err := NodeSlotFromID(id); err != nil {
		return 0, err
	}
	return uint16(id & maxSequence), nil
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

func randomUint16(minValue, maxValue int) (uint16, error) {
	if minValue < 0 || maxValue < minValue {
		return 0, fmt.Errorf("invalid random range %d..%d", minValue, maxValue)
	}
	span := big.NewInt(int64(maxValue - minValue + 1))
	value, err := rand.Int(rand.Reader, span)
	if err != nil {
		return 0, err
	}
	return uint16(value.Int64() + int64(minValue)), nil
}
