package clock

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"
)

const (
	maxSequence = 4095
	epochMs     = int64(1735689600000) // 2025-01-01T00:00:00Z
)

type IDGenerator struct {
	mu       sync.Mutex
	lastMs   int64
	sequence uint16
}

func GenerateNodeID() (int64, error) {
	nodeID, err := randomInt64(1, math.MaxInt64)
	if err != nil {
		return 0, fmt.Errorf("generate node id: %w", err)
	}
	return nodeID, nil
}

func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
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
	return (nowMs << 12) | int64(g.sequence)
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

func randomInt64(minValue, maxValue int64) (int64, error) {
	if minValue < 0 || maxValue < minValue {
		return 0, fmt.Errorf("invalid random range %d..%d", minValue, maxValue)
	}
	span := big.NewInt(maxValue - minValue + 1)
	value, err := rand.Int(rand.Reader, span)
	if err != nil {
		return 0, err
	}
	return value.Int64() + minValue, nil
}
