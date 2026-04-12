package clock

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"
)

const (
	maxSequence       = 4095
	snowflakeNodeBits = 10
	snowflakeSeqBits  = 12
	snowflakeNodeMax  = (1 << snowflakeNodeBits) - 1
	snowflakeSeqMax   = (1 << snowflakeSeqBits) - 1
	epochMs           = int64(1775952000000) // 2026-04-12T00:00:00Z
)

type IDGenerator struct {
	state atomic.Uint64
}

func GenerateNodeID() (int64, error) {
	nowMs := currentIDTimeMs()
	randomNode, err := randomUint16(0, snowflakeNodeMax)
	if err != nil {
		return 0, fmt.Errorf("generate node bits: %w", err)
	}
	randomSeq, err := randomUint16(0, snowflakeSeqMax)
	if err != nil {
		return 0, fmt.Errorf("generate sequence bits: %w", err)
	}

	nodeID := (nowMs << (snowflakeNodeBits + snowflakeSeqBits)) |
		(int64(randomNode) << snowflakeSeqBits) |
		int64(randomSeq)
	if nodeID <= 0 {
		return 0, fmt.Errorf("generated invalid node id %d", nodeID)
	}
	return nodeID, nil
}

func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

func (g *IDGenerator) Next() int64 {
	for {
		current := g.state.Load()
		lastMs, sequence := unpackIDState(current)

		nowMs := currentIDTimeMs()
		if nowMs < lastMs {
			nowMs = lastMs
		}

		var next uint64
		if nowMs == lastMs {
			if sequence >= maxSequence {
				nowMs = waitNextMs(lastMs)
				next = packIDState(nowMs, 0)
			} else {
				next = current + 1
			}
		} else {
			next = packIDState(nowMs, 0)
		}

		if g.state.CompareAndSwap(current, next) {
			return int64(next)
		}
	}
}

func waitNextMs(lastMs int64) int64 {
	for {
		nowMs := currentIDTimeMs()
		if nowMs > lastMs {
			return nowMs
		}
		time.Sleep(time.Millisecond)
	}
}

func currentIDTimeMs() int64 {
	nowMs := time.Now().UTC().UnixMilli() - epochMs
	if nowMs < 0 {
		return 0
	}
	return nowMs
}

func packIDState(ms int64, sequence uint16) uint64 {
	return (uint64(ms) << 12) | uint64(sequence)
}

func unpackIDState(state uint64) (int64, uint16) {
	if state == 0 {
		return -1, maxSequence
	}
	return int64(state >> 12), uint16(state & maxSequence)
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
