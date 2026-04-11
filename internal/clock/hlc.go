package clock

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Timestamp is a lexicographically sortable HLC representation.
type Timestamp struct {
	WallTimeMs int64
	Logical    uint16
	NodeID     uint16
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%013d-%05d-%05d", t.WallTimeMs, t.Logical, t.NodeID)
}

func ParseTimestamp(raw string) (Timestamp, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 3 {
		return Timestamp{}, fmt.Errorf("invalid timestamp %q", raw)
	}

	wall, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Timestamp{}, fmt.Errorf("parse wall time: %w", err)
	}
	logical, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return Timestamp{}, fmt.Errorf("parse logical counter: %w", err)
	}
	nodeID, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return Timestamp{}, fmt.Errorf("parse node id: %w", err)
	}

	return Timestamp{
		WallTimeMs: wall,
		Logical:    uint16(logical),
		NodeID:     uint16(nodeID),
	}, nil
}

func (t Timestamp) Compare(other Timestamp) int {
	switch {
	case t.WallTimeMs < other.WallTimeMs:
		return -1
	case t.WallTimeMs > other.WallTimeMs:
		return 1
	case t.Logical < other.Logical:
		return -1
	case t.Logical > other.Logical:
		return 1
	case t.NodeID < other.NodeID:
		return -1
	case t.NodeID > other.NodeID:
		return 1
	default:
		return 0
	}
}

type Clock struct {
	mu        sync.Mutex
	nodeID    uint16
	last      Timestamp
	wallClock func() int64
	offsetMs  int64
}

func NewClock(nodeID uint16) *Clock {
	return NewClockWithSource(nodeID, currentWallTimeMs)
}

func NewClockWithSource(nodeID uint16, wallClock func() int64) *Clock {
	if wallClock == nil {
		wallClock = currentWallTimeMs
	}
	return &Clock{
		nodeID:    nodeID,
		wallClock: wallClock,
	}
}

func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nextLocked(c.adjustedWallTimeLocked())
}

func (c *Clock) Observe(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	nowMs := c.adjustedWallTimeLocked()
	maxWall := maxInt64(nowMs, c.last.WallTimeMs, remote.WallTimeMs)

	switch {
	case maxWall == c.last.WallTimeMs && maxWall == remote.WallTimeMs:
		c.last.Logical = maxUint16(c.last.Logical, remote.Logical) + 1
	case maxWall == c.last.WallTimeMs:
		c.last.Logical++
	case maxWall == remote.WallTimeMs:
		c.last.Logical = remote.Logical + 1
	default:
		c.last.Logical = 0
	}

	c.last.WallTimeMs = maxWall
	c.last.NodeID = c.nodeID
	return c.last
}

func (c *Clock) SetOffsetMs(offsetMs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsetMs = offsetMs
}

func (c *Clock) OffsetMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsetMs
}

func (c *Clock) WallTimeMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.adjustedWallTimeLocked()
}

func (c *Clock) PhysicalTimeMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wallClock()
}

func (c *Clock) nextLocked(nowMs int64) Timestamp {
	if nowMs > c.last.WallTimeMs {
		c.last.WallTimeMs = nowMs
		c.last.Logical = 0
	} else {
		c.last.Logical++
	}

	c.last.NodeID = c.nodeID
	return c.last
}

func (c *Clock) adjustedWallTimeLocked() int64 {
	return c.wallClock() + c.offsetMs
}

func currentWallTimeMs() int64 {
	return time.Now().UTC().UnixMilli()
}

func maxInt64(values ...int64) int64 {
	var max int64
	for i, value := range values {
		if i == 0 || value > max {
			max = value
		}
	}
	return max
}

func maxUint16(values ...uint16) uint16 {
	var max uint16
	for i, value := range values {
		if i == 0 || value > max {
			max = value
		}
	}
	return max
}
