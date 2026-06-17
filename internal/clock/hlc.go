// Package clock 提供混合逻辑时钟（Hybrid Logical Clock, HLC）和全局唯一 ID 生成器。
//
// HLC 是一种分布式系统时间戳机制，它结合了物理时钟（Wall Time，毫秒级 Unix 时间戳）
// 和逻辑计数器（Logical Counter），在无需原子钟或 GPS 同步的前提下提供因果一致的排序能力。
// 当物理时钟发生回拨时，HLC 自动递增逻辑计数器来维持时间戳的单调递增性，保证
// 分布式节点间的事件偏序关系（happens-before）可以被正确捕获。
//
// 核心类型：
//   - Timestamp：HLC 时间戳，由 WallTimeMs（物理时间）、Logical（逻辑计数器）和
//     NodeID（节点标识）三部分组成，其 String() 输出可字典序排序。
//   - Clock：HLC 时钟的核心实现，通过互斥锁保证并发安全，提供 Now() 本地生成和
//     Observe() 追赶远程时间戳两种递进方式。
//   - IDGenerator：基于 Snowflake 算法的无锁单调递增 ID 生成器，使用 atomic CAS
//     操作实现并发安全。
package clock

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Timestamp 表示一个可字典序排序的 HLC 时间戳。
// 由三部分组成：毫秒级 Unix 时间戳、16 位逻辑计数器、节点标识。
// String() 方法的零填充格式保证了字符串形式的字典序与数值大小序一致，适合作为
// 数据库主键或分布式日志中的排序键。
type Timestamp struct {
	WallTimeMs int64  // 物理时钟时间，自 Unix Epoch 以来的毫秒数（UTC），来自系统时钟
	Logical    uint16 // 逻辑计数器，同一毫秒内的递增序号，解决物理时间分辨率不足的问题
	NodeID     int64  // 节点标识，生成该时间戳的节点唯一编号，用于打破全局平局
}

// String 返回 Timestamp 的固定宽度字符串表示，格式为 "WallTimeMs-Logical-NodeID"。
// WallTimeMs 零填充到 13 位、Logical 零填充到 5 位、NodeID 零填充到 19 位，
// 确保字符串字典序与时间戳的数值大小序一致。此属性使得该字符串可直接用于
// 数据库或分布式键值存储的范围查询。
func (t Timestamp) String() string {
	return fmt.Sprintf("%013d-%05d-%019d", t.WallTimeMs, t.Logical, t.NodeID)
}

// ParseTimestamp 解析 String() 生成的字符串，将其还原为 Timestamp 结构体。
// raw 参数必须严格符合 "WallTimeMs-Logical-NodeID" 格式及各部分的宽度要求：
// WallTimeMs 13 位、Logical 5 位、NodeID 19 位，且 NodeID 必须为正数。
// 解析失败时返回包含详细原因的错误，用于协助排查格式不匹配的问题。
func ParseTimestamp(raw string) (Timestamp, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 3 {
		return Timestamp{}, fmt.Errorf("invalid timestamp %q", raw)
	}

	wall, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Timestamp{}, fmt.Errorf("parse wall time: %w", err)
	}
	if len(parts[0]) != 13 {
		return Timestamp{}, fmt.Errorf("invalid wall time width in %q", raw)
	}
	logical, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return Timestamp{}, fmt.Errorf("parse logical counter: %w", err)
	}
	if len(parts[1]) != 5 {
		return Timestamp{}, fmt.Errorf("invalid logical counter width in %q", raw)
	}
	nodeID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Timestamp{}, fmt.Errorf("parse node id: %w", err)
	}
	if len(parts[2]) != 19 {
		return Timestamp{}, fmt.Errorf("invalid node id width in %q", raw)
	}
	if nodeID <= 0 {
		return Timestamp{}, fmt.Errorf("node id must be positive")
	}

	return Timestamp{
		WallTimeMs: wall,
		Logical:    uint16(logical),
		NodeID:     nodeID,
	}, nil
}

// Compare 比较两个 Timestamp 的先后顺序。
// 先比较 WallTimeMs，若相等则比较 Logical，若仍然相等则比较 NodeID。
// 返回 -1 表示 t < other（t 时间更早），0 表示相等，1 表示 t > other（t 时间更晚）。
// 这种比较方式完整实现了 HLC 的偏序关系：（wall, logical, nodeID）的三元组字典序。
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

// Clock 是一个并发安全的 HLC 混合逻辑时钟。
// 通过 sync.Mutex 互斥锁保护内部状态，提供 Now() 本地生成时间戳和 Observe() 追赶
// 远程时间戳两种递进方式。零值不可使用，必须通过 NewClock 或 NewClockWithSource 创建。
type Clock struct {
	mu        sync.Mutex   // 保护以下所有字段的互斥锁
	nodeID    int64        // 本节点标识，在创建时确定且不可变更
	last      Timestamp    // 最近一次生成的时间戳，用于维持 HLC 单调递增性
	wallClock func() int64 // 物理时钟源函数，返回毫秒级 Unix 时间戳（UTC）
	offsetMs  int64        // 时钟偏移量，对 wallClock 的返回值进行调整，单位毫秒
}

// NewClock 使用默认的物理时钟源创建 HLC 时钟。
// nodeID 是当前节点的唯一标识，必须为正整数，通常由 GenerateNodeID() 生成的
// 分布式唯一值。默认时钟源为 time.Now().UTC().UnixMilli()。
func NewClock(nodeID int64) *Clock {
	return NewClockWithSource(nodeID, currentWallTimeMs)
}

// NewClockWithSource 使用自定义物理时钟源创建 HLC 时钟。
// wallClock 函数应返回毫秒级 Unix 时间戳（UTC），传入 nil 时回退到默认实现。
// 自定义时钟源的主要用途是在测试中模拟时钟回拨、时间跳跃等边界场景，
// 使 HLC 算法的正确性可以被完整验证。
func NewClockWithSource(nodeID int64, wallClock func() int64) *Clock {
	if wallClock == nil {
		wallClock = currentWallTimeMs
	}
	return &Clock{
		nodeID:    nodeID,
		wallClock: wallClock,
	}
}

// Now 生成一个新的 HLC 时间戳。
// 内部先获取调整后的物理时间（wallClock + offsetMs），然后调用 nextLocked 执行
// HLC 核心递增逻辑：如果新物理时间大于上次时间戳的物理时间，则更新物理时间并重置
// 逻辑计数器为 0；否则（包括物理时间停滞或回拨的情况）递增逻辑计数器以维持单调性。
// 返回的时间戳的 NodeID 被设置为当前节点的 nodeID。
// 线程安全。
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nextLocked(c.adjustedWallTimeLocked())
}

// Observe 接收一个来自远程节点的时间戳，将其与本地时钟状态合并，返回更新后的本地时间戳。
// 这是 HLC 实现因果一致性的关键方法：当看到其他节点产生的时间戳时，本地时钟必须"追赶"
// 到至少与远程时间戳相同的水平，从而保证跨节点的事件偏序关系可以被正确识别。
//
// 算法步骤：
//  1. 获取当前调整后的物理时间 nowMs
//  2. 取 nowMs、本地上次时间戳的 WallTimeMs、远程时间戳的 WallTimeMs 三者的最大值 maxWall
//  3. 根据 maxWall 的来源决定逻辑计数器的取值：
//     - maxWall 同时等于本地和远程的 WallTimeMs：取两者逻辑计数器的最大值再加 1
//     - maxWall 仅等于本地的 WallTimeMs：将本地逻辑计数器加 1
//     - maxWall 仅等于远程的 WallTimeMs：将远程逻辑计数器加 1
//     - maxWall 来自 nowMs（物理时钟前进到了新值）：重置逻辑计数器为 0
//  4. 更新本地状态：WallTimeMs = maxWall，Logical = 新值，NodeID = 本节点 ID
//
// 线程安全。
func (c *Clock) Observe(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	nowMs := c.adjustedWallTimeLocked()
	maxWall := maxInt64(nowMs, c.last.WallTimeMs, remote.WallTimeMs)

	switch {
	case maxWall == c.last.WallTimeMs && maxWall == remote.WallTimeMs:
		// 本地和远程的 WallTimeMs 相同且都是最大值：取两者逻辑计数器的最大值再递增
		c.last.Logical = maxUint16(c.last.Logical, remote.Logical) + 1
	case maxWall == c.last.WallTimeMs:
		// 本地 WallTimeMs 最大：递增本地逻辑计数器
		c.last.Logical++
	case maxWall == remote.WallTimeMs:
		// 远程 WallTimeMs 最大：追赶远程逻辑计数器再加 1
		c.last.Logical = remote.Logical + 1
	default:
		// 当前物理时钟前进到了新的最大值：逻辑计数器重置为 0
		c.last.Logical = 0
	}

	c.last.WallTimeMs = maxWall
	c.last.NodeID = c.nodeID
	return c.last
}

// SetOffsetMs 设置时钟偏移量，单位毫秒。
// 偏移量会被叠加到物理时钟源的值上，用于修正本地时钟与预期标准时间之间的偏差。
// 正偏移表示将当前时间"向前"调整，负偏移表示"向后"调整。
// 线程安全。
func (c *Clock) SetOffsetMs(offsetMs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsetMs = offsetMs
}

// OffsetMs 返回当前时钟偏移量，单位毫秒。
// 线程安全。
func (c *Clock) OffsetMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsetMs
}

// WallTimeMs 返回调整后的物理时间，即 wallClock() + offsetMs，单位毫秒。
// 该值反映的是经过偏移校正后的"本地视角"的物理时间，用于 HLC 的时间戳生成。
// 线程安全。
func (c *Clock) WallTimeMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.adjustedWallTimeLocked()
}

// PhysicalTimeMs 返回未经偏移调整的原始物理时钟值，即 wallClock() 的返回值，单位毫秒。
// 与 WallTimeMs 不同，该值不包含 offsetMs 偏移量，反映的是未经修正的底层时钟源的值。
// 线程安全。
func (c *Clock) PhysicalTimeMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wallClock()
}

// nextLocked 在已持有互斥锁的前提下执行 HLC 时间戳递增逻辑。
// 如果新物理时间 nowMs 大于上次的 WallTimeMs，则使用新物理时间并重置逻辑计数器为 0；
// 否则（包括物理时间不变或回拨）递增逻辑计数器。
// 调用者必须持有 c.mu 锁。返回的 Timestamp 的 NodeID 会被设置为当前节点的 nodeID。
func (c *Clock) nextLocked(nowMs int64) Timestamp {
	if nowMs > c.last.WallTimeMs {
		// 物理时钟前进了新的毫秒：更新物理时间，重置逻辑计数器
		c.last.WallTimeMs = nowMs
		c.last.Logical = 0
	} else {
		// 物理时钟停滞或回拨：递增逻辑计数器以维持单调递增性
		c.last.Logical++
	}

	c.last.NodeID = c.nodeID
	return c.last
}

// adjustedWallTimeLocked 返回调整后的物理时间，即 wallClock() + offsetMs。
// 调用者必须持有 c.mu 锁。
func (c *Clock) adjustedWallTimeLocked() int64 {
	return c.wallClock() + c.offsetMs
}

// currentWallTimeMs 返回 UTC 时间的毫秒级 Unix 时间戳，作为默认的物理时钟源。
func currentWallTimeMs() int64 {
	return time.Now().UTC().UnixMilli()
}

// maxInt64 返回一组 int64 值中的最大值。传入切片为空时返回零值。
func maxInt64(values ...int64) int64 {
	var max int64
	for i, value := range values {
		if i == 0 || value > max {
			max = value
		}
	}
	return max
}

// maxUint16 返回一组 uint16 值中的最大值。传入切片为空时返回零值。
func maxUint16(values ...uint16) uint16 {
	var max uint16
	for i, value := range values {
		if i == 0 || value > max {
			max = value
		}
	}
	return max
}
