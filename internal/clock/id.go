// 本文件提供全局唯一 ID 的生成能力。
// 包含两类生成器：
//   - GenerateNodeID：生成 HLC Clock 所需的节点标识（NodeID），混合当前时间
//     和密码学安全随机数，保证分布式节点间的唯一性。
//   - IDGenerator：基于 Snowflake 算法的无锁单调递增 ID 生成器，使用
//     atomic.CompareAndSwap 实现并发安全。

package clock

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"
)

// Snowflake 风格 ID 生成常量。
const (
	// maxSequence 是单毫秒内序列号的最大值（4095 = 2^12 - 1），即 12 位序列空间。
	maxSequence = 4095
	// snowflakeNodeBits 是 GenerateNodeID 结果中随机节点部分占用的位数。
	snowflakeNodeBits = 10
	// snowflakeSeqBits 是 GenerateNodeID 结果中随机序列部分占用的位数。
	snowflakeSeqBits = 12
	// snowflakeNodeMax 是随机器位部分的最大值（1023 = 2^10 - 1）。
	snowflakeNodeMax = (1 << snowflakeNodeBits) - 1
	// snowflakeSeqMax 是随机序列部分的最大值（4095 = 2^12 - 1）。
	snowflakeSeqMax = (1 << snowflakeSeqBits) - 1
	// epochMs 是自定义纪元起始时间（2026-04-12T00:00:00Z）。
	// 使用自定义纪元而非 Unix Epoch 可以缩短 ID 中时间戳部分的位数，延长 ID 空间的寿命。
	epochMs = int64(1775952000000) // 2026-04-12T00:00:00Z
)

// IDGenerator 是一个基于原子操作的、并发安全的单调递增 ID 生成器。
// 采用 Snowflake 算法思想，将时间戳（自 epochMs 起算的毫秒数）和序列号打包
// 到一个 uint64 中：高位为毫秒数（52 位），低位为序列号（12 位）。
// 使用 atomic.CompareAndSwap 实现无锁并发，多个 goroutine 可同时安全调用。
// 零值可用，无需额外初始化。
type IDGenerator struct {
	state atomic.Uint64 // 打包后的内部状态：高 52 位为毫秒数，低 12 位为序列号
}

// GenerateNodeID 生成一个用于 HLC Clock 的节点 ID（NodeID）。
// 节点 ID 的位布局（从高位到低位）：
//   - 高 N 位：自 epochMs 以来的毫秒数（保证节点 ID 携带时间信息，大致有序）
//   - 中间 10 位：随机节点号（crypto/rand 生成，降低不同节点 ID 冲突概率）
//   - 低 12 位：随机序列号（进一步增加随机性）
//
// 使用 crypto/rand 作为随机源，确保不同节点间 ID 的不可预测性和唯一性。
// 生成的 ID 必定为正 int64 值。
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

// NewIDGenerator 创建一个新的 ID 生成器。
// 初始状态下内部原子值为 0，首次调用 Next() 时会通过 unpackIDState(0) 的特殊处理
// 自动初始化为当前时间，序列号为 0。
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

// Next 生成下一个全局唯一且单调递增的 ID。
// 使用 CAS（Compare-And-Swap）循环实现无锁并发，多个 goroutine 可同时安全调用。
//
// 生成规则：
//   - 当前毫秒数大于上次的毫秒数：使用当前毫秒数，序列号重置为 0
//   - 当前毫秒数等于上次的毫秒数：递增序列号（通过原子状态值加 1 实现）
//   - 序列号达到 maxSequence（4095）：自旋等待下一毫秒再生成
//   - 发生时钟回拨（当前毫秒数小于上次）：使用上次的毫秒数，继续递增序列号
//
// 返回的 int64 值在数值上严格递增，适合用作数据库主键或分布式追踪 ID。
func (g *IDGenerator) Next() int64 {
	for {
		current := g.state.Load()
		lastMs, sequence := unpackIDState(current)

		nowMs := currentIDTimeMs()
		if nowMs < lastMs {
			// 时钟回拨保护：使用上次的毫秒数，防止 ID 冲突
			nowMs = lastMs
		}

		var next uint64
		if nowMs == lastMs {
			if sequence >= maxSequence {
				// 同一毫秒内序列号已耗尽：等待系统时钟进入下一毫秒
				nowMs = waitNextMs(lastMs)
				next = packIDState(nowMs, 0)
			} else {
				// 同一毫秒内：递增序列号（整体加 1 即可，因序列号在低位）
				next = current + 1
			}
		} else {
			// 进入新的一毫秒：使用新毫秒数，序列号重置为 0
			next = packIDState(nowMs, 0)
		}

		if g.state.CompareAndSwap(current, next) {
			return int64(next)
		}
		// CAS 失败说明有其他 goroutine 并发修改了 state，重新读取并重试
	}
}

// waitNextMs 自旋等待系统时钟进入下一毫秒。
// lastMs 是当前毫秒数，函数会持续检查直到 currentIDTimeMs() > lastMs。
// 每次检查间隔约 1 毫秒，用于处理单毫秒内序列号空间耗尽的情况。
func waitNextMs(lastMs int64) int64 {
	for {
		nowMs := currentIDTimeMs()
		if nowMs > lastMs {
			return nowMs
		}
		time.Sleep(time.Millisecond)
	}
}

// currentIDTimeMs 返回自 epochMs（2026-04-12T00:00:00Z）以来的毫秒数。
// 如果系统当前时间早于 epochMs，返回 0 以保证 ID 不会为负值。
func currentIDTimeMs() int64 {
	nowMs := time.Now().UTC().UnixMilli() - epochMs
	if nowMs < 0 {
		return 0
	}
	return nowMs
}

// packIDState 将毫秒数和序列号打包为一个 uint64。
// 毫秒数占用高 52 位（左移 12 位），序列号占用低 12 位。
// 序列号 0-4095 对应 0x000-0xFFF，刚好占满低 12 位。
func packIDState(ms int64, sequence uint16) uint64 {
	return (uint64(ms) << 12) | uint64(sequence)
}

// unpackIDState 将 uint64 状态解包为毫秒数和序列号。
// 特殊处理 state == 0 的情况：返回 lastMs = -1、sequence = maxSequence（4095）。
// 这样在 Next() 的首次调用中，currentIDTimeMs() >= 0 > -1 触发"新毫秒"分支，
// 自动将状态初始化为当前时间和序列号 0。
func unpackIDState(state uint64) (int64, uint16) {
	if state == 0 {
		return -1, maxSequence
	}
	return int64(state >> 12), uint16(state & maxSequence)
}

// randomUint16 在 [minValue, maxValue] 闭区间内生成一个密码学安全的随机 uint16 值。
// 使用 crypto/rand 作为随机源，适用于生成本机节点 ID 等安全性敏感的随机数。
// 当 minValue < 0 或 maxValue < minValue 时返回错误。
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

// randomInt64 在 [minValue, maxValue] 闭区间内生成一个密码学安全的随机 int64 值。
// 使用 crypto/rand 作为随机源。当前包内未被直接调用，保留供外部使用。
// 当 minValue < 0 或 maxValue < minValue 时返回错误。
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
