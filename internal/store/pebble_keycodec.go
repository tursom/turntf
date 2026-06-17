package store

import (
	"bytes"
	"encoding/binary"

	"github.com/tursom/turntf/internal/clock"
)

// Pebble 键空间前缀标签常量。
//
// 键空间按前缀字节划分为三大区域：
//   - 0x01~0x06：元数据区域，存储全局序列号、游标等系统状态
//   - 0x10~0x11：事件日志区域，存储事件序列和来源追踪
//   - 0x20~0x25：消息区域，存储消息本体及各种索引（用户维度、生产者维度、会话维度、收件箱等）
//
// 每类键使用不同的前缀字节进行隔离，确保不同数据类型的键不会相互冲突，
// 同时利用 Pebble 的有序特性实现高效的范围扫描。
const (
	// metaEventSequenceTag    = 全局事件序列号的元数据键前缀
	metaEventSequenceTag byte = 0x01
	// metaMessageSequenceTag  = 消息序列号的元数据键前缀
	metaMessageSequenceTag byte = 0x02
	// metaMessageUserStateTag = 用户消息状态的元数据键前缀
	metaMessageUserStateTag byte = 0x03
	// metaPeerAckCursorTag    = 对端确认游标的元数据键前缀
	metaPeerAckCursorTag byte = 0x04
	// metaOriginCursorTag     = 来源游标的元数据键前缀
	metaOriginCursorTag byte = 0x05
	// metaPendingProjectionTag = 待处理投影的元数据键前缀
	metaPendingProjectionTag byte = 0x06

	// eventSeqTag    = 事件日志序列索引键前缀（按单调递增序列号索引事件）
	eventSeqTag byte = 0x10
	// eventOriginTag = 事件来源索引键前缀（按来源节点+来源序列号索引事件）
	eventOriginTag byte = 0x11

	// messageIDTag          = 消息主键索引前缀（按接收者+生产者+序列号唯一定位一条消息）
	messageIDTag byte = 0x20
	// messageUserTag        = 消息用户维度索引前缀（按接收者+时间范围检索消息，时间倒序）
	messageUserTag byte = 0x21
	// messageProducerTag    = 消息生产者维度索引前缀（按消息生产者+时间范围检索，时间倒序）
	messageProducerTag byte = 0x22
	// messageSessionTag     = 消息会话维度索引前缀（按发送者/接收者对+时间范围检索，时间倒序）
	messageSessionTag byte = 0x23
	// messageInboxTag       = 用户收件箱索引前缀（按收件箱所有者+时间范围检索，时间倒序）
	messageInboxTag byte = 0x24
	// messageInboxSourceTag = 收件箱来源反向索引前缀（从消息反向查找所有收件箱条目）
	messageInboxSourceTag byte = 0x25
)

// encodeUint64 将 v 编码为大端序 8 字节，追加到 buf 末尾并返回。
func encodeUint64(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

// decodeUint64 从 b[0:8] 读取大端序 uint64。
func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// encodeUint64Desc 将 v 按位取反后编码为大端序 8 字节追加到 buf 末尾。
// 利用取反实现降序排列：原始值越大，取反后的编码越小，Pebble 遍历时大值在前。
func encodeUint64Desc(buf []byte, v uint64) []byte {
	return encodeUint64(buf, ^v)
}

// decodeUint64Desc 从 b[0:8] 读取降序编码的 uint64（与 encodeUint64Desc 对应）。
func decodeUint64Desc(b []byte) uint64 {
	return ^decodeUint64(b)
}

// encodeTimestamp 将 HLC 时间戳编码为 18 字节定长格式追加到 buf 末尾：
//
//	[WallTimeMs:8 BE][Logical:2 BE][NodeID:8 BE]
//
// 18 字节定长编码保证时间戳可以按字典序比较（WallTimeMs 在前）。
func encodeTimestamp(buf []byte, ts clock.Timestamp) []byte {
	buf = encodeUint64(buf, uint64(ts.WallTimeMs))
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], ts.Logical)
	buf = append(buf, tmp[:]...)
	return encodeUint64(buf, uint64(ts.NodeID))
}

// decodeTimestamp 从 18 字节编码中解码 HLC 时间戳（与 encodeTimestamp 对应）。
func decodeTimestamp(b []byte) clock.Timestamp {
	return clock.Timestamp{
		WallTimeMs: int64(decodeUint64(b[0:8])),
		Logical:    binary.BigEndian.Uint16(b[8:10]),
		NodeID:     int64(decodeUint64(b[10:18])),
	}
}

// encodeTimestampDesc 将时间戳编码为 18 字节降序格式追加到 buf 末尾。
// 对 encodeTimestamp 输出的每个字节取反，使得较新的时间戳在 Pebble 遍历时排在前面。
func encodeTimestampDesc(buf []byte, ts clock.Timestamp) []byte {
	off := len(buf)
	buf = encodeTimestamp(buf, ts)
	for i := off; i < len(buf); i++ {
		buf[i] ^= 0xff
	}
	return buf
}

// decodeTimestampDesc 从 18 字节降序编码中解码 HLC 时间戳（与 encodeTimestampDesc 对应）。
func decodeTimestampDesc(b []byte) clock.Timestamp {
	flipped := make([]byte, 18)
	for i := range flipped {
		flipped[i] = b[i] ^ 0xff
	}
	return decodeTimestamp(flipped)
}

// prefixUpperBound 返回一个键，它是所有具有给定前缀的键的最小上界。
// 实现方式：从最右字节开始递增进位，遇到非 0xff 的字节则加 1 并截断返回。
// 返回 nil 表示前缀全为 0xff 字节，不存在有界上界。
//
// 用于 Pebble 迭代时的 UpperBound 参数，配合 LowerBound=prefix 实现前缀范围扫描。
func prefixUpperBound(prefix []byte) []byte {
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

// encodeInt64 将 int64 编码为大端序 8 字节（用于值编码，非键编码）。
func encodeInt64(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

// decodeInt64 从 value[0:8] 读取大端序 int64（用于值解码，非键解码）。
func decodeInt64(value []byte) int64 {
	return int64(binary.BigEndian.Uint64(value))
}
