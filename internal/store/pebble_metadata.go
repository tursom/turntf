package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/tursom/turntf/internal/clock"
)

// pebbleCursorValueVersion 是 Pebble 游标值的版本号，用于向前兼容。
const pebbleCursorValueVersion = byte(1)

// pebblePeerAckCursorRepository 是 Pebble 后端的对等节点确认游标仓库。
//
// 在事件溯源架构中，每个对等节点（peer）需要跟踪它已从某个来源节点（origin）
// 确认处理到哪个事件 ID。此仓库持久化这些游标信息。
//
// 键空间布局: metaPeerAckCursorTag + peerNodeID + originNodeID
type pebblePeerAckCursorRepository struct {
	// db 是底层的 Pebble 数据库实例
	db *pebble.DB
	// writes 是写入协调器，用于组提交优化
	writes *pebbleWriteCoordinator
	// clock 是混合逻辑时钟，用于为游标更新时间戳
	clock *clock.Clock
	// mu 保护 Upsert 操作的读-改-写原子性
	mu sync.Mutex
}

// pebbleOriginCursorRepository 是 Pebble 后端的来源节点游标仓库。
//
// 跟踪本节点已应用来自每个来源节点的最新事件 ID。
// 用于复制协议中确定哪些事件需要从对等节点拉取。
//
// 键空间布局: metaOriginCursorTag + originNodeID
type pebbleOriginCursorRepository struct {
	// db 是底层的 Pebble 数据库实例
	db *pebble.DB
	// writes 是写入协调器，用于组提交优化
	writes *pebbleWriteCoordinator
	// clock 是混合逻辑时钟，用于为游标更新时间戳
	clock *clock.Clock
	// mu 保护 Upsert 操作的读-改-写原子性
	mu sync.Mutex
}

// pebblePendingProjectionRepository 是 Pebble 后端的待处理投影仓库。
//
// 当事件投影失败时（例如消息接收者不存在），将事件记录在此处。
// 后续通过 ReplayPendingEvents 重试这些失败的投影。
//
// 键空间布局: metaPendingProjectionTag + originNodeID + eventID
type pebblePendingProjectionRepository struct {
	// db 是底层的 Pebble 数据库实例
	db *pebble.DB
	// writes 是写入协调器，用于组提交优化
	writes *pebbleWriteCoordinator
	// clock 是混合逻辑时钟，用于标记失败时间戳
	clock *clock.Clock
	// mu 保护 Record/List 等操作的原子性
	mu sync.Mutex
}

// pebblePendingProjectionRecord 是待处理投影记录的 JSON 序列化结构。
// 记录了失败投影的事件信息、重试次数和失败时间。
type pebblePendingProjectionRecord struct {
	// EventType 是失败的事件类型
	EventType string `json:"event_type"`
	// AggregateType 是聚合类型
	AggregateType string `json:"aggregate_type"`
	// AggregateNodeID 是聚合所在节点 ID
	AggregateNodeID int64 `json:"aggregate_node_id"`
	// AggregateID 是聚合 ID
	AggregateID int64 `json:"aggregate_id"`
	// AttemptCount 是已重试次数
	AttemptCount int64 `json:"attempt_count"`
	// LastError 是最近一次失败的错误信息
	LastError string `json:"last_error"`
	// FirstFailedAtHLC 是首次失败时的 HLC 时间戳
	FirstFailedAtHLC string `json:"first_failed_at_hlc"`
	// LastFailedAtHLC 是最近一次失败时的 HLC 时间戳
	LastFailedAtHLC string `json:"last_failed_at_hlc"`
}

// Get 查询指定对等节点对指定来源节点的确认游标。
// 如果游标不存在，返回空游标（不视为错误）。
func (r *pebblePeerAckCursorRepository) Get(ctx context.Context, peerNodeID, originNodeID int64) (PeerAckCursor, error) {
	if peerNodeID <= 0 {
		return PeerAckCursor{}, fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}
	if originNodeID <= 0 {
		return PeerAckCursor{}, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return PeerAckCursor{}, err
	}
	value, closer, err := r.db.Get(pebblePeerAckCursorKey(peerNodeID, originNodeID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return PeerAckCursor{PeerNodeID: peerNodeID, OriginNodeID: originNodeID}, nil
		}
		return PeerAckCursor{}, fmt.Errorf("get pebble peer ack cursor: %w", err)
	}
	defer closer.Close()

	ackedEventID, updatedAt, err := decodePebbleCursorValue(value)
	if err != nil {
		return PeerAckCursor{}, err
	}
	return PeerAckCursor{
		PeerNodeID:   peerNodeID,
		OriginNodeID: originNodeID,
		AckedEventID: ackedEventID,
		UpdatedAt:    updatedAt,
	}, nil
}

// List 列出所有对等节点确认游标。
// 遍历 metaPeerAckCursorTag 前缀下的所有键值对，解析出每个游标。
func (r *pebblePeerAckCursorRepository) List(ctx context.Context) ([]PeerAckCursor, error) {
	prefix := []byte{metaPeerAckCursorTag}
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open pebble peer ack cursor iterator: %w", err)
	}
	defer iter.Close()

	cursors := make([]PeerAckCursor, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		peerNodeID, originNodeID, err := parsePebblePeerAckCursorKey(iter.Key())
		if err != nil {
			return nil, err
		}
		ackedEventID, updatedAt, err := decodePebbleCursorValue(iter.Value())
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, PeerAckCursor{
			PeerNodeID:   peerNodeID,
			OriginNodeID: originNodeID,
			AckedEventID: ackedEventID,
			UpdatedAt:    updatedAt,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate pebble peer ack cursors: %w", err)
	}
	return cursors, nil
}

// Upsert 更新（或插入）对等节点确认游标。
// 使用更-改-写模式：先读取当前值，仅当新值更大时才更新（单调递增保证）。
// 并发安全：通过 mu 互斥锁保护。
func (r *pebblePeerAckCursorRepository) Upsert(ctx context.Context, peerNodeID, originNodeID, ackedEventID int64) error {
	if peerNodeID <= 0 {
		return fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}
	if originNodeID <= 0 {
		return fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if ackedEventID < 0 {
		return fmt.Errorf("%w: acked event id cannot be negative", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := pebblePeerAckCursorKey(peerNodeID, originNodeID)
	current, ok, err := readPebbleCursorValue(r.db, key)
	if err != nil {
		return err
	}
	if ok && ackedEventID < current {
		ackedEventID = current
	}
	return setPebbleCursorValue(r.db, r.writes, key, ackedEventID, r.clock.Now())
}

// Get 查询指定来源节点的已应用事件游标。
// 如果游标不存在，返回空游标（不视为错误）。
func (r *pebbleOriginCursorRepository) Get(ctx context.Context, originNodeID int64) (OriginCursor, error) {
	if originNodeID <= 0 {
		return OriginCursor{}, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return OriginCursor{}, err
	}
	value, closer, err := r.db.Get(pebbleOriginCursorKey(originNodeID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return OriginCursor{OriginNodeID: originNodeID}, nil
		}
		return OriginCursor{}, fmt.Errorf("get pebble origin cursor: %w", err)
	}
	defer closer.Close()

	appliedEventID, updatedAt, err := decodePebbleCursorValue(value)
	if err != nil {
		return OriginCursor{}, err
	}
	return OriginCursor{
		OriginNodeID:   originNodeID,
		AppliedEventID: appliedEventID,
		UpdatedAt:      updatedAt,
	}, nil
}

// List 列出所有来源节点游标。
// 遍历 metaOriginCursorTag 前缀下的所有键值对。
func (r *pebbleOriginCursorRepository) List(ctx context.Context) ([]OriginCursor, error) {
	prefix := []byte{metaOriginCursorTag}
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open pebble origin cursor iterator: %w", err)
	}
	defer iter.Close()

	cursors := make([]OriginCursor, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		originNodeID, err := parsePebbleOriginCursorKey(iter.Key())
		if err != nil {
			return nil, err
		}
		appliedEventID, updatedAt, err := decodePebbleCursorValue(iter.Value())
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, OriginCursor{
			OriginNodeID:   originNodeID,
			AppliedEventID: appliedEventID,
			UpdatedAt:      updatedAt,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate pebble origin cursors: %w", err)
	}
	return cursors, nil
}

// Upsert 更新（或插入）来源节点游标。
// 使用读-改-写模式，保证游标值单调递增。
// 并发安全：通过 mu 互斥锁保护。
func (r *pebbleOriginCursorRepository) Upsert(ctx context.Context, originNodeID, appliedEventID int64) error {
	if originNodeID <= 0 {
		return fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if appliedEventID < 0 {
		return fmt.Errorf("%w: applied event id cannot be negative", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := pebbleOriginCursorKey(originNodeID)
	current, ok, err := readPebbleCursorValue(r.db, key)
	if err != nil {
		return err
	}
	if ok && appliedEventID < current {
		appliedEventID = current
	}
	return setPebbleCursorValue(r.db, r.writes, key, appliedEventID, r.clock.Now())
}

// Record 记录一次失败的投影事件，供后续重试。
// 如果该事件已有失败记录，则更新重试次数、错误信息，并保留首次失败时间。
func (r *pebblePendingProjectionRepository) Record(ctx context.Context, event Event, reason error) error {
	if event.OriginNodeID <= 0 || event.EventID <= 0 {
		return fmt.Errorf("%w: pending projection event identity is required", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	message := "projection failed"
	if reason != nil && reason.Error() != "" {
		message = reason.Error()
	}
	now := r.clock.Now().String()
	key := pebblePendingProjectionKey(event.OriginNodeID, event.EventID)

	r.mu.Lock()
	defer r.mu.Unlock()

	record := pebblePendingProjectionRecord{
		EventType:        string(event.EventType),
		AggregateType:    event.Aggregate,
		AggregateNodeID:  event.AggregateNodeID,
		AggregateID:      event.AggregateID,
		AttemptCount:     1,
		LastError:        message,
		FirstFailedAtHLC: now,
		LastFailedAtHLC:  now,
	}
	// 如果已有记录，递增重试次数，保留首次失败时间
	if value, closer, err := r.db.Get(key); err == nil {
		defer closer.Close()
		current, err := decodePebblePendingProjectionRecord(value)
		if err != nil {
			return err
		}
		record.AttemptCount = current.AttemptCount + 1
		record.FirstFailedAtHLC = current.FirstFailedAtHLC
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("read pebble pending projection: %w", err)
	}

	value, err := encodePebblePendingProjectionRecord(record)
	if err != nil {
		return err
	}
	return applyPebbleValueSet(r.db, r.writes, key, value, false)
}

// Clear 清除指定事件的待处理投影记录。
// 如果事件已成功投影，调用此方法移除其失败记录。
func (r *pebblePendingProjectionRepository) Clear(ctx context.Context, originNodeID, eventID int64) error {
	if originNodeID <= 0 || eventID <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := applyPebbleValueDelete(r.db, r.writes, pebblePendingProjectionKey(originNodeID, eventID), false); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("clear pebble pending projection: %w", err)
	}
	return nil
}

// List 列出待处理投影事件，按最后失败时间升序排列。
// limit 限制返回数量（0-1000，超出时使用默认值 100）。
// 排序策略：先按 LastFailedAt 升序（最早失败优先），再按 (OriginNodeID, EventID) 升序。
func (r *pebblePendingProjectionRepository) List(ctx context.Context, limit int) ([]pendingProjectionEnvelope, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	prefix := []byte{metaPendingProjectionTag}
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open pebble pending projection iterator: %w", err)
	}
	defer iter.Close()

	items := make([]pendingProjectionEnvelope, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		originNodeID, eventID, err := parsePebblePendingProjectionKey(iter.Key())
		if err != nil {
			return nil, err
		}
		record, err := decodePebblePendingProjectionRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		lastFailedAt, err := clock.ParseTimestamp(record.LastFailedAtHLC)
		if err != nil {
			return nil, fmt.Errorf("parse pebble pending projection last_failed_at: %w", err)
		}
		items = append(items, pendingProjectionEnvelope{
			OriginNodeID: originNodeID,
			EventID:      eventID,
			Record:       record,
			LastFailedAt: lastFailedAt,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate pebble pending projections: %w", err)
	}

	sort.Slice(items, func(i, j int) bool {
		if cmp := items[i].LastFailedAt.Compare(items[j].LastFailedAt); cmp != 0 {
			return cmp < 0
		}
		if items[i].OriginNodeID != items[j].OriginNodeID {
			return items[i].OriginNodeID < items[j].OriginNodeID
		}
		return items[i].EventID < items[j].EventID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

// Stats 返回待处理投影的统计信息，包括总数和最近失败时间。
func (r *pebblePendingProjectionRepository) Stats(ctx context.Context) (ProjectionStats, error) {
	prefix := []byte{metaPendingProjectionTag}
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return ProjectionStats{}, fmt.Errorf("open pebble pending projection stats iterator: %w", err)
	}
	defer iter.Close()

	var (
		total      int64
		lastFailed *clock.Timestamp
	)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return ProjectionStats{}, err
		}
		record, err := decodePebblePendingProjectionRecord(iter.Value())
		if err != nil {
			return ProjectionStats{}, err
		}
		last, err := clock.ParseTimestamp(record.LastFailedAtHLC)
		if err != nil {
			return ProjectionStats{}, fmt.Errorf("parse pebble pending projection stats timestamp: %w", err)
		}
		total++
		if lastFailed == nil || last.Compare(*lastFailed) > 0 {
			lastCopy := last
			lastFailed = &lastCopy
		}
	}
	if err := iter.Error(); err != nil {
		return ProjectionStats{}, fmt.Errorf("iterate pebble pending projection stats: %w", err)
	}
	return ProjectionStats{
		PendingTotal: total,
		LastFailedAt: lastFailed,
	}, nil
}

// pendingProjectionEnvelope 是待处理投影的查询结果包装结构。
// 包含解析后的键信息和记录数据以及 HLC 时间戳。
type pendingProjectionEnvelope struct {
	OriginNodeID int64
	EventID      int64
	Record       pebblePendingProjectionRecord
	LastFailedAt clock.Timestamp
}

// pebblePeerAckCursorKey 构造对等节点确认游标的 Pebble 键。
// 格式: [metaPeerAckCursorTag, peerNodeID(uint64), originNodeID(uint64)]，总长 17 字节。
func pebblePeerAckCursorKey(peerNodeID, originNodeID int64) []byte {
	buf := make([]byte, 0, 17)
	buf = append(buf, metaPeerAckCursorTag)
	buf = encodeUint64(buf, uint64(peerNodeID))
	return encodeUint64(buf, uint64(originNodeID))
}

// pebbleOriginCursorKey 构造来源节点游标的 Pebble 键。
// 格式: [metaOriginCursorTag, originNodeID(uint64)]，总长 9 字节。
func pebbleOriginCursorKey(originNodeID int64) []byte {
	buf := make([]byte, 0, 9)
	buf = append(buf, metaOriginCursorTag)
	return encodeUint64(buf, uint64(originNodeID))
}

// pebblePendingProjectionKey 构造待处理投影记录的 Pebble 键。
// 格式: [metaPendingProjectionTag, originNodeID(uint64), eventID(uint64)]，总长 17 字节。
func pebblePendingProjectionKey(originNodeID, eventID int64) []byte {
	buf := make([]byte, 0, 17)
	buf = append(buf, metaPendingProjectionTag)
	buf = encodeUint64(buf, uint64(originNodeID))
	return encodeUint64(buf, uint64(eventID))
}

// parsePebblePeerAckCursorKey 解析对等节点确认游标键，返回 peerNodeID 和 originNodeID。
func parsePebblePeerAckCursorKey(key []byte) (int64, int64, error) {
	if len(key) != 17 || key[0] != metaPeerAckCursorTag {
		return 0, 0, fmt.Errorf("parse pebble peer ack cursor key %q: invalid format", key)
	}
	return int64(decodeUint64(key[1:9])), int64(decodeUint64(key[9:17])), nil
}

// parsePebbleOriginCursorKey 解析来源节点游标键，返回 originNodeID。
func parsePebbleOriginCursorKey(key []byte) (int64, error) {
	if len(key) != 9 || key[0] != metaOriginCursorTag {
		return 0, fmt.Errorf("parse pebble origin cursor key %q: invalid format", key)
	}
	return int64(decodeUint64(key[1:9])), nil
}

// parsePebblePendingProjectionKey 解析待处理投影键，返回 originNodeID 和 eventID。
func parsePebblePendingProjectionKey(key []byte) (int64, int64, error) {
	if len(key) != 17 || key[0] != metaPendingProjectionTag {
		return 0, 0, fmt.Errorf("parse pebble pending projection key %q: invalid format", key)
	}
	return int64(decodeUint64(key[1:9])), int64(decodeUint64(key[9:17])), nil
}

// readPebbleCursorValue 读取 Pebble 游标值，返回已确认/已应用的事件 ID。
// 如果键不存在，返回 (0, false, nil)。
// 用于 Upsert 操作前的读-改-写检查。
func readPebbleCursorValue(db *pebble.DB, key []byte) (int64, bool, error) {
	value, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read pebble cursor value: %w", err)
	}
	defer closer.Close()

	id, _, err := decodePebbleCursorValue(value)
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// setPebbleCursorValue 设置 Pebble 游标值，包含事件 ID 和 HLC 时间戳。
func setPebbleCursorValue(db *pebble.DB, writes *pebbleWriteCoordinator, key []byte, id int64, updatedAt clock.Timestamp) error {
	return applyPebbleValueSet(db, writes, key, encodePebbleCursorValue(id, updatedAt.String()), false)
}

// applyPebbleValueSet 设置一个 Pebble 键值对（通过批处理）。
// 创建新批次、写入键值、通过协调器提交。
func applyPebbleValueSet(db *pebble.DB, writes *pebbleWriteCoordinator, key, value []byte, forceSync bool) error {
	if db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}
	batch := db.NewBatch()
	if err := batch.Set(key, value, nil); err != nil {
		_ = batch.Close()
		return err
	}
	return applyPebbleBatch(batch, writes, forceSync)
}

// applyPebbleValueDelete 删除一个 Pebble 键值对（通过批处理）。
func applyPebbleValueDelete(db *pebble.DB, writes *pebbleWriteCoordinator, key []byte, forceSync bool) error {
	if db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}
	batch := db.NewBatch()
	if err := batch.Delete(key, nil); err != nil {
		_ = batch.Close()
		return err
	}
	return applyPebbleBatch(batch, writes, forceSync)
}

// encodePebbleCursorValue 编码游标值为二进制格式。
// 格式: [version(1B), eventID(int64 BE), timestampLen(4B BE), timestamp(string)]。
func encodePebbleCursorValue(id int64, updatedAt string) []byte {
	raw := []byte(updatedAt)
	value := make([]byte, 1+8+4+len(raw))
	value[0] = pebbleCursorValueVersion
	copy(value[1:9], encodeInt64(id))
	copy(value[9:13], encodeInt64(int64(len(raw)))[4:])
	copy(value[13:], raw)
	return value
}

// decodePebbleCursorValue 解码游标值，返回事件 ID 和 HLC 时间戳。
// 验证版本号、长度一致性。
func decodePebbleCursorValue(value []byte) (int64, clock.Timestamp, error) {
	if len(value) < 13 {
		return 0, clock.Timestamp{}, fmt.Errorf("%w: invalid pebble cursor value length %d", ErrInvalidInput, len(value))
	}
	if value[0] != pebbleCursorValueVersion {
		return 0, clock.Timestamp{}, fmt.Errorf("%w: unsupported pebble cursor value version %d", ErrInvalidInput, value[0])
	}
	id := decodeInt64(value[1:9])
	rawLen := int(decodeInt64(append(make([]byte, 4), value[9:13]...)))
	if rawLen < 0 || len(value) != 13+rawLen {
		return 0, clock.Timestamp{}, fmt.Errorf("%w: invalid pebble cursor timestamp length %d", ErrInvalidInput, rawLen)
	}
	updatedAt, err := clock.ParseTimestamp(string(value[13:]))
	if err != nil {
		return 0, clock.Timestamp{}, fmt.Errorf("parse pebble cursor timestamp: %w", err)
	}
	return id, updatedAt, nil
}

// encodePebblePendingProjectionRecord 将待处理投影记录编码为 JSON。
func encodePebblePendingProjectionRecord(record pebblePendingProjectionRecord) ([]byte, error) {
	value, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal pebble pending projection: %w", err)
	}
	return value, nil
}

// decodePebblePendingProjectionRecord 从 JSON 解码待处理投影记录。
func decodePebblePendingProjectionRecord(value []byte) (pebblePendingProjectionRecord, error) {
	var record pebblePendingProjectionRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return pebblePendingProjectionRecord{}, fmt.Errorf("unmarshal pebble pending projection: %w", err)
	}
	return record, nil
}
