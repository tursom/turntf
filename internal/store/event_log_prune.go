package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/tursom/turntf/internal/clock"
)

// eventLogTrimStatsScope 是全局裁剪统计在 event_log_trim_stats 表中的 scope 标识。
const eventLogTrimStatsScope = "global"

// EventLogTrimStats 是自节点启动以来事件日志裁剪的累计统计。
// 存储在 SQLite event_log_trim_stats 表中，用于运维监控和历史追溯。
type EventLogTrimStats struct {
	// TrimmedTotal 自节点运行以来累计裁剪的事件总数，单调递增。
	TrimmedTotal int64
	// LastTrimmedAt 最近一次裁剪操作执行时的 HLC 时间戳。
	LastTrimmedAt *clock.Timestamp
}

// EventLogPruneResult 是 PruneEventLogOnce 单次调用返回的裁剪操作结果摘要。
type EventLogPruneResult struct {
	// TrimmedEvents 本次裁剪操作实际删除的事件总数。
	TrimmedEvents int64
	// OriginsAffected 事件数超过上限而被裁剪的来源节点个数。
	OriginsAffected int
	// MaxEventsPerOrigin 本次裁剪使用的每来源保留上限值。
	MaxEventsPerOrigin int
}

// PruneEventLogOnce 对所有来源节点执行一次事件日志裁剪，是外部调用的入口方法。
// 裁剪策略：对于每一个 origin_node_id，仅保留最新的 maxEvents 条事件，
// 删除更早的历史事件。通过 s.backend.EventLog().ListOriginProgress() 获取
// 所有来源节点的进度信息，然后逐来源调用 pruneEventLogOrigin 执行实际裁剪。
// 裁剪完成后会更新 event_log_truncation_meta（记录每个来源的裁剪边界）和
// event_log_trim_stats（全局累计统计）。
// 并发安全：由外部调用者保证同一时段仅一次裁剪操作。
func (s *Store) PruneEventLogOnce(ctx context.Context) (EventLogPruneResult, error) {
	maxEvents := normalizeEventLogMaxEventsPerOrigin(s.eventLogMaxEventsPerOrigin)
	progress, err := s.backend.EventLog().ListOriginProgress(ctx)
	if err != nil {
		return EventLogPruneResult{}, err
	}

	result := EventLogPruneResult{MaxEventsPerOrigin: maxEvents}
	for _, item := range progress {
		if item.OriginNodeID <= 0 {
			continue
		}
		trimmed, err := s.pruneEventLogOrigin(ctx, item.OriginNodeID, maxEvents)
		if err != nil {
			return EventLogPruneResult{}, err
		}
		if trimmed == 0 {
			continue
		}
		result.TrimmedEvents += trimmed
		result.OriginsAffected++
	}
	return result, nil
}

// EventLogTruncatedBefore 查询指定来源节点已被裁剪截断的最早 event_id 边界。
// 返回值为 truncated_before_event_id，小于该值的 event_id 已被裁剪删除。
// 如果该来源从未被裁剪过，则返回 0。
// 用于复制协议：对端节点应跳过已经裁剪掉的事件，避免等待永不存在的旧事件。
func (s *Store) EventLogTruncatedBefore(ctx context.Context, originNodeID int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}

	var truncatedBefore int64
	err := s.db.QueryRowContext(ctx, `
SELECT truncated_before_event_id
FROM event_log_truncation_meta
WHERE origin_node_id = ?
`, originNodeID).Scan(&truncatedBefore)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query event log truncation meta: %w", err)
	}
	return truncatedBefore, nil
}

// eventLogTrimStats 从 SQLite 数据库 event_log_trim_stats 表中读取事件日志裁剪的
// 全局统计信息。包含累计裁剪总数和最近一次裁剪的 HLC 时间戳。
// 当表中无记录时返回零值 EventLogTrimStats（TrimmedTotal=0, LastTrimmedAt=nil）。
// 仅在 Pebble 后端执行裁剪后由 recordEventLogTrim / recordEventLogTrimTx 写入。
func (s *Store) eventLogTrimStats(ctx context.Context) (EventLogTrimStats, error) {
	var stats EventLogTrimStats
	var lastTrimmedAtRaw sql.NullString

	err := s.db.QueryRowContext(ctx, `
SELECT trimmed_total, last_trimmed_at_hlc
FROM event_log_trim_stats
WHERE scope = ?
`, eventLogTrimStatsScope).Scan(&stats.TrimmedTotal, &lastTrimmedAtRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EventLogTrimStats{}, nil
		}
		return EventLogTrimStats{}, fmt.Errorf("query event log trim stats: %w", err)
	}
	if lastTrimmedAtRaw.Valid && lastTrimmedAtRaw.String != "" {
		parsed, err := clock.ParseTimestamp(lastTrimmedAtRaw.String)
		if err != nil {
			return EventLogTrimStats{}, fmt.Errorf("parse event log trim timestamp: %w", err)
		}
		stats.LastTrimmedAt = &parsed
	}
	return stats, nil
}

// pruneEventLogOrigin 对单个来源节点执行事件日志裁剪。
// 将实际裁剪操作委托给 s.backend.PruneEventLogOrigin，由底层后端
// （SQLite 或 Pebble）根据存储引擎特性实现具体的删除逻辑。
// 此方法只负责调度，裁剪记录的持久化由后端实现内部完成。
func (s *Store) pruneEventLogOrigin(ctx context.Context, originNodeID int64, maxEvents int) (int64, error) {
	return s.backend.PruneEventLogOrigin(ctx, s.db, s.clock, originNodeID, maxEvents)
}

// countPebbleOriginEvents 统计 Pebble 存储中指定来源节点的事件总数。
// 通过遍历 eventOriginTag + originNodeID 前缀范围来计数，不使用单独的计数器。
// 该方法仅在确定裁剪边界前调用，计算结果临时用于判断是否超出 maxEvents 上限。
// 由于是前缀全扫描，在事件数较大时可能较慢。
func countPebbleOriginEvents(ctx context.Context, db *pebble.DB, originNodeID int64) (int64, error) {
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, eventOriginTag)
	prefix = encodeUint64(prefix, uint64(originNodeID))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open pebble event count iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate pebble event count: %w", err)
	}
	return count, nil
}

// nthPebbleOriginEventID 获取 Pebble 中指定来源节点按 eventID 升序排列的第 offset
// （从 0 开始计数）个事件的 event_id。
// 用途：确定裁剪边界——假设某个来源有 N 条事件，maxEvents = M（N > M），
// 则第 M 个事件的 eventID 就是应保留的最早事件的 ID，更早的（offset < N-M）都应删除。
// 具体计算公式：offset = N - maxEvents，即找到应删除的最后一条事件。
// 如果 offset 超出实际范围则返回 error。
func nthPebbleOriginEventID(ctx context.Context, db *pebble.DB, originNodeID, offset int64) (int64, error) {
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, eventOriginTag)
	prefix = encodeUint64(prefix, uint64(originNodeID))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open pebble truncation boundary iterator: %w", err)
	}
	defer iter.Close()

	var index int64
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if index == offset {
			_, eventID, err := parsePebbleOriginKey(iter.Key())
			if err != nil {
				return 0, err
			}
			return eventID, nil
		}
		index++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate pebble truncation boundary: %w", err)
	}
	return 0, fmt.Errorf("pebble event offset %d for origin %d not found", offset, originNodeID)
}

// deletePebbleOriginEvents 从 Pebble 中批量删除指定来源节点的事件，最多删除 limit 条。
// 使用批处理（每批 batchSize=1024 条）提交删除操作，避免单次事务内存过高。
// 每次删除操作同时删除两条索引：
//   - 事件来源索引（eventOriginTag + originNodeID + eventID）：按来源节点范围遍历
//   - 事件序列号索引（eventSeqTag + sequence）：通过值的解码获取 sequence 后删除
//
// 函数返回值 trimmed 为实际删除的事件数，可能小于 limit（已无更多事件可删时停止）。
// 每批提交后重新创建 batch 对象（db.NewBatch()），保留原 batch 的引用覆盖。
func deletePebbleOriginEvents(ctx context.Context, db *pebble.DB, originNodeID, limit int64) (int64, error) {
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, eventOriginTag)
	prefix = encodeUint64(prefix, uint64(originNodeID))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open pebble delete iterator: %w", err)
	}
	defer iter.Close()

	const batchSize = 1024
	var trimmed int64
	batch := db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	pending := 0
	commitBatch := func() error {
		if pending == 0 {
			return nil
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("commit pebble event log prune batch: %w", err)
		}
		if err := batch.Close(); err != nil {
			return fmt.Errorf("close pebble event log prune batch: %w", err)
		}
		batch = db.NewBatch()
		pending = 0
		return nil
	}

	for valid := iter.First(); valid && trimmed < limit; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		_, eventID, err := parsePebbleOriginKey(iter.Key())
		if err != nil {
			return 0, err
		}
		sequence := decodeInt64(iter.Value())
		if err := batch.Delete(pebbleEventOriginKey(originNodeID, eventID), nil); err != nil {
			return 0, fmt.Errorf("delete pebble origin event key: %w", err)
		}
		if err := batch.Delete(pebbleEventSeqKey(sequence), nil); err != nil {
			return 0, fmt.Errorf("delete pebble event sequence key: %w", err)
		}
		trimmed++
		pending++
		if pending >= batchSize {
			if err := commitBatch(); err != nil {
				return 0, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate pebble delete iterator: %w", err)
	}
	if err := commitBatch(); err != nil {
		return 0, err
	}
	return trimmed, nil
}

// upsertEventLogTruncation 更新 SQLite 中指定来源节点的裁剪边界记录。
// truncated_before_event_id 表示小于该值的事件已被裁剪删除。
// 使用 ON CONFLICT 确保 truncated_before_event_id 只增不减——在同一来源上
// 多次裁剪后，边界只会向前推进，不会回退，这是数据安全性的关键保证。
// 同时记录 updated_at_hlc 用于追踪最近一次裁剪操作的时间。
func upsertEventLogTruncation(ctx context.Context, db *sql.DB, clk *clock.Clock, originNodeID, truncatedBefore int64) error {
	now := clk.Now().String()
	if _, err := db.ExecContext(ctx, `
INSERT INTO event_log_truncation_meta(origin_node_id, truncated_before_event_id, updated_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(origin_node_id) DO UPDATE SET
    truncated_before_event_id = CASE
        WHEN excluded.truncated_before_event_id > event_log_truncation_meta.truncated_before_event_id THEN excluded.truncated_before_event_id
        ELSE event_log_truncation_meta.truncated_before_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, originNodeID, truncatedBefore, now); err != nil {
		return fmt.Errorf("upsert event log truncation meta: %w", err)
	}
	return nil
}

// upsertEventLogTruncationTx 在已有事务中更新指定来源节点的裁剪边界记录。
// 与 upsertEventLogTruncation 的区别在于接受 exec 接口而非 *sql.DB，
// 以便在事务上下文（*sql.Tx）中调用，与主操作保持事务一致性。
// 调用方需确保传入的 exec 与外部事务是同一个连接。
func upsertEventLogTruncationTx(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, originNodeID, truncatedBefore int64, updatedAt string) error {
	if _, err := exec.ExecContext(ctx, `
INSERT INTO event_log_truncation_meta(origin_node_id, truncated_before_event_id, updated_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(origin_node_id) DO UPDATE SET
    truncated_before_event_id = CASE
        WHEN excluded.truncated_before_event_id > event_log_truncation_meta.truncated_before_event_id THEN excluded.truncated_before_event_id
        ELSE event_log_truncation_meta.truncated_before_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, originNodeID, truncatedBefore, updatedAt); err != nil {
		return fmt.Errorf("upsert event log truncation meta: %w", err)
	}
	return nil
}

// recordEventLogTrim 记录事件日志裁剪的全局统计信息到 event_log_trim_stats 表。
// 每次裁剪完成后调用，累计裁剪总数（trimmed_total 累加）并更新最后裁剪时间
// （last_trimmed_at_hlc）。使用 ON CONFLICT 合并策略，scope='global' 为固定标识。
// 当 trimmed <= 0 时直接返回 nil，不写入数据库。
func recordEventLogTrim(ctx context.Context, db *sql.DB, clk *clock.Clock, trimmed int64) error {
	if trimmed <= 0 {
		return nil
	}
	now := clk.Now().String()
	if _, err := db.ExecContext(ctx, `
INSERT INTO event_log_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = event_log_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, eventLogTrimStatsScope, trimmed, now); err != nil {
		return fmt.Errorf("record event log trim stats: %w", err)
	}
	return nil
}

// recordEventLogTrimTx 在已有事务中记录事件日志裁剪的全局统计信息。
// 与 recordEventLogTrim 功能相同，但接受 exec 接口以支持在事务上下文调用。
// 调用方需保证传入的 exec 与外部事务是同一个连接。
func recordEventLogTrimTx(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, trimmed int64, updatedAt string) error {
	if trimmed <= 0 {
		return nil
	}
	if _, err := exec.ExecContext(ctx, `
INSERT INTO event_log_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = event_log_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, eventLogTrimStatsScope, trimmed, updatedAt); err != nil {
		return fmt.Errorf("record event log trim stats: %w", err)
	}
	return nil
}
