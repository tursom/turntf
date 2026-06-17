package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/tursom/turntf/internal/clock"
)

// OperationsStats 是 Store 运行时运维统计信息的顶层聚合结构。
// 通过 OperationsStats() 方法一次性收集，用于监控面板展示和调试诊断。
// 包含以下维度的信息：
//   - 本节点基础信息（node_id、消息窗口大小、最新事件序列号）
//   - 各 Peer 的来源维度确认/应用进度
//   - 系统冲突计数
//   - 消息裁剪和事件日志裁剪的累计统计
//   - 投影系统的待处理队列状态
type OperationsStats struct {
	// NodeID 本节点的 ID，对应 schema_meta 中的 node_id。
	NodeID int64
	// MessageWindowSize 每条用户消息保留的上限数量，由 Store 配置决定。
	MessageWindowSize int
	// LastEventSequence 事件日志中最新事件的全局序列号。
	LastEventSequence int64
	// Peers 每个已知 peer 的逐来源进度统计。
	Peers []PeerOperationsStats
	// UserConflictsTotal user_conflicts 表中的总冲突记录数。
	UserConflictsTotal int64
	// MessageTrim 消息裁剪累计统计。
	MessageTrim MessageTrimStats
	// EventLogTrim 事件日志裁剪累计统计。
	EventLogTrim EventLogTrimStats
	// Projection 投影系统的待处理事件数和最近失败时间。
	Projection ProjectionStats
}

// PeerOperationsStats 是单个 peer 节点在所有来源上的事件同步进度的聚合。
// 对 PeerOperationsStats.Origins 按 OriginNodeID 升序排列。
type PeerOperationsStats struct {
	// PeerNodeID 对等节点 ID。
	PeerNodeID int64
	// Origins 该 peer 在每个来源节点上的确认和应用状态列表。
	Origins []PeerOriginOperationsStats
}

// PeerOriginOperationsStats 描述某个 peer 在某来源节点上的事件同步状态。
// 通过对比 AckedEventID 与本地事件总数计算 UnconfirmedEvents 指标，
// 用于判断 peer 的复制进度是否滞后。
type PeerOriginOperationsStats struct {
	// OriginNodeID 来源节点 ID。
	OriginNodeID int64
	// AckedEventID 该 peer 已确认收到（acknowledged）的最新事件 ID。
	AckedEventID int64
	// AppliedEventID 本节点已应用（applied）该来源的最新事件 ID。
	AppliedEventID int64
	// UnconfirmedEvents 该 peer 尚未确认的事件数量（本节点事件数 - peer 已确认数）。
	UnconfirmedEvents int64
	// UpdatedAt 该条游标记录最近一次更新的 HLC 时间戳。
	UpdatedAt *clock.Timestamp
}

// MessageTrimStats 是自节点启动以来消息裁剪的累计统计。
// 记录裁剪总数和最近一次裁剪的时间。
type MessageTrimStats struct {
	// TrimmedTotal 累计裁剪的消息总条数。
	TrimmedTotal int64
	// LastTrimmedAt 最近一次消息裁剪操作的 HLC 时间戳。
	LastTrimmedAt *clock.Timestamp
}

// localOriginEventStats 是本地各来源节点事件统计的内部中间类型。
// 由 backend.ListLocalOriginEventStats 填充，用于在 peerOperationsStats 中
// 计算 peer 在每个来源上的未确认事件数。
type localOriginEventStats struct {
	// LastEventID 该来源节点已知的最后一个事件 ID。
	LastEventID int64
	// EventCount 该来源节点在本节点上的事件总数。
	EventCount int64
}

// OperationsStats 收集 Store 的综合性运维统计信息，是运维监控的入口方法。
// 一次性收集以下六个维度的数据：
//  1. 本地事件序列号（LastEventSequence）
//  2. 各 peer 在各来源上的确认和应用进度（peerOperationsStats）
//  3. 用户冲突记录总数（userConflictCount）
//  4. 消息裁剪统计（messageTrimStats）
//  5. 事件日志裁剪统计（eventLogTrimStats）
//  6. 投影系统待处理统计（projectionStats）
//
// peerNodeIDs 参数指定需要查询的 peer 节点 ID 列表，用于收集每个 peer
// 在各个来源节点上的确认事件 ID 和应用事件 ID，以及未确认事件数量。
// 各子查询独立执行，任一子查询失败则返回错误。
func (s *Store) OperationsStats(ctx context.Context, peerNodeIDs []int64) (OperationsStats, error) {
	lastSequence, err := s.LastEventSequence(ctx)
	if err != nil {
		return OperationsStats{}, err
	}

	peerStats, err := s.peerOperationsStats(ctx, peerNodeIDs)
	if err != nil {
		return OperationsStats{}, err
	}

	conflicts, err := s.userConflictCount(ctx)
	if err != nil {
		return OperationsStats{}, err
	}

	trimStats, err := s.messageTrimStats(ctx)
	if err != nil {
		return OperationsStats{}, err
	}
	eventLogTrimStats, err := s.eventLogTrimStats(ctx)
	if err != nil {
		return OperationsStats{}, err
	}
	projectionStats, err := s.projectionStats(ctx)
	if err != nil {
		return OperationsStats{}, err
	}

	return OperationsStats{
		NodeID:             s.nodeID,
		MessageWindowSize:  normalizeMessageWindowSize(s.messageWindowSize),
		LastEventSequence:  lastSequence,
		Peers:              peerStats,
		UserConflictsTotal: conflicts,
		MessageTrim:        trimStats,
		EventLogTrim:       eventLogTrimStats,
		Projection:         projectionStats,
	}, nil
}

// peerOperationsStats 聚合每个 peer 在各来源节点上的事件同步进度统计数据。
// 核心算法：通过三个数据源（本地事件统计、peer ack 游标、origin 应用游标）
// 进行合并计算：
//  1. 从 backend 获取本地各来源的事件统计（lastEventID + eventCount）
//  2. 从 peer_ack_cursors 获取各 peer 在各来源上的确认进度（ackedEventID）
//  3. 从 origin_cursors 获取本节点在各来源上的应用进度（appliedEventID）
//  4. 取三者的来源并集，确保每个 peer 在每个已知来源上都有统计条目
//  5. 通过 countUnconfirmedOriginEvents 计算每个 peer 的未确认事件数
//  6. 对结果按 PeerNodeID 和 OriginNodeID 升序排列
//
// 返回的 []PeerOperationsStats 仅包含 peerNodeIDs 列表中指定的 peer。
func (s *Store) peerOperationsStats(ctx context.Context, peerNodeIDs []int64) ([]PeerOperationsStats, error) {
	localStats, err := s.listLocalOriginEventStats(ctx)
	if err != nil {
		return nil, err
	}
	ackCursors, err := s.ListPeerAckCursors(ctx)
	if err != nil {
		return nil, err
	}
	originCursors, err := s.ListOriginCursors(ctx)
	if err != nil {
		return nil, err
	}

	appliedByOrigin := make(map[int64]OriginCursor, len(originCursors))
	for _, cursor := range originCursors {
		appliedByOrigin[cursor.OriginNodeID] = cursor
	}

	peerOrigins := make(map[int64]map[int64]PeerOriginOperationsStats, len(peerNodeIDs))
	for _, peerID := range peerNodeIDs {
		if peerID <= 0 {
			continue
		}
		peerOrigins[peerID] = make(map[int64]PeerOriginOperationsStats)
	}

	unionOrigins := make(map[int64]struct{}, len(localStats)+len(appliedByOrigin))
	for originNodeID := range localStats {
		unionOrigins[originNodeID] = struct{}{}
	}
	for originNodeID := range appliedByOrigin {
		unionOrigins[originNodeID] = struct{}{}
	}

	for _, cursor := range ackCursors {
		unionOrigins[cursor.OriginNodeID] = struct{}{}
		if _, ok := peerOrigins[cursor.PeerNodeID]; !ok {
			peerOrigins[cursor.PeerNodeID] = make(map[int64]PeerOriginOperationsStats)
		}
		item := peerOrigins[cursor.PeerNodeID][cursor.OriginNodeID]
		item.OriginNodeID = cursor.OriginNodeID
		item.AckedEventID = cursor.AckedEventID
		item.UpdatedAt = chooseLaterTimestamp(item.UpdatedAt, &cursor.UpdatedAt)
		peerOrigins[cursor.PeerNodeID][cursor.OriginNodeID] = item
	}

	for peerID, origins := range peerOrigins {
		for originNodeID := range unionOrigins {
			item := origins[originNodeID]
			item.OriginNodeID = originNodeID
			if applied, ok := appliedByOrigin[originNodeID]; ok {
				item.AppliedEventID = applied.AppliedEventID
				item.UpdatedAt = chooseLaterTimestamp(item.UpdatedAt, &applied.UpdatedAt)
			}
			if local, ok := localStats[originNodeID]; ok {
				unconfirmed, err := s.countUnconfirmedOriginEvents(ctx, originNodeID, item.AckedEventID, local.EventCount)
				if err != nil {
					return nil, err
				}
				item.UnconfirmedEvents = unconfirmed
			}
			origins[originNodeID] = item
		}
		peerOrigins[peerID] = origins
	}

	peers := make([]PeerOperationsStats, 0, len(peerOrigins))
	for peerID, origins := range peerOrigins {
		stats := PeerOperationsStats{
			PeerNodeID: peerID,
			Origins:    make([]PeerOriginOperationsStats, 0, len(origins)),
		}
		for _, item := range origins {
			if item.OriginNodeID <= 0 {
				continue
			}
			stats.Origins = append(stats.Origins, item)
		}
		sort.Slice(stats.Origins, func(i, j int) bool {
			return stats.Origins[i].OriginNodeID < stats.Origins[j].OriginNodeID
		})
		peers = append(peers, stats)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].PeerNodeID < peers[j].PeerNodeID
	})
	return peers, nil
}

// listLocalOriginEventStats 委托 backend 获取本地所有来源节点的事件统计。
// 返回 map[int64]localOriginEventStats，key 为 originNodeID，
// value 包含最后 event_id 和事件总数。用于计算 peer 未确认事件数时的基数。
func (s *Store) listLocalOriginEventStats(ctx context.Context) (map[int64]localOriginEventStats, error) {
	return s.backend.ListLocalOriginEventStats(ctx, s.db)
}

// countUnconfirmedOriginEvents 计算 peer 在某来源上尚未确认的事件数。
// 委托给 s.backend.CountUnconfirmedOriginEvents 实现。
// ackedEventID 是 peer 已确认的最新事件 ID，fallbackCount 是兜底计数
// （用于后端无法精确计算时的替代值）。返回值为 peer 尚未收到的事件数。
func (s *Store) countUnconfirmedOriginEvents(ctx context.Context, originNodeID, ackedEventID, fallbackCount int64) (int64, error) {
	return s.backend.CountUnconfirmedOriginEvents(ctx, s.db, originNodeID, ackedEventID, fallbackCount)
}

// chooseLaterTimestamp 返回两个时间戳指针中 HLC 时间较晚的一个的深拷贝。
// 处理 nil 指针：如果其中一个为 nil 则直接返回另一个的拷贝副本。
// 用于在合并多个游标记录时选择最新的更新版本。
func chooseLaterTimestamp(current, candidate *clock.Timestamp) *clock.Timestamp {
	switch {
	case current == nil:
		return cloneTimestamp(candidate)
	case candidate == nil:
		return cloneTimestamp(current)
	case candidate.Compare(*current) > 0:
		return cloneTimestamp(candidate)
	default:
		return cloneTimestamp(current)
	}
}

// cloneTimestamp 深拷贝 clock.Timestamp 指针，创建新的 Timestamp 值副本。
// 如果输入为 nil 则返回 nil。用于在多处引用同一 Timestamp 时避免
// 因指针共享导致的时序问题（如 chooseLaterTimestamp 返回的值被后续操作修改）。
func cloneTimestamp(ts *clock.Timestamp) *clock.Timestamp {
	if ts == nil {
		return nil
	}
	cloned := *ts
	return &cloned
}

// userConflictCount 查询 user_conflicts 表中用户冲突记录的总行数。
// 用户冲突记录在跨节点复制时产生（如不同节点同时创建同用户名用户），
// 该计数用于监控冲突频率和人工介入清理的时机判断。
func (s *Store) userConflictCount(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_conflicts`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count user conflicts: %w", err)
	}
	return count, nil
}

// messageTrimStats 从 SQLite 数据库 message_trim_stats 表中读取消息裁剪的
// 全局累计统计信息。包含裁剪总条数（TrimmedTotal）和最近一次裁剪的 HLC 时间戳。
// 当表中无记录时返回零值 MessageTrimStats。
// 消息裁剪由 trimMessagesForUserTx 在每次创建新消息后触发。
func (s *Store) messageTrimStats(ctx context.Context) (MessageTrimStats, error) {
	var total int64
	var last sql.NullString
	err := s.db.QueryRowContext(ctx, `
SELECT trimmed_total, last_trimmed_at_hlc
FROM message_trim_stats
WHERE scope = 'global'
`).Scan(&total, &last)
	if err != nil {
		if err == sql.ErrNoRows {
			return MessageTrimStats{}, nil
		}
		return MessageTrimStats{}, fmt.Errorf("query message trim stats: %w", err)
	}

	stats := MessageTrimStats{TrimmedTotal: total}
	if last.Valid && last.String != "" {
		parsed, err := clock.ParseTimestamp(last.String)
		if err != nil {
			return MessageTrimStats{}, fmt.Errorf("parse message trim timestamp: %w", err)
		}
		stats.LastTrimmedAt = &parsed
	}
	return stats, nil
}
