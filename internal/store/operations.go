package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/tursom/turntf/internal/clock"
)

type OperationsStats struct {
	NodeID             int64
	MessageWindowSize  int
	LastEventSequence  int64
	Peers              []PeerOperationsStats
	UserConflictsTotal int64
	MessageTrim        MessageTrimStats
	Projection         ProjectionStats
}

type PeerOperationsStats struct {
	PeerNodeID int64
	Origins    []PeerOriginOperationsStats
}

type PeerOriginOperationsStats struct {
	OriginNodeID      int64
	AckedEventID      int64
	AppliedEventID    int64
	UnconfirmedEvents int64
	UpdatedAt         *clock.Timestamp
}

type MessageTrimStats struct {
	TrimmedTotal  int64
	LastTrimmedAt *clock.Timestamp
}

type localOriginEventStats struct {
	LastEventID int64
	EventCount  int64
}

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
		Projection:         projectionStats,
	}, nil
}

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

func (s *Store) listLocalOriginEventStats(ctx context.Context) (map[int64]localOriginEventStats, error) {
	if s.engine != EngineSQLite {
		progress, err := s.eventLog.ListOriginProgress(ctx)
		if err != nil {
			return nil, err
		}
		stats := make(map[int64]localOriginEventStats, len(progress))
		for _, item := range progress {
			count, err := s.eventLog.CountEventsByOrigin(ctx, item.OriginNodeID, 0)
			if err != nil {
				return nil, err
			}
			stats[item.OriginNodeID] = localOriginEventStats{
				LastEventID: item.LastEventID,
				EventCount:  count,
			}
		}
		return stats, nil
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT origin_node_id, COALESCE(MAX(event_id), 0), COUNT(*)
FROM event_log
GROUP BY origin_node_id
ORDER BY origin_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query local origin event stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[int64]localOriginEventStats)
	for rows.Next() {
		var originNodeID int64
		var item localOriginEventStats
		if err := rows.Scan(&originNodeID, &item.LastEventID, &item.EventCount); err != nil {
			return nil, fmt.Errorf("scan local origin event stats: %w", err)
		}
		stats[originNodeID] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate local origin event stats: %w", err)
	}
	return stats, nil
}

func (s *Store) countUnconfirmedOriginEvents(ctx context.Context, originNodeID, ackedEventID, fallbackCount int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	if ackedEventID <= 0 {
		return fallbackCount, nil
	}
	if s.engine != EngineSQLite {
		return s.eventLog.CountEventsByOrigin(ctx, originNodeID, ackedEventID)
	}

	var count int64
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM event_log
WHERE origin_node_id = ? AND event_id > ?
`, originNodeID, ackedEventID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count unconfirmed origin events: %w", err)
	}
	return count, nil
}

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

func cloneTimestamp(ts *clock.Timestamp) *clock.Timestamp {
	if ts == nil {
		return nil
	}
	cloned := *ts
	return &cloned
}

func (s *Store) userConflictCount(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_conflicts`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count user conflicts: %w", err)
	}
	return count, nil
}

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
