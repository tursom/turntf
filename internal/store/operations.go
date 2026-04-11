package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"notifier/internal/clock"
)

type OperationsStats struct {
	NodeID             int64
	MessageWindowSize  int
	LastEventSequence  int64
	PeerCursors        []PeerOperationsStats
	UserConflictsTotal int64
	MessageTrim        MessageTrimStats
}

type PeerOperationsStats struct {
	PeerNodeID        int64
	AckedSequence     int64
	AppliedSequence   int64
	UnconfirmedEvents int64
	UpdatedAt         *clock.Timestamp
}

type MessageTrimStats struct {
	TrimmedTotal  int64
	LastTrimmedAt *clock.Timestamp
}

func (s *Store) OperationsStats(ctx context.Context, peerNodeIDs []int64) (OperationsStats, error) {
	lastSequence, err := s.LastEventSequence(ctx)
	if err != nil {
		return OperationsStats{}, err
	}

	peerCursors, err := s.peerOperationsStats(ctx, peerNodeIDs, lastSequence)
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

	return OperationsStats{
		NodeID:             s.nodeID,
		MessageWindowSize:  normalizeMessageWindowSize(s.messageWindowSize),
		LastEventSequence:  lastSequence,
		PeerCursors:        peerCursors,
		UserConflictsTotal: conflicts,
		MessageTrim:        trimStats,
	}, nil
}

func (s *Store) peerOperationsStats(ctx context.Context, peerNodeIDs []int64, lastSequence int64) ([]PeerOperationsStats, error) {
	index := make(map[int64]PeerOperationsStats)
	for _, peerID := range peerNodeIDs {
		if peerID <= 0 {
			continue
		}
		index[peerID] = PeerOperationsStats{
			PeerNodeID:        peerID,
			UnconfirmedEvents: lastSequence,
		}
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT peer_node_id, acked_sequence, applied_sequence, updated_at_hlc
FROM peer_cursors
ORDER BY peer_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query peer cursor stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		cursor, err := scanPeerCursor(rows)
		if err != nil {
			return nil, err
		}
		updatedAt := cursor.UpdatedAt
		index[cursor.PeerNodeID] = PeerOperationsStats{
			PeerNodeID:        cursor.PeerNodeID,
			AckedSequence:     cursor.AckedSequence,
			AppliedSequence:   cursor.AppliedSequence,
			UnconfirmedEvents: nonNegativeDelta(lastSequence, cursor.AckedSequence),
			UpdatedAt:         &updatedAt,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate peer cursor stats: %w", err)
	}

	stats := make([]PeerOperationsStats, 0, len(index))
	for _, item := range index {
		stats = append(stats, item)
	}
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].PeerNodeID < stats[j].PeerNodeID
	})
	return stats, nil
}

func nonNegativeDelta(value, subtract int64) int64 {
	if value <= subtract {
		return 0
	}
	return value - subtract
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
