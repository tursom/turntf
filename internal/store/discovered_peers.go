package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

type DiscoveredPeer struct {
	NodeID                     int64
	URL                        string
	ZeroMQCurveServerPublicKey string
	SourcePeerNodeID           int64
	State                      string
	FirstSeenAt                clock.Timestamp
	LastSeenAt                 clock.Timestamp
	LastConnectedAt            *clock.Timestamp
	LastError                  string
	Generation                 uint64
}

func (s *Store) ListDiscoveredPeers(ctx context.Context) ([]DiscoveredPeer, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT node_id, url, zeromq_curve_server_public_key, source_peer_node_id, state, first_seen_at_hlc, last_seen_at_hlc, last_connected_at_hlc, last_error, generation
FROM discovered_peers
ORDER BY node_id ASC, url ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list discovered peers: %w", err)
	}
	defer rows.Close()

	peers := make([]DiscoveredPeer, 0)
	for rows.Next() {
		peer, err := scanDiscoveredPeer(rows)
		if err != nil {
			return nil, err
		}
		peers = append(peers, peer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate discovered peers: %w", err)
	}
	return peers, nil
}

func (s *Store) UpsertDiscoveredPeer(ctx context.Context, peer DiscoveredPeer) error {
	if peer.NodeID <= 0 {
		return fmt.Errorf("%w: discovered peer node id cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(peer.URL) == "" {
		return fmt.Errorf("%w: discovered peer url cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(peer.State) == "" {
		return fmt.Errorf("%w: discovered peer state cannot be empty", ErrInvalidInput)
	}
	if peer.SourcePeerNodeID < 0 {
		return fmt.Errorf("%w: discovered peer source node id cannot be negative", ErrInvalidInput)
	}

	now := s.clock.Now()
	firstSeenAt := peer.FirstSeenAt
	if firstSeenAt == (clock.Timestamp{}) {
		firstSeenAt = now
	}
	lastSeenAt := peer.LastSeenAt
	if lastSeenAt == (clock.Timestamp{}) {
		lastSeenAt = now
	}
	var connectedRaw any
	if peer.LastConnectedAt != nil {
		connectedRaw = peer.LastConnectedAt.String()
	}

	if _, err := s.db.ExecContext(ctx, `
INSERT INTO discovered_peers(node_id, url, zeromq_curve_server_public_key, source_peer_node_id, state, first_seen_at_hlc, last_seen_at_hlc, last_connected_at_hlc, last_error, generation)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id, url) DO UPDATE SET
    zeromq_curve_server_public_key = CASE
        WHEN excluded.zeromq_curve_server_public_key != '' THEN excluded.zeromq_curve_server_public_key
        ELSE discovered_peers.zeromq_curve_server_public_key
    END,
    source_peer_node_id = excluded.source_peer_node_id,
    state = excluded.state,
    last_seen_at_hlc = excluded.last_seen_at_hlc,
    last_connected_at_hlc = COALESCE(excluded.last_connected_at_hlc, discovered_peers.last_connected_at_hlc),
    last_error = excluded.last_error,
    generation = CASE
        WHEN excluded.generation > discovered_peers.generation THEN excluded.generation
        ELSE discovered_peers.generation
    END
`, peer.NodeID, strings.TrimSpace(peer.URL), strings.TrimSpace(peer.ZeroMQCurveServerPublicKey), peer.SourcePeerNodeID, strings.TrimSpace(peer.State), firstSeenAt.String(), lastSeenAt.String(), connectedRaw, strings.TrimSpace(peer.LastError), peer.Generation); err != nil {
		return fmt.Errorf("upsert discovered peer: %w", err)
	}
	return nil
}

func (s *Store) RecordDiscoveredPeerState(ctx context.Context, nodeID int64, rawURL, state, lastError string, connected bool) error {
	if nodeID <= 0 {
		return fmt.Errorf("%w: discovered peer node id cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(rawURL) == "" {
		return fmt.Errorf("%w: discovered peer url cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(state) == "" {
		return fmt.Errorf("%w: discovered peer state cannot be empty", ErrInvalidInput)
	}

	now := s.clock.Now()
	var connectedRaw any
	if connected {
		connectedRaw = now.String()
	}
	if _, err := s.db.ExecContext(ctx, `
UPDATE discovered_peers
SET state = ?,
    last_seen_at_hlc = ?,
    last_connected_at_hlc = COALESCE(?, last_connected_at_hlc),
    last_error = ?
WHERE node_id = ? AND url = ?
`, strings.TrimSpace(state), now.String(), connectedRaw, strings.TrimSpace(lastError), nodeID, strings.TrimSpace(rawURL)); err != nil {
		return fmt.Errorf("update discovered peer state: %w", err)
	}
	return nil
}

func scanDiscoveredPeer(scanner interface {
	Scan(dest ...any) error
}) (DiscoveredPeer, error) {
	var peer DiscoveredPeer
	var firstSeenRaw string
	var lastSeenRaw string
	var lastConnectedRaw sql.NullString
	var generation int64

	if err := scanner.Scan(
		&peer.NodeID,
		&peer.URL,
		&peer.ZeroMQCurveServerPublicKey,
		&peer.SourcePeerNodeID,
		&peer.State,
		&firstSeenRaw,
		&lastSeenRaw,
		&lastConnectedRaw,
		&peer.LastError,
		&generation,
	); err != nil {
		return DiscoveredPeer{}, err
	}

	firstSeenAt, err := clock.ParseTimestamp(firstSeenRaw)
	if err != nil {
		return DiscoveredPeer{}, fmt.Errorf("parse discovered peer first seen: %w", err)
	}
	lastSeenAt, err := clock.ParseTimestamp(lastSeenRaw)
	if err != nil {
		return DiscoveredPeer{}, fmt.Errorf("parse discovered peer last seen: %w", err)
	}
	peer.FirstSeenAt = firstSeenAt
	peer.LastSeenAt = lastSeenAt
	if lastConnectedRaw.Valid && strings.TrimSpace(lastConnectedRaw.String) != "" {
		lastConnectedAt, err := clock.ParseTimestamp(lastConnectedRaw.String)
		if err != nil {
			return DiscoveredPeer{}, fmt.Errorf("parse discovered peer last connected: %w", err)
		}
		peer.LastConnectedAt = &lastConnectedAt
	}
	if generation > 0 {
		peer.Generation = uint64(generation)
	}
	return peer, nil
}
