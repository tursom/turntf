package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

// DiscoveredPeer 代表通过 mesh 发现协议探测到的对等节点。
// 存储于 discovered_peers 表中，复合主键为 (node_id, url)。
// 记录节点的连接信息、当前状态、拓扑代际计数和首次/最后发现时间。
// 用于 mesh 网络自动发现和节点状态追踪，支持同一节点多 URL 的情况。
type DiscoveredPeer struct {
	// NodeID 对等节点的唯一 ID。
	NodeID int64
	// URL 对等节点的连接地址（如 tcp://host:port）。
	URL string
	// ZeroMQCurveServerPublicKey 对等端的 CurveZMQ 加密公钥，用于安全认证。
	ZeroMQCurveServerPublicKey string
	// SourcePeerNodeID 发现该节点的来源 peer（通过哪个已知节点获知此节点信息）。
	// 0 表示直接发现（直连），>0 表示 gossip 传播。
	SourcePeerNodeID int64
	// State 对等节点的当前连接状态（如 "connected"、"disconnected"、"unreachable"）。
	State string
	// FirstSeenAt 首次发现该节点的 HLC 时间戳。
	FirstSeenAt clock.Timestamp
	// LastSeenAt 最近一次发现或更新该节点的 HLC 时间戳。
	LastSeenAt clock.Timestamp
	// LastConnectedAt 最近一次与该节点成功建立连接的 HLC 时间戳，nil 表示从未连接。
	LastConnectedAt *clock.Timestamp
	// LastError 最近一次与该节点通信时遇到的错误信息，空字符串表示无错误。
	LastError string
	// Generation 该节点的拓扑代际计数，用于检测 mesh 拓扑变更。
	// 仅在 new_generation > old_generation 时更新，避免 gossip 乱序导致代际回退。
	Generation uint64
}

// ListDiscoveredPeers 查询 discovered_peers 表返回所有已发现的 peer 记录。
// 返回按 node_id ASC、url ASC 排序的结果，便于进行节点分组展示。
// 遍历过程中每行调用 scanDiscoveredPeer 解析时间和代际字段。
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

// UpsertDiscoveredPeer 创建或更新一个 discovered_peers 记录（插入或合并更新）。
// 复合主键为 (node_id, url)，当记录已存在时执行部分字段合并：
//   - zeromq_curve_server_public_key：仅当新值非空时覆盖，避免空字符串擦除已有密钥
//   - source_peer_node_id：总是覆盖为新值（最新的信息来源优先）
//   - state：总是覆盖为新值
//   - last_seen_at_hlc：总是覆盖为新值（更新最后发现时间）
//   - last_connected_at_hlc：仅当新值非空时覆盖（COALESCE），保留已有连接记录
//   - last_error：总是覆盖为新值
//   - generation：仅在新代际更大时更新（只增不减），防止 gossip 乱序导致的代际回退
//
// 参数验证：NodeID > 0、URL 和 State 不能为空、SourcePeerNodeID 不能为负。
// 如果 FirstSeenAt/LastSeenAt 为零值，自动使用当前时钟时间填充。
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

// RecordDiscoveredPeerState 更新已发现 peer 的运行时状态，不改变核心连接信息。
// 参数说明：
//   - state：新的连接状态（如 "connected" / "disconnected"）
//   - lastError：最近一次错误信息
//   - connected：为 true 时同步更新 last_connected_at 为当前 HLC 时间戳
//
// 此方法执行 UPDATE（非 UPSERT），因此记录必须已存在。
// 参数验证：nodeID > 0、URL 和 state 不能为空。
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

// scanDiscoveredPeer 从 SQL scanner 扫描 discovered_peers 表的一行到 DiscoveredPeer，
// 解析 HLC 时间戳字符串为 clock.Timestamp 结构体。
// 扫描字段：node_id、url、zeromq_curve_server_public_key、source_peer_node_id、
// state、first_seen_at_hlc、last_seen_at_hlc、last_connected_at_hlc、last_error、generation。
// last_connected_at_hlc 使用 sql.NullString 处理数据库 NULL（从未连接过）。
// generation 在数据库中以 int64 存储，非零时转为 uint64 赋值。
// 非并发安全——调用方保证 scanner 的并发安全性。
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
