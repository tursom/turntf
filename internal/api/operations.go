package api

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	"github.com/tursom/turntf/internal/store"
)

type clusterStatusProvider interface {
	Status(context.Context) (app.ClusterStatus, error)
	ConfiguredPeerNodeIDs() []int64
}

type clusterNodeResponse struct {
	NodeID        int64  `json:"node_id"`
	IsLocal       bool   `json:"is_local"`
	ConfiguredURL string `json:"configured_url"`
}

type clusterNodesResponse struct {
	Nodes []clusterNodeResponse `json:"nodes"`
}

type loggedInUserResponse struct {
	NodeID   int64  `json:"node_id"`
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type nodeLoggedInUsersResponse struct {
	TargetNodeID int64                  `json:"target_node_id"`
	Items        []loggedInUserResponse `json:"items"`
	Count        int                    `json:"count"`
}

type operationsStatus struct {
	NodeID            int64                `json:"node_id"`
	MessageWindowSize int                  `json:"message_window_size"`
	LastEventSequence int64                `json:"last_event_sequence"`
	WriteGateReady    bool                 `json:"write_gate_ready"`
	ConflictTotal     int64                `json:"conflict_total"`
	MessageTrim       messageTrimStatus    `json:"message_trim"`
	Projection        projectionStatus     `json:"projection"`
	Peers             []peerStatusResponse `json:"peers"`
}

type messageTrimStatus struct {
	TrimmedTotal  int64  `json:"trimmed_total"`
	LastTrimmedAt string `json:"last_trimmed_at,omitempty"`
}

type projectionStatus struct {
	PendingTotal int64  `json:"pending_total"`
	LastFailedAt string `json:"last_failed_at,omitempty"`
}

type peerOriginStatusResponse struct {
	OriginNodeID      int64  `json:"origin_node_id"`
	AckedEventID      int64  `json:"acked_event_id"`
	AppliedEventID    int64  `json:"applied_event_id"`
	UnconfirmedEvents int64  `json:"unconfirmed_events"`
	CursorUpdatedAt   string `json:"cursor_updated_at,omitempty"`
	RemoteLastEventID uint64 `json:"remote_last_event_id"`
	PendingCatchup    bool   `json:"pending_catchup"`
}

type peerStatusResponse struct {
	NodeID                    int64                      `json:"node_id"`
	ConfiguredURL             string                     `json:"configured_url,omitempty"`
	Connected                 bool                       `json:"connected"`
	SessionDirection          string                     `json:"session_direction,omitempty"`
	Origins                   []peerOriginStatusResponse `json:"origins"`
	PendingSnapshotPartitions int                        `json:"pending_snapshot_partitions"`
	RemoteSnapshotVersion     string                     `json:"remote_snapshot_version,omitempty"`
	RemoteMessageWindowSize   int                        `json:"remote_message_window_size,omitempty"`
	ClockOffsetMs             int64                      `json:"clock_offset_ms"`
	LastClockSync             string                     `json:"last_clock_sync,omitempty"`
	SnapshotDigestsSentTotal  uint64                     `json:"snapshot_digests_sent_total"`
	SnapshotDigestsRecvTotal  uint64                     `json:"snapshot_digests_received_total"`
	SnapshotChunksSentTotal   uint64                     `json:"snapshot_chunks_sent_total"`
	SnapshotChunksRecvTotal   uint64                     `json:"snapshot_chunks_received_total"`
	LastSnapshotDigestAt      string                     `json:"last_snapshot_digest_at,omitempty"`
	LastSnapshotChunkAt       string                     `json:"last_snapshot_chunk_at,omitempty"`
}

func (s *Service) OperationsStatus(ctx context.Context) (operationsStatus, error) {
	var (
		clusterStatus app.ClusterStatus
		peerNodeIDs   []int64
	)
	clusterStatus.WriteGateReady = true

	if provider, ok := s.eventSink.(clusterStatusProvider); ok {
		status, err := provider.Status(ctx)
		if err != nil {
			return operationsStatus{}, err
		}
		clusterStatus = status
		peerNodeIDs = provider.ConfiguredPeerNodeIDs()
	}

	storeStats, err := s.store.OperationsStats(ctx, peerNodeIDs)
	if err != nil {
		return operationsStatus{}, err
	}

	response := operationsStatus{
		NodeID:            storeStats.NodeID,
		MessageWindowSize: storeStats.MessageWindowSize,
		LastEventSequence: storeStats.LastEventSequence,
		WriteGateReady:    clusterStatus.WriteGateReady,
		ConflictTotal:     storeStats.UserConflictsTotal,
		MessageTrim: messageTrimStatus{
			TrimmedTotal:  storeStats.MessageTrim.TrimmedTotal,
			LastTrimmedAt: timestampString(storeStats.MessageTrim.LastTrimmedAt),
		},
		Projection: projectionStatus{
			PendingTotal: storeStats.Projection.PendingTotal,
			LastFailedAt: timestampString(storeStats.Projection.LastFailedAt),
		},
		Peers: mergePeerStatus(storeStats.Peers, clusterStatus.Peers),
	}
	if response.NodeID == 0 {
		response.NodeID = clusterStatus.NodeID
	}
	if response.MessageWindowSize == 0 {
		response.MessageWindowSize = clusterStatus.MessageWindowSize
	}
	return response, nil
}

func (s *Service) ClusterNodes(ctx context.Context) (clusterNodesResponse, error) {
	nodeID := s.store.NodeID()
	nodes := []clusterNodeResponse{{
		NodeID:        nodeID,
		IsLocal:       true,
		ConfiguredURL: "",
	}}

	if provider, ok := s.eventSink.(clusterStatusProvider); ok {
		status, err := provider.Status(ctx)
		if err != nil {
			return clusterNodesResponse{}, err
		}
		if status.NodeID > 0 {
			nodes[0].NodeID = status.NodeID
		}
		nodes = append(nodes, connectedClusterNodes(status.Peers)...)
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].IsLocal != nodes[j].IsLocal {
			return nodes[i].IsLocal
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return clusterNodesResponse{Nodes: nodes}, nil
}

func (s *Service) ListNodeLoggedInUsers(ctx context.Context, nodeID int64) (nodeLoggedInUsersResponse, error) {
	if nodeID <= 0 {
		return nodeLoggedInUsersResponse{}, fmt.Errorf("%w: node_id must be positive", store.ErrInvalidInput)
	}

	var (
		users []app.LoggedInUserSummary
		err   error
	)
	if nodeID == s.store.NodeID() {
		if s.localUsers == nil {
			return nodeLoggedInUsersResponse{}, fmt.Errorf("%w: local logged-in users provider is not configured", app.ErrServiceUnavailable)
		}
		users, err = s.localUsers.ListLoggedInUsers(ctx)
	} else {
		if s.remoteUsers == nil {
			return nodeLoggedInUsersResponse{}, fmt.Errorf("%w: cluster logged-in user query is not configured", app.ErrServiceUnavailable)
		}
		users, err = s.remoteUsers.QueryLoggedInUsers(ctx, nodeID)
	}
	if err != nil {
		return nodeLoggedInUsersResponse{}, err
	}

	items := make([]loggedInUserResponse, 0, len(users))
	for _, user := range users {
		items = append(items, loggedInUserResponse{
			NodeID:   user.NodeID,
			UserID:   user.UserID,
			Username: user.Username,
		})
	}
	return nodeLoggedInUsersResponse{
		TargetNodeID: nodeID,
		Items:        items,
		Count:        len(items),
	}, nil
}

func (s *Service) Metrics(ctx context.Context) (string, error) {
	status, err := s.OperationsStatus(ctx)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	nodeIDLabel := strconv.FormatInt(status.NodeID, 10)
	writeMetricHelp(&buf, "notifier_event_log_last_sequence", "Local event log last sequence.", "gauge")
	writeGauge(&buf, "notifier_event_log_last_sequence", map[string]string{"node_id": nodeIDLabel}, float64(status.LastEventSequence))
	writeMetricHelp(&buf, "notifier_user_conflicts_total", "Total recorded user conflicts.", "counter")
	writeGauge(&buf, "notifier_user_conflicts_total", map[string]string{"node_id": nodeIDLabel}, float64(status.ConflictTotal))
	writeMetricHelp(&buf, "notifier_message_trimmed_total", "Total messages trimmed by the local window.", "counter")
	writeGauge(&buf, "notifier_message_trimmed_total", map[string]string{"node_id": nodeIDLabel}, float64(status.MessageTrim.TrimmedTotal))
	writeMetricHelp(&buf, "notifier_pending_projections", "Pending event projections waiting to be replayed.", "gauge")
	writeGauge(&buf, "notifier_pending_projections", map[string]string{"node_id": nodeIDLabel}, float64(status.Projection.PendingTotal))
	writeMetricHelp(&buf, "notifier_write_gate_ready", "Whether the node currently allows local writes.", "gauge")
	writeGauge(&buf, "notifier_write_gate_ready", map[string]string{"node_id": nodeIDLabel}, boolGauge(status.WriteGateReady))

	writeMetricHelp(&buf, "notifier_peer_connected", "Whether the peer has an active cluster session.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_acked_event_id", "Highest origin event id acknowledged by the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_applied_event_id", "Highest origin event id applied locally.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_remote_last_event_id", "Highest remote origin event id observed from the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_unconfirmed_events", "Local origin events not yet acknowledged by the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_pending_snapshot_partitions", "Pending anti-entropy snapshot partitions for the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_clock_offset_ms", "Last trusted clock offset for the peer in milliseconds.", "gauge")
	for _, peer := range status.Peers {
		if peer.NodeID <= 0 {
			continue
		}
		peerLabels := map[string]string{
			"node_id":      nodeIDLabel,
			"peer_node_id": strconv.FormatInt(peer.NodeID, 10),
		}
		writeGauge(&buf, "notifier_peer_connected", peerLabels, boolGauge(peer.Connected))
		writeGauge(&buf, "notifier_peer_pending_snapshot_partitions", peerLabels, float64(peer.PendingSnapshotPartitions))
		writeGauge(&buf, "notifier_clock_offset_ms", peerLabels, float64(peer.ClockOffsetMs))
		for _, origin := range peer.Origins {
			labels := map[string]string{
				"node_id":        nodeIDLabel,
				"peer_node_id":   strconv.FormatInt(peer.NodeID, 10),
				"origin_node_id": strconv.FormatInt(origin.OriginNodeID, 10),
			}
			writeGauge(&buf, "notifier_peer_origin_acked_event_id", labels, float64(origin.AckedEventID))
			writeGauge(&buf, "notifier_peer_origin_applied_event_id", labels, float64(origin.AppliedEventID))
			writeGauge(&buf, "notifier_peer_origin_remote_last_event_id", labels, float64(origin.RemoteLastEventID))
			writeGauge(&buf, "notifier_peer_origin_unconfirmed_events", labels, float64(origin.UnconfirmedEvents))
		}
	}
	return buf.String(), nil
}

func mergePeerStatus(storePeers []store.PeerOperationsStats, clusterPeers []app.ClusterPeerStatus) []peerStatusResponse {
	index := make(map[int64]peerStatusResponse, len(storePeers)+len(clusterPeers))
	unknown := make([]peerStatusResponse, 0)
	for _, peer := range storePeers {
		item := peerStatusResponse{
			NodeID:  peer.PeerNodeID,
			Origins: make([]peerOriginStatusResponse, 0, len(peer.Origins)),
		}
		for _, origin := range peer.Origins {
			item.Origins = append(item.Origins, peerOriginStatusResponse{
				OriginNodeID:      origin.OriginNodeID,
				AckedEventID:      origin.AckedEventID,
				AppliedEventID:    origin.AppliedEventID,
				UnconfirmedEvents: origin.UnconfirmedEvents,
				CursorUpdatedAt:   timestampString(origin.UpdatedAt),
			})
		}
		index[peer.PeerNodeID] = item
	}
	for _, peer := range clusterPeers {
		if peer.NodeID <= 0 {
			unknown = append(unknown, peerStatusResponse{
				NodeID:                    peer.NodeID,
				ConfiguredURL:             peer.ConfiguredURL,
				Connected:                 peer.Connected,
				SessionDirection:          peer.SessionDirection,
				PendingSnapshotPartitions: peer.PendingSnapshotPartitions,
				RemoteSnapshotVersion:     peer.RemoteSnapshotVersion,
				RemoteMessageWindowSize:   peer.RemoteMessageWindowSize,
				ClockOffsetMs:             peer.ClockOffsetMs,
				LastClockSync:             timeString(peer.LastClockSync),
				SnapshotDigestsSentTotal:  peer.SnapshotDigestsSentTotal,
				SnapshotDigestsRecvTotal:  peer.SnapshotDigestsRecvTotal,
				SnapshotChunksSentTotal:   peer.SnapshotChunksSentTotal,
				SnapshotChunksRecvTotal:   peer.SnapshotChunksRecvTotal,
				LastSnapshotDigestAt:      timeString(peer.LastSnapshotDigestAt),
				LastSnapshotChunkAt:       timeString(peer.LastSnapshotChunkAt),
				Origins:                   mergePeerOrigins(nil, peer.Origins),
			})
			continue
		}
		item := index[peer.NodeID]
		item.NodeID = peer.NodeID
		item.ConfiguredURL = peer.ConfiguredURL
		item.Connected = peer.Connected
		item.SessionDirection = peer.SessionDirection
		item.PendingSnapshotPartitions = peer.PendingSnapshotPartitions
		item.RemoteSnapshotVersion = peer.RemoteSnapshotVersion
		item.RemoteMessageWindowSize = peer.RemoteMessageWindowSize
		item.ClockOffsetMs = peer.ClockOffsetMs
		item.LastClockSync = timeString(peer.LastClockSync)
		item.SnapshotDigestsSentTotal = peer.SnapshotDigestsSentTotal
		item.SnapshotDigestsRecvTotal = peer.SnapshotDigestsRecvTotal
		item.SnapshotChunksSentTotal = peer.SnapshotChunksSentTotal
		item.SnapshotChunksRecvTotal = peer.SnapshotChunksRecvTotal
		item.LastSnapshotDigestAt = timeString(peer.LastSnapshotDigestAt)
		item.LastSnapshotChunkAt = timeString(peer.LastSnapshotChunkAt)
		item.Origins = mergePeerOrigins(item.Origins, peer.Origins)
		index[peer.NodeID] = item
	}

	peers := make([]peerStatusResponse, 0, len(index))
	for _, peer := range index {
		sort.Slice(peer.Origins, func(i, j int) bool {
			return peer.Origins[i].OriginNodeID < peer.Origins[j].OriginNodeID
		})
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].NodeID < peers[j].NodeID
	})
	sort.Slice(unknown, func(i, j int) bool {
		return unknown[i].ConfiguredURL < unknown[j].ConfiguredURL
	})
	return append(unknown, peers...)
}

func mergePeerOrigins(storeOrigins []peerOriginStatusResponse, clusterOrigins []app.ClusterPeerOriginStatus) []peerOriginStatusResponse {
	index := make(map[int64]peerOriginStatusResponse, len(storeOrigins)+len(clusterOrigins))
	for _, origin := range storeOrigins {
		index[origin.OriginNodeID] = origin
	}
	for _, origin := range clusterOrigins {
		item := index[origin.OriginNodeID]
		item.OriginNodeID = origin.OriginNodeID
		item.RemoteLastEventID = origin.RemoteLastEventID
		item.PendingCatchup = origin.PendingCatchup
		index[origin.OriginNodeID] = item
	}

	origins := make([]peerOriginStatusResponse, 0, len(index))
	for _, origin := range index {
		origins = append(origins, origin)
	}
	return origins
}

func connectedClusterNodes(peers []app.ClusterPeerStatus) []clusterNodeResponse {
	nodes := make([]clusterNodeResponse, 0, len(peers))
	for _, peer := range peers {
		if !peer.Connected || peer.NodeID <= 0 {
			continue
		}
		nodes = append(nodes, clusterNodeResponse{
			NodeID:        peer.NodeID,
			IsLocal:       false,
			ConfiguredURL: peer.ConfiguredURL,
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return nodes
}

func timestampString(ts *clock.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.String()
}

func timeString(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func writeMetricHelp(buf *bytes.Buffer, name, help, metricType string) {
	fmt.Fprintf(buf, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func writeGauge(buf *bytes.Buffer, name string, labels map[string]string, value float64) {
	fmt.Fprintf(buf, "%s%s %s\n", name, formatLabels(labels), strconv.FormatFloat(value, 'f', -1, 64))
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+`="`+escapeLabelValue(labels[key])+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	return replacer.Replace(value)
}
