package api

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"notifier/internal/app"
	"notifier/internal/clock"
	"notifier/internal/store"
)

type clusterStatusProvider interface {
	Status(context.Context) (app.ClusterStatus, error)
	ConfiguredPeerNodeIDs() []int64
}

type operationsStatus struct {
	NodeID            int64                `json:"node_id"`
	MessageWindowSize int                  `json:"message_window_size"`
	LastEventSequence int64                `json:"last_event_sequence"`
	WriteGateReady    bool                 `json:"write_gate_ready"`
	ConflictTotal     int64                `json:"conflict_total"`
	MessageTrim       messageTrimStatus    `json:"message_trim"`
	Peers             []peerStatusResponse `json:"peers"`
}

type messageTrimStatus struct {
	TrimmedTotal  int64  `json:"trimmed_total"`
	LastTrimmedAt string `json:"last_trimmed_at,omitempty"`
}

type peerStatusResponse struct {
	NodeID                    int64  `json:"node_id"`
	ConfiguredURL             string `json:"configured_url,omitempty"`
	Connected                 bool   `json:"connected"`
	SessionDirection          string `json:"session_direction,omitempty"`
	AckedSequence             int64  `json:"acked_sequence"`
	AppliedSequence           int64  `json:"applied_sequence"`
	UnconfirmedEvents         int64  `json:"unconfirmed_events"`
	CursorUpdatedAt           string `json:"cursor_updated_at,omitempty"`
	LastAck                   uint64 `json:"last_ack"`
	RemoteLastSequence        uint64 `json:"remote_last_sequence"`
	PendingCatchup            bool   `json:"pending_catchup"`
	PendingSnapshotPartitions int    `json:"pending_snapshot_partitions"`
	RemoteSnapshotVersion     string `json:"remote_snapshot_version,omitempty"`
	RemoteMessageWindowSize   int    `json:"remote_message_window_size,omitempty"`
	ClockOffsetMs             int64  `json:"clock_offset_ms"`
	LastClockSync             string `json:"last_clock_sync,omitempty"`
	SnapshotDigestsSentTotal  uint64 `json:"snapshot_digests_sent_total"`
	SnapshotDigestsRecvTotal  uint64 `json:"snapshot_digests_received_total"`
	SnapshotChunksSentTotal   uint64 `json:"snapshot_chunks_sent_total"`
	SnapshotChunksRecvTotal   uint64 `json:"snapshot_chunks_received_total"`
	LastSnapshotDigestAt      string `json:"last_snapshot_digest_at,omitempty"`
	LastSnapshotChunkAt       string `json:"last_snapshot_chunk_at,omitempty"`
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
		Peers: mergePeerStatus(storeStats.PeerCursors, clusterStatus.Peers),
	}
	if response.NodeID == 0 {
		response.NodeID = clusterStatus.NodeID
	}
	if response.MessageWindowSize == 0 {
		response.MessageWindowSize = clusterStatus.MessageWindowSize
	}
	return response, nil
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
	writeMetricHelp(&buf, "notifier_write_gate_ready", "Whether the node currently allows local writes.", "gauge")
	writeGauge(&buf, "notifier_write_gate_ready", map[string]string{"node_id": nodeIDLabel}, boolGauge(status.WriteGateReady))

	writeMetricHelp(&buf, "notifier_peer_connected", "Whether the peer has an active cluster session.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_unconfirmed_events", "Local events not yet acknowledged by the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_applied_sequence", "Highest peer event sequence applied locally.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_remote_last_sequence", "Highest remote sequence observed from the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_pending_snapshot_partitions", "Pending anti-entropy snapshot partitions for the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_clock_offset_ms", "Last trusted clock offset for the peer in milliseconds.", "gauge")
	for _, peer := range status.Peers {
		labels := map[string]string{
			"node_id":      nodeIDLabel,
			"peer_node_id": strconv.FormatInt(peer.NodeID, 10),
		}
		writeGauge(&buf, "notifier_peer_connected", labels, boolGauge(peer.Connected))
		writeGauge(&buf, "notifier_peer_unconfirmed_events", labels, float64(peer.UnconfirmedEvents))
		writeGauge(&buf, "notifier_peer_applied_sequence", labels, float64(peer.AppliedSequence))
		writeGauge(&buf, "notifier_peer_remote_last_sequence", labels, float64(peer.RemoteLastSequence))
		writeGauge(&buf, "notifier_peer_pending_snapshot_partitions", labels, float64(peer.PendingSnapshotPartitions))
		writeGauge(&buf, "notifier_clock_offset_ms", labels, float64(peer.ClockOffsetMs))
	}
	return buf.String(), nil
}

func mergePeerStatus(storePeers []store.PeerOperationsStats, clusterPeers []app.ClusterPeerStatus) []peerStatusResponse {
	index := make(map[int64]peerStatusResponse, len(storePeers)+len(clusterPeers))
	for _, peer := range storePeers {
		index[peer.PeerNodeID] = peerStatusResponse{
			NodeID:            peer.PeerNodeID,
			AckedSequence:     peer.AckedSequence,
			AppliedSequence:   peer.AppliedSequence,
			UnconfirmedEvents: peer.UnconfirmedEvents,
			CursorUpdatedAt:   timestampString(peer.UpdatedAt),
		}
	}
	for _, peer := range clusterPeers {
		item := index[peer.NodeID]
		item.NodeID = peer.NodeID
		item.ConfiguredURL = peer.ConfiguredURL
		item.Connected = peer.Connected
		item.SessionDirection = peer.SessionDirection
		item.LastAck = peer.LastAck
		item.RemoteLastSequence = peer.RemoteLastSequence
		item.PendingCatchup = peer.PendingCatchup
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
		index[peer.NodeID] = item
	}

	peers := make([]peerStatusResponse, 0, len(index))
	for _, peer := range index {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].NodeID < peers[j].NodeID
	})
	return peers
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
		parts = append(parts, key+"="+strconv.Quote(labels[key]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
