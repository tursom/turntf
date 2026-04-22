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
	Source        string `json:"source,omitempty"`
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
	NodeID               int64                `json:"node_id"`
	MessageWindowSize    int                  `json:"message_window_size"`
	LastEventSequence    int64                `json:"last_event_sequence"`
	WriteGateReady       bool                 `json:"write_gate_ready"`
	ClockState           string               `json:"clock_state,omitempty"`
	ClockReason          string               `json:"clock_reason,omitempty"`
	LastTrustedClockSync string               `json:"last_trusted_clock_sync,omitempty"`
	ConflictTotal        int64                `json:"conflict_total"`
	MessageTrim          messageTrimStatus    `json:"message_trim"`
	Projection           projectionStatus     `json:"projection"`
	Discovery            discoveryStatus      `json:"discovery"`
	Mesh                 meshStatus           `json:"mesh,omitempty"`
	Peers                []peerStatusResponse `json:"peers"`
}

type meshStatus struct {
	Enabled               bool                      `json:"enabled"`
	ForwardingEnabled     bool                      `json:"forwarding_enabled"`
	BridgeEnabled         bool                      `json:"bridge_enabled"`
	NodeFeeWeight         int64                     `json:"node_fee_weight"`
	TopologyGeneration    uint64                    `json:"topology_generation"`
	TransportCapabilities []meshTransportCapability `json:"transport_capabilities,omitempty"`
	TrafficRules          []meshTrafficRule         `json:"traffic_rules,omitempty"`
	Routes                []meshRoute               `json:"routes,omitempty"`
	Metrics               meshMetrics               `json:"metrics,omitempty"`
}

type meshTransportCapability struct {
	Transport                 string   `json:"transport"`
	InboundEnabled            bool     `json:"inbound_enabled"`
	OutboundEnabled           bool     `json:"outbound_enabled"`
	NativeRelayClientEnabled  bool     `json:"native_relay_client_enabled"`
	NativeRelayServiceEnabled bool     `json:"native_relay_service_enabled"`
	AdvertisedEndpoints       []string `json:"advertised_endpoints,omitempty"`
}

type meshTrafficRule struct {
	TrafficClass string `json:"traffic_class"`
	Disposition  string `json:"disposition"`
}

type meshRoute struct {
	DestinationNodeID  int64  `json:"destination_node_id"`
	TrafficClass       string `json:"traffic_class"`
	Reachable          bool   `json:"reachable"`
	NextHopNodeID      int64  `json:"next_hop_node_id,omitempty"`
	OutboundTransport  string `json:"outbound_transport,omitempty"`
	PathClass          string `json:"path_class,omitempty"`
	EstimatedCost      int64  `json:"estimated_cost,omitempty"`
	TopologyGeneration uint64 `json:"topology_generation"`
}

type meshMetrics struct {
	ForwardedPackets []meshMetricSample `json:"forwarded_packets,omitempty"`
	ForwardedBytes   []meshMetricSample `json:"forwarded_bytes,omitempty"`
	RoutingNoPath    []meshMetricSample `json:"routing_no_path,omitempty"`
	DecisionCost     []meshCostSample   `json:"decision_cost,omitempty"`
	BridgeForwards   []meshMetricSample `json:"bridge_forwards,omitempty"`
}

type meshMetricSample struct {
	TrafficClass string `json:"traffic_class"`
	PathClass    string `json:"path_class,omitempty"`
	Value        uint64 `json:"value"`
}

type meshCostSample struct {
	TrafficClass string `json:"traffic_class"`
	Value        int64  `json:"value"`
}

type discoveryStatus struct {
	DiscoveredPeers       int            `json:"discovered_peers"`
	DynamicPeers          int            `json:"dynamic_peers"`
	MembershipUpdatesSent uint64         `json:"membership_updates_sent"`
	MembershipUpdatesRecv uint64         `json:"membership_updates_received"`
	RejectedTotal         uint64         `json:"rejected_total"`
	PersistFailuresTotal  uint64         `json:"persist_failures_total"`
	PeersByState          map[string]int `json:"peers_by_state,omitempty"`
	PeersByScheme         map[string]int `json:"peers_by_scheme,omitempty"`
	ZeroMQMode            string         `json:"zeromq_mode,omitempty"`
	ZeroMQSecurity        string         `json:"zeromq_security,omitempty"`
	ZeroMQListenerRunning bool           `json:"zeromq_listener_running"`
	LibP2PMode            string         `json:"libp2p_mode,omitempty"`
	LibP2PPeerID          string         `json:"libp2p_peer_id,omitempty"`
	LibP2PListenAddrs     []string       `json:"libp2p_listen_addrs,omitempty"`
	LibP2PVerifiedAddrs   []string       `json:"libp2p_verified_addrs,omitempty"`
	LibP2PDHTEnabled      bool           `json:"libp2p_dht_enabled"`
	LibP2PDHTBootstrapped bool           `json:"libp2p_dht_bootstrapped"`
	LibP2PGossipSubTopic  string         `json:"libp2p_gossipsub_topic,omitempty"`
	LibP2PGossipSubPeers  int            `json:"libp2p_gossipsub_peers"`
	LibP2PRelayEnabled    bool           `json:"libp2p_relay_enabled"`
	LibP2PHolePunching    bool           `json:"libp2p_hole_punching"`
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
	Transport                 string                     `json:"transport,omitempty"`
	Source                    string                     `json:"source,omitempty"`
	DiscoveredURL             string                     `json:"discovered_url,omitempty"`
	DiscoveryState            string                     `json:"discovery_state,omitempty"`
	LastDiscoveredAt          string                     `json:"last_discovered_at,omitempty"`
	LastConnectedAt           string                     `json:"last_connected_at,omitempty"`
	LastDiscoveryError        string                     `json:"last_discovery_error,omitempty"`
	Connected                 bool                       `json:"connected"`
	SessionDirection          string                     `json:"session_direction,omitempty"`
	Origins                   []peerOriginStatusResponse `json:"origins"`
	PendingSnapshotPartitions int                        `json:"pending_snapshot_partitions"`
	RemoteSnapshotVersion     string                     `json:"remote_snapshot_version,omitempty"`
	RemoteMessageWindowSize   int                        `json:"remote_message_window_size,omitempty"`
	ClockOffsetMs             int64                      `json:"clock_offset_ms"`
	ClockState                string                     `json:"clock_state,omitempty"`
	ClockUncertaintyMs        int64                      `json:"clock_uncertainty_ms"`
	ClockFailures             uint64                     `json:"clock_failures"`
	LastClockError            string                     `json:"last_clock_error,omitempty"`
	LastClockSync             string                     `json:"last_clock_sync,omitempty"`
	LastCredibleClockSync     string                     `json:"last_credible_clock_sync,omitempty"`
	TrustedForOffset          bool                       `json:"trusted_for_offset"`
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
		NodeID:               storeStats.NodeID,
		MessageWindowSize:    storeStats.MessageWindowSize,
		LastEventSequence:    storeStats.LastEventSequence,
		WriteGateReady:       clusterStatus.WriteGateReady,
		ClockState:           clusterStatus.ClockState,
		ClockReason:          clusterStatus.ClockReason,
		LastTrustedClockSync: timeString(clusterStatus.LastTrustedClockSync),
		ConflictTotal:        storeStats.UserConflictsTotal,
		MessageTrim: messageTrimStatus{
			TrimmedTotal:  storeStats.MessageTrim.TrimmedTotal,
			LastTrimmedAt: timestampString(storeStats.MessageTrim.LastTrimmedAt),
		},
		Projection: projectionStatus{
			PendingTotal: storeStats.Projection.PendingTotal,
			LastFailedAt: timestampString(storeStats.Projection.LastFailedAt),
		},
		Discovery: discoveryStatus{
			DiscoveredPeers:       clusterStatus.Discovery.DiscoveredPeers,
			DynamicPeers:          clusterStatus.Discovery.DynamicPeers,
			MembershipUpdatesSent: clusterStatus.Discovery.MembershipUpdatesSent,
			MembershipUpdatesRecv: clusterStatus.Discovery.MembershipUpdatesRecv,
			RejectedTotal:         clusterStatus.Discovery.RejectedTotal,
			PersistFailuresTotal:  clusterStatus.Discovery.PersistFailuresTotal,
			PeersByState:          clusterStatus.Discovery.PeersByState,
			PeersByScheme:         clusterStatus.Discovery.PeersByScheme,
			ZeroMQMode:            clusterStatus.Discovery.ZeroMQMode,
			ZeroMQSecurity:        clusterStatus.Discovery.ZeroMQSecurity,
			ZeroMQListenerRunning: clusterStatus.Discovery.ZeroMQListenerRunning,
			LibP2PMode:            clusterStatus.Discovery.LibP2PMode,
			LibP2PPeerID:          clusterStatus.Discovery.LibP2PPeerID,
			LibP2PListenAddrs:     clusterStatus.Discovery.LibP2PListenAddrs,
			LibP2PVerifiedAddrs:   clusterStatus.Discovery.LibP2PVerifiedAddrs,
			LibP2PDHTEnabled:      clusterStatus.Discovery.LibP2PDHTEnabled,
			LibP2PDHTBootstrapped: clusterStatus.Discovery.LibP2PDHTBootstrapped,
			LibP2PGossipSubTopic:  clusterStatus.Discovery.LibP2PGossipSubTopic,
			LibP2PGossipSubPeers:  clusterStatus.Discovery.LibP2PGossipSubPeers,
			LibP2PRelayEnabled:    clusterStatus.Discovery.LibP2PRelayEnabled,
			LibP2PHolePunching:    clusterStatus.Discovery.LibP2PHolePunching,
		},
		Mesh:  meshStatusFromCluster(clusterStatus.Mesh),
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
	writeMetricHelp(&buf, "notifier_blacklist_rejected_total", "Total message or packet deliveries rejected by blacklist rules.", "counter")
	writeGauge(&buf, "notifier_blacklist_rejected_total", map[string]string{"node_id": nodeIDLabel}, float64(s.BlacklistHitsTotal()))
	writeMetricHelp(&buf, "notifier_pending_projections", "Pending event projections waiting to be replayed.", "gauge")
	writeGauge(&buf, "notifier_pending_projections", map[string]string{"node_id": nodeIDLabel}, float64(status.Projection.PendingTotal))
	writeMetricHelp(&buf, "notifier_write_gate_ready", "Whether the node currently allows local writes.", "gauge")
	writeGauge(&buf, "notifier_write_gate_ready", map[string]string{"node_id": nodeIDLabel}, boolGauge(status.WriteGateReady))
	writeMetricHelp(&buf, "notifier_clock_state", "Current aggregated cluster clock state for the node.", "gauge")
	writeGauge(&buf, "notifier_clock_state", map[string]string{"node_id": nodeIDLabel, "state": status.ClockState}, 1)
	writeMetricHelp(&buf, "notifier_discovered_peers", "Total discovered peer records known by the node.", "gauge")
	writeGauge(&buf, "notifier_discovered_peers", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.DiscoveredPeers))
	writeMetricHelp(&buf, "notifier_discovered_peers_by_state", "Discovered peer records grouped by discovery state.", "gauge")
	for state, total := range status.Discovery.PeersByState {
		writeGauge(&buf, "notifier_discovered_peers_by_state", map[string]string{"node_id": nodeIDLabel, "state": state}, float64(total))
	}
	writeMetricHelp(&buf, "notifier_discovered_peers_by_scheme", "Discovered peer records grouped by peer URL scheme.", "gauge")
	for scheme, total := range status.Discovery.PeersByScheme {
		writeGauge(&buf, "notifier_discovered_peers_by_scheme", map[string]string{"node_id": nodeIDLabel, "scheme": scheme}, float64(total))
	}
	writeMetricHelp(&buf, "notifier_dynamic_peer_dialers", "Dynamic peer dial loops started from discovery.", "gauge")
	writeGauge(&buf, "notifier_dynamic_peer_dialers", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.DynamicPeers))
	writeMetricHelp(&buf, "notifier_zeromq_listener_running", "Whether the local ZeroMQ listener is currently running.", "gauge")
	writeGauge(&buf, "notifier_zeromq_listener_running", map[string]string{
		"node_id":  nodeIDLabel,
		"mode":     status.Discovery.ZeroMQMode,
		"security": status.Discovery.ZeroMQSecurity,
	}, boolGauge(status.Discovery.ZeroMQListenerRunning))
	writeMetricHelp(&buf, "notifier_libp2p_enabled", "Whether libp2p cluster transport is enabled.", "gauge")
	writeGauge(&buf, "notifier_libp2p_enabled", map[string]string{
		"node_id": nodeIDLabel,
		"mode":    status.Discovery.LibP2PMode,
	}, boolGauge(status.Discovery.LibP2PMode != "" && status.Discovery.LibP2PMode != "disabled"))
	writeMetricHelp(&buf, "notifier_libp2p_gossipsub_peers", "Current libp2p Gossipsub topic peer count.", "gauge")
	writeGauge(&buf, "notifier_libp2p_gossipsub_peers", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.LibP2PGossipSubPeers))
	writeMetricHelp(&buf, "notifier_libp2p_dht_bootstrapped", "Whether libp2p DHT bootstrap has completed.", "gauge")
	writeGauge(&buf, "notifier_libp2p_dht_bootstrapped", map[string]string{"node_id": nodeIDLabel}, boolGauge(status.Discovery.LibP2PDHTBootstrapped))
	writeMetricHelp(&buf, "notifier_membership_updates_sent_total", "Membership updates sent to peers.", "counter")
	writeGauge(&buf, "notifier_membership_updates_sent_total", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.MembershipUpdatesSent))
	writeMetricHelp(&buf, "notifier_membership_updates_received_total", "Membership updates received from peers.", "counter")
	writeGauge(&buf, "notifier_membership_updates_received_total", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.MembershipUpdatesRecv))
	writeMetricHelp(&buf, "notifier_membership_advertisements_rejected_total", "Membership advertisements rejected by discovery validation.", "counter")
	writeGauge(&buf, "notifier_membership_advertisements_rejected_total", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.RejectedTotal))
	writeMetricHelp(&buf, "notifier_discovered_peer_persist_failures_total", "Discovered peer persistence failures.", "counter")
	writeGauge(&buf, "notifier_discovered_peer_persist_failures_total", map[string]string{"node_id": nodeIDLabel}, float64(status.Discovery.PersistFailuresTotal))
	writeMetricHelp(&buf, "forwarded_packets_total", "Mesh forwarded packets grouped by traffic and path class.", "counter")
	for _, sample := range status.Mesh.Metrics.ForwardedPackets {
		writeGauge(&buf, "forwarded_packets_total", map[string]string{
			"node_id":       nodeIDLabel,
			"traffic_class": sample.TrafficClass,
			"path_class":    sample.PathClass,
		}, float64(sample.Value))
	}
	writeMetricHelp(&buf, "forwarded_bytes_total", "Mesh forwarded bytes grouped by traffic and path class.", "counter")
	for _, sample := range status.Mesh.Metrics.ForwardedBytes {
		writeGauge(&buf, "forwarded_bytes_total", map[string]string{
			"node_id":       nodeIDLabel,
			"traffic_class": sample.TrafficClass,
			"path_class":    sample.PathClass,
		}, float64(sample.Value))
	}
	writeMetricHelp(&buf, "routing_decision_cost", "Last observed mesh routing decision cost by traffic class.", "gauge")
	for _, sample := range status.Mesh.Metrics.DecisionCost {
		writeGauge(&buf, "routing_decision_cost", map[string]string{
			"node_id":       nodeIDLabel,
			"traffic_class": sample.TrafficClass,
		}, float64(sample.Value))
	}
	writeMetricHelp(&buf, "routing_no_path_total", "Mesh routing no-path decisions grouped by traffic class.", "counter")
	for _, sample := range status.Mesh.Metrics.RoutingNoPath {
		writeGauge(&buf, "routing_no_path_total", map[string]string{
			"node_id":       nodeIDLabel,
			"traffic_class": sample.TrafficClass,
		}, float64(sample.Value))
	}
	writeMetricHelp(&buf, "topology_generation", "Current mesh topology generation.", "gauge")
	writeGauge(&buf, "topology_generation", map[string]string{"node_id": nodeIDLabel}, float64(status.Mesh.TopologyGeneration))
	writeMetricHelp(&buf, "node_fee_weight", "Local mesh node fee weight.", "gauge")
	writeGauge(&buf, "node_fee_weight", map[string]string{"node_id": nodeIDLabel}, float64(status.Mesh.NodeFeeWeight))
	writeMetricHelp(&buf, "bridge_forward_total", "Mesh cross-transport bridge forwards grouped by traffic class.", "counter")
	for _, sample := range status.Mesh.Metrics.BridgeForwards {
		writeGauge(&buf, "bridge_forward_total", map[string]string{
			"node_id":       nodeIDLabel,
			"traffic_class": sample.TrafficClass,
		}, float64(sample.Value))
	}

	writeMetricHelp(&buf, "notifier_peer_connected", "Whether the peer has an active cluster session.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_acked_event_id", "Highest origin event id acknowledged by the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_applied_event_id", "Highest origin event id applied locally.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_remote_last_event_id", "Highest remote origin event id observed from the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_origin_unconfirmed_events", "Local origin events not yet acknowledged by the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_pending_snapshot_partitions", "Pending anti-entropy snapshot partitions for the peer.", "gauge")
	writeMetricHelp(&buf, "notifier_clock_offset_ms", "Last trusted clock offset for the peer in milliseconds.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_clock_state", "Current per-peer clock state.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_clock_uncertainty_ms", "Current per-peer clock uncertainty in milliseconds.", "gauge")
	writeMetricHelp(&buf, "notifier_peer_clock_failures_total", "Total per-peer time sync failures recorded by clock protection.", "counter")
	for _, peer := range status.Peers {
		if peer.NodeID <= 0 {
			continue
		}
		peerLabels := map[string]string{
			"node_id":      nodeIDLabel,
			"peer_node_id": strconv.FormatInt(peer.NodeID, 10),
		}
		if peer.Transport != "" {
			peerLabels["transport"] = peer.Transport
		}
		writeGauge(&buf, "notifier_peer_connected", peerLabels, boolGauge(peer.Connected))
		writeGauge(&buf, "notifier_peer_pending_snapshot_partitions", peerLabels, float64(peer.PendingSnapshotPartitions))
		writeGauge(&buf, "notifier_clock_offset_ms", peerLabels, float64(peer.ClockOffsetMs))
		if peer.ClockState != "" {
			clockLabels := map[string]string{
				"node_id":      nodeIDLabel,
				"peer_node_id": strconv.FormatInt(peer.NodeID, 10),
				"state":        peer.ClockState,
			}
			if peer.Transport != "" {
				clockLabels["transport"] = peer.Transport
			}
			writeGauge(&buf, "notifier_peer_clock_state", clockLabels, 1)
		}
		writeGauge(&buf, "notifier_peer_clock_uncertainty_ms", peerLabels, float64(peer.ClockUncertaintyMs))
		writeGauge(&buf, "notifier_peer_clock_failures_total", peerLabels, float64(peer.ClockFailures))
		for _, origin := range peer.Origins {
			labels := map[string]string{
				"node_id":        nodeIDLabel,
				"peer_node_id":   strconv.FormatInt(peer.NodeID, 10),
				"origin_node_id": strconv.FormatInt(origin.OriginNodeID, 10),
			}
			if peer.Transport != "" {
				labels["transport"] = peer.Transport
			}
			writeGauge(&buf, "notifier_peer_origin_acked_event_id", labels, float64(origin.AckedEventID))
			writeGauge(&buf, "notifier_peer_origin_applied_event_id", labels, float64(origin.AppliedEventID))
			writeGauge(&buf, "notifier_peer_origin_remote_last_event_id", labels, float64(origin.RemoteLastEventID))
			writeGauge(&buf, "notifier_peer_origin_unconfirmed_events", labels, float64(origin.UnconfirmedEvents))
		}
	}
	writeMetricHelp(&buf, "notifier_clock_state_transitions_total", "Total aggregated node clock state transitions.", "counter")
	if provider, ok := s.eventSink.(clusterStatusProvider); ok {
		clusterStatus, err := provider.Status(ctx)
		if err != nil {
			return "", err
		}
		for _, transition := range clusterStatus.ClockTransitions {
			writeGauge(&buf, "notifier_clock_state_transitions_total", map[string]string{
				"node_id": nodeIDLabel,
				"from":    transition.FromState,
				"to":      transition.ToState,
				"reason":  transition.Reason,
			}, float64(transition.Total))
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
				Transport:                 peer.Transport,
				Source:                    peer.Source,
				DiscoveredURL:             peer.DiscoveredURL,
				DiscoveryState:            peer.DiscoveryState,
				LastDiscoveredAt:          timeString(peer.LastDiscoveredAt),
				LastConnectedAt:           timeString(peer.LastConnectedAt),
				LastDiscoveryError:        peer.LastDiscoveryError,
				Connected:                 peer.Connected,
				SessionDirection:          peer.SessionDirection,
				PendingSnapshotPartitions: peer.PendingSnapshotPartitions,
				RemoteSnapshotVersion:     peer.RemoteSnapshotVersion,
				RemoteMessageWindowSize:   peer.RemoteMessageWindowSize,
				ClockState:                peer.ClockState,
				ClockOffsetMs:             peer.ClockOffsetMs,
				ClockUncertaintyMs:        peer.ClockUncertaintyMs,
				ClockFailures:             peer.ClockFailures,
				LastClockError:            peer.LastClockError,
				LastClockSync:             timeString(peer.LastClockSync),
				LastCredibleClockSync:     timeString(peer.LastCredibleClockSync),
				TrustedForOffset:          peer.TrustedForOffset,
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
		item.Transport = peer.Transport
		item.Source = peer.Source
		item.DiscoveredURL = peer.DiscoveredURL
		item.DiscoveryState = peer.DiscoveryState
		item.LastDiscoveredAt = timeString(peer.LastDiscoveredAt)
		item.LastConnectedAt = timeString(peer.LastConnectedAt)
		item.LastDiscoveryError = peer.LastDiscoveryError
		item.Connected = peer.Connected
		item.SessionDirection = peer.SessionDirection
		item.PendingSnapshotPartitions = peer.PendingSnapshotPartitions
		item.RemoteSnapshotVersion = peer.RemoteSnapshotVersion
		item.RemoteMessageWindowSize = peer.RemoteMessageWindowSize
		item.ClockState = peer.ClockState
		item.ClockOffsetMs = peer.ClockOffsetMs
		item.ClockUncertaintyMs = peer.ClockUncertaintyMs
		item.ClockFailures = peer.ClockFailures
		item.LastClockError = peer.LastClockError
		item.LastClockSync = timeString(peer.LastClockSync)
		item.LastCredibleClockSync = timeString(peer.LastCredibleClockSync)
		item.TrustedForOffset = peer.TrustedForOffset
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

func meshStatusFromCluster(status app.ClusterMeshStatus) meshStatus {
	out := meshStatus{
		Enabled:            status.Enabled,
		ForwardingEnabled:  status.ForwardingEnabled,
		BridgeEnabled:      status.BridgeEnabled,
		NodeFeeWeight:      status.NodeFeeWeight,
		TopologyGeneration: status.TopologyGeneration,
	}
	for _, capability := range status.TransportCapabilities {
		out.TransportCapabilities = append(out.TransportCapabilities, meshTransportCapability{
			Transport:                 capability.Transport,
			InboundEnabled:            capability.InboundEnabled,
			OutboundEnabled:           capability.OutboundEnabled,
			NativeRelayClientEnabled:  capability.NativeRelayClientEnabled,
			NativeRelayServiceEnabled: capability.NativeRelayServiceEnabled,
			AdvertisedEndpoints:       append([]string(nil), capability.AdvertisedEndpoints...),
		})
	}
	for _, rule := range status.TrafficRules {
		out.TrafficRules = append(out.TrafficRules, meshTrafficRule{
			TrafficClass: rule.TrafficClass,
			Disposition:  rule.Disposition,
		})
	}
	for _, route := range status.Routes {
		out.Routes = append(out.Routes, meshRoute{
			DestinationNodeID:  route.DestinationNodeID,
			TrafficClass:       route.TrafficClass,
			Reachable:          route.Reachable,
			NextHopNodeID:      route.NextHopNodeID,
			OutboundTransport:  route.OutboundTransport,
			PathClass:          route.PathClass,
			EstimatedCost:      route.EstimatedCost,
			TopologyGeneration: route.TopologyGeneration,
		})
	}
	for _, sample := range status.Metrics.ForwardedPackets {
		out.Metrics.ForwardedPackets = append(out.Metrics.ForwardedPackets, meshMetricSample(sample))
	}
	for _, sample := range status.Metrics.ForwardedBytes {
		out.Metrics.ForwardedBytes = append(out.Metrics.ForwardedBytes, meshMetricSample(sample))
	}
	for _, sample := range status.Metrics.RoutingNoPath {
		out.Metrics.RoutingNoPath = append(out.Metrics.RoutingNoPath, meshMetricSample(sample))
	}
	for _, sample := range status.Metrics.DecisionCost {
		out.Metrics.DecisionCost = append(out.Metrics.DecisionCost, meshCostSample(sample))
	}
	for _, sample := range status.Metrics.BridgeForwards {
		out.Metrics.BridgeForwards = append(out.Metrics.BridgeForwards, meshMetricSample(sample))
	}
	return out
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
		peerURL := peer.ConfiguredURL
		if peerURL == "" {
			peerURL = peer.DiscoveredURL
		}
		nodes = append(nodes, clusterNodeResponse{
			NodeID:        peer.NodeID,
			IsLocal:       false,
			ConfiguredURL: peerURL,
			Source:        peer.Source,
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
