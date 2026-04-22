package cluster

import (
	"context"
	"sort"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
)

func statusTransport(active *session, configuredURL, discoveredURL string) string {
	if transport := sessionTransport(active); transport != "" {
		return transport
	}
	if transport := transportForPeerURL(configuredURL); transport != "" {
		return transport
	}
	return transportForPeerURL(discoveredURL)
}

func peerURLSchemeLabel(raw string) string {
	scheme, ok := peerURLScheme(raw)
	if !ok {
		return ""
	}
	return scheme
}

func (m *Manager) Status(context.Context) (app.ClusterStatus, error) {
	if m == nil {
		return app.ClusterStatus{WriteGateReady: true}, nil
	}

	type peerSnapshot struct {
		status app.ClusterPeerStatus
		active *session
	}

	m.mu.Lock()
	peers := make([]peerSnapshot, 0, len(m.configuredPeers)+len(m.peers))
	transitions := make([]app.ClockStateTransition, 0, len(m.clockStateTransitions))
	seen := make(map[int64]struct{}, len(m.peers))
	for _, configured := range m.configuredPeers {
		discovery := m.discoveryStateByURLLocked(configured.URL)
		item := app.ClusterPeerStatus{
			NodeID:                   configured.nodeID,
			ConfiguredURL:            configured.URL,
			Transport:                statusTransport(nil, configured.URL, discovery.url),
			Source:                   peerSourceStatic,
			DiscoveredURL:            discovery.url,
			DiscoveryState:           discovery.state,
			LastDiscoveredAt:         timePointer(discovery.lastSeenAt),
			LastConnectedAt:          timePointer(discovery.lastConnectedAt),
			LastDiscoveryError:       discovery.lastError,
			SnapshotDigestsSentTotal: 0,
			SnapshotDigestsRecvTotal: 0,
			SnapshotChunksSentTotal:  0,
			SnapshotChunksRecvTotal:  0,
		}
		var active *session
		if configured.nodeID > 0 {
			seen[configured.nodeID] = struct{}{}
		}
		if peer := m.peers[configured.nodeID]; peer != nil {
			active = peer.active
			item.Connected = peer.active != nil
			item.Transport = statusTransport(peer.active, configured.URL, discovery.url)
			item.ClockState = string(peer.clockState)
			item.ClockOffsetMs = peer.clockOffsetMs
			item.ClockUncertaintyMs = peer.clockUncertaintyMs
			item.ClockFailures = peer.clockFailures
			item.LastClockError = peer.clockLastError
			item.LastClockSync = timePointer(peer.lastClockSync)
			item.LastCredibleClockSync = timePointer(peer.lastCredibleClockSync)
			item.TrustedForOffset = peer.trustedSession != nil
			item.SnapshotDigestsSentTotal = peer.snapshotDigestsSent
			item.SnapshotDigestsRecvTotal = peer.snapshotDigestsReceived
			item.SnapshotChunksSentTotal = peer.snapshotChunksSent
			item.SnapshotChunksRecvTotal = peer.snapshotChunksReceived
			item.LastSnapshotDigestAt = timePointer(peer.lastSnapshotDigestAt)
			item.LastSnapshotChunkAt = timePointer(peer.lastSnapshotChunkAt)
		}
		peers = append(peers, peerSnapshot{status: item, active: active})
	}
	for nodeID, peer := range m.peers {
		if _, ok := seen[nodeID]; ok {
			continue
		}
		discovery := m.discoveryStateByNodeIDLocked(nodeID)
		source := peerSourceInbound
		if discovery.url != "" {
			source = peerSourceDiscovered
		}
		item := app.ClusterPeerStatus{
			NodeID:                   nodeID,
			Transport:                statusTransport(peer.active, "", discovery.url),
			Source:                   source,
			DiscoveredURL:            discovery.url,
			DiscoveryState:           discovery.state,
			LastDiscoveredAt:         timePointer(discovery.lastSeenAt),
			LastConnectedAt:          timePointer(discovery.lastConnectedAt),
			LastDiscoveryError:       discovery.lastError,
			SnapshotDigestsSentTotal: peer.snapshotDigestsSent,
			SnapshotDigestsRecvTotal: peer.snapshotDigestsReceived,
			SnapshotChunksSentTotal:  peer.snapshotChunksSent,
			SnapshotChunksRecvTotal:  peer.snapshotChunksReceived,
			LastSnapshotDigestAt:     timePointer(peer.lastSnapshotDigestAt),
			LastSnapshotChunkAt:      timePointer(peer.lastSnapshotChunkAt),
			Connected:                peer.active != nil,
			ClockState:               string(peer.clockState),
			ClockOffsetMs:            peer.clockOffsetMs,
			ClockUncertaintyMs:       peer.clockUncertaintyMs,
			ClockFailures:            peer.clockFailures,
			LastClockError:           peer.clockLastError,
			LastClockSync:            timePointer(peer.lastClockSync),
			LastCredibleClockSync:    timePointer(peer.lastCredibleClockSync),
			TrustedForOffset:         peer.trustedSession != nil,
		}
		peers = append(peers, peerSnapshot{status: item, active: peer.active})
	}
	for _, discovered := range m.discoveredPeers {
		if discovered == nil || discovered.nodeID <= 0 {
			continue
		}
		if _, ok := seen[discovered.nodeID]; ok {
			continue
		}
		if _, ok := m.peers[discovered.nodeID]; ok {
			continue
		}
		seen[discovered.nodeID] = struct{}{}
		peers = append(peers, peerSnapshot{status: app.ClusterPeerStatus{
			NodeID:             discovered.nodeID,
			Transport:          statusTransport(nil, "", discovered.url),
			Source:             peerSourceDiscovered,
			DiscoveredURL:      discovered.url,
			DiscoveryState:     discovered.state,
			LastDiscoveredAt:   timePointer(discovered.lastSeenAt),
			LastConnectedAt:    timePointer(discovered.lastConnectedAt),
			LastDiscoveryError: discovered.lastError,
		}})
	}
	m.refreshNodeClockStateLocked()
	writeGateReady := m.hasWritableClockSyncLocked()
	clockState := m.clockState
	clockReason := m.clockReason
	lastTrustedClockSync := timePointer(m.lastTrustedClockSync)
	discovery := app.ClusterDiscoveryStatus{
		DiscoveredPeers:       len(m.discoveredPeers),
		DynamicPeers:          len(m.dynamicPeers),
		MembershipUpdatesSent: m.membershipUpdatesSent,
		MembershipUpdatesRecv: m.membershipUpdatesRecv,
		RejectedTotal:         m.discoveryRejects,
		PersistFailuresTotal:  m.discoveryPersistFailures,
		PeersByState:          make(map[string]int),
		PeersByScheme:         make(map[string]int),
		ZeroMQMode:            m.cfg.zeroMQMode(),
		ZeroMQSecurity:        m.cfg.zeroMQSecurity(),
		ZeroMQListenerRunning: m.zeroMQListenerRunning,
		LibP2PMode:            m.cfg.libP2PMode(),
	}
	if m.libp2p != nil {
		discovery.LibP2PPeerID = m.libp2p.PeerID()
		discovery.LibP2PListenAddrs = m.libp2p.ListenAddrs()
		discovery.LibP2PVerifiedAddrs = m.libP2PVerifiedAddrsLocked()
		discovery.LibP2PDHTEnabled = m.cfg.LibP2P.EnableDHT
		discovery.LibP2PDHTBootstrapped = m.libp2p.DHTBootstrapped()
		discovery.LibP2PGossipSubTopic = m.libp2p.Topic()
		discovery.LibP2PGossipSubPeers = m.libp2p.TopicPeers()
		discovery.LibP2PRelayEnabled = len(m.cfg.LibP2P.RelayPeers) > 0
		discovery.LibP2PHolePunching = m.cfg.LibP2P.EnableHolePunching && len(m.cfg.LibP2P.RelayPeers) > 0
	}
	for _, peer := range m.discoveredPeers {
		if peer == nil || peer.state == "" {
			continue
		}
		discovery.PeersByState[peer.state]++
		if scheme := peerURLSchemeLabel(peer.url); scheme != "" {
			discovery.PeersByScheme[scheme]++
		}
	}
	for key, total := range m.clockStateTransitions {
		transitions = append(transitions, app.ClockStateTransition{
			FromState: key.FromState,
			ToState:   key.ToState,
			Reason:    key.Reason,
			Total:     total,
		})
	}
	m.mu.Unlock()

	status := app.ClusterStatus{
		NodeID:               m.cfg.NodeID,
		MessageWindowSize:    m.cfg.MessageWindowSize,
		WriteGateReady:       writeGateReady,
		ClockState:           string(clockState),
		ClockReason:          clockReason,
		LastTrustedClockSync: lastTrustedClockSync,
		ClockTransitions:     transitions,
		Discovery:            discovery,
		Mesh:                 m.meshStatusSnapshot(),
		Peers:                make([]app.ClusterPeerStatus, 0, len(peers)),
	}
	for _, peer := range peers {
		item := peer.status
		if peer.active != nil {
			item.Transport = statusTransport(peer.active, item.ConfiguredURL, item.DiscoveredURL)
			item.SessionDirection = peer.active.direction()
			item.Origins = peer.active.originStatuses()
			item.PendingSnapshotPartitions = peer.active.pendingSnapshotCount()
			item.RemoteSnapshotVersion = peer.active.snapshotVersion()
			item.RemoteMessageWindowSize = peer.active.messageWindowSize()
		}
		status.Peers = append(status.Peers, item)
	}
	return status, nil
}

func (m *Manager) meshStatusSnapshot() app.ClusterMeshStatus {
	binding := m.MeshRuntime()
	if binding == nil {
		return app.ClusterMeshStatus{}
	}
	runtime := binding.Runtime()
	if runtime == nil {
		return app.ClusterMeshStatus{}
	}
	policy := runtime.LocalPolicy()
	status := app.ClusterMeshStatus{
		Enabled:            true,
		ForwardingEnabled:  policy.GetTransitEnabled(),
		BridgeEnabled:      policy.GetBridgeEnabled(),
		NodeFeeWeight:      policy.GetNodeFeeWeight(),
		TopologyGeneration: binding.TopologyStore().Snapshot().TopologyGeneration,
	}
	for _, capability := range runtime.LocalCapabilities() {
		if capability == nil {
			continue
		}
		status.TransportCapabilities = append(status.TransportCapabilities, app.ClusterMeshTransportCapability{
			Transport:                 meshTransportLabel(capability.Transport),
			InboundEnabled:            capability.InboundEnabled,
			OutboundEnabled:           capability.OutboundEnabled,
			NativeRelayClientEnabled:  capability.NativeRelayClientEnabled,
			NativeRelayServiceEnabled: capability.NativeRelayServiceEnabled,
			AdvertisedEndpoints:       append([]string(nil), capability.AdvertisedEndpoints...),
		})
	}
	sort.Slice(status.TransportCapabilities, func(i, j int) bool {
		return status.TransportCapabilities[i].Transport < status.TransportCapabilities[j].Transport
	})
	for _, rule := range policy.GetTrafficRules() {
		if rule == nil {
			continue
		}
		status.TrafficRules = append(status.TrafficRules, app.ClusterMeshTrafficRule{
			TrafficClass: meshTrafficClassLabel(rule.TrafficClass),
			Disposition:  meshDispositionLabel(rule.Disposition),
		})
	}
	sort.Slice(status.TrafficRules, func(i, j int) bool {
		return status.TrafficRules[i].TrafficClass < status.TrafficRules[j].TrafficClass
	})
	snapshot := binding.TopologyStore().Snapshot()
	metrics := m.meshMetricsSnapshot()
	for _, sample := range metrics.ForwardedPackets {
		status.Metrics.ForwardedPackets = append(status.Metrics.ForwardedPackets, app.ClusterMeshMetricSample(sample))
	}
	for _, sample := range metrics.ForwardedBytes {
		status.Metrics.ForwardedBytes = append(status.Metrics.ForwardedBytes, app.ClusterMeshMetricSample(sample))
	}
	for _, sample := range metrics.RoutingNoPath {
		status.Metrics.RoutingNoPath = append(status.Metrics.RoutingNoPath, app.ClusterMeshMetricSample(sample))
	}
	for _, sample := range metrics.DecisionCost {
		status.Metrics.DecisionCost = append(status.Metrics.DecisionCost, app.ClusterMeshCostSample(sample))
	}
	for _, sample := range metrics.BridgeForwards {
		status.Metrics.BridgeForwards = append(status.Metrics.BridgeForwards, app.ClusterMeshMetricSample(sample))
	}
	trafficClasses := []mesh.TrafficClass{
		mesh.TrafficControlCritical,
		mesh.TrafficControlQuery,
		mesh.TrafficTransientInteractive,
		mesh.TrafficReplicationStream,
		mesh.TrafficSnapshotBulk,
	}
	for destinationNodeID := range snapshot.Nodes {
		if destinationNodeID <= 0 || destinationNodeID == m.cfg.NodeID {
			continue
		}
		for _, trafficClass := range trafficClasses {
			route := app.ClusterMeshRoute{
				DestinationNodeID:  destinationNodeID,
				TrafficClass:       meshTrafficClassLabel(trafficClass),
				Reachable:          false,
				TopologyGeneration: snapshot.TopologyGeneration,
			}
			if decision, ok := binding.DescribeRoute(destinationNodeID, trafficClass); ok {
				route.Reachable = true
				route.NextHopNodeID = decision.NextHopNodeID
				route.OutboundTransport = meshTransportLabel(decision.OutboundTransport)
				route.PathClass = meshPathClassLabel(decision.PathClass)
				route.EstimatedCost = decision.EstimatedCost
				route.TopologyGeneration = decision.TopologyGeneration
			}
			status.Routes = append(status.Routes, route)
		}
	}
	sort.Slice(status.Routes, func(i, j int) bool {
		if status.Routes[i].DestinationNodeID != status.Routes[j].DestinationNodeID {
			return status.Routes[i].DestinationNodeID < status.Routes[j].DestinationNodeID
		}
		return status.Routes[i].TrafficClass < status.Routes[j].TrafficClass
	})
	return status
}

func (m *Manager) libP2PVerifiedAddrsLocked() []string {
	seen := make(map[string]struct{})
	addrs := make([]string, 0)
	add := func(raw string) {
		if !isLibP2PPeerURL(raw) {
			return
		}
		if _, ok := seen[raw]; ok {
			return
		}
		seen[raw] = struct{}{}
		addrs = append(addrs, raw)
	}
	for _, peer := range m.configuredPeers {
		if peer != nil && peer.nodeID > 0 {
			add(peer.URL)
		}
	}
	for _, peer := range m.discoveredPeers {
		if peer != nil && peer.nodeID > 0 && peer.state == discoveryStateConnected {
			add(peer.url)
		}
	}
	for rawURL := range m.selfKnownURLs {
		add(rawURL)
	}
	sort.Strings(addrs)
	return addrs
}

func (m *Manager) ConfiguredPeerNodeIDs() []int64 {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[int64]struct{}, len(m.configuredPeers)+len(m.discoveredPeers))
	ids := make([]int64, 0, len(m.configuredPeers)+len(m.discoveredPeers))
	for _, peer := range m.configuredPeers {
		if peer.nodeID <= 0 {
			continue
		}
		if _, ok := seen[peer.nodeID]; ok {
			continue
		}
		seen[peer.nodeID] = struct{}{}
		ids = append(ids, peer.nodeID)
	}
	for _, peer := range m.discoveredPeers {
		if peer == nil || peer.nodeID <= 0 {
			continue
		}
		if _, ok := seen[peer.nodeID]; ok {
			continue
		}
		seen[peer.nodeID] = struct{}{}
		ids = append(ids, peer.nodeID)
	}
	return ids
}

func (m *Manager) markSnapshotDigestSent(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotDigestsSent++
		peer.lastSnapshotDigestAt = now
	})
}

func (m *Manager) markSnapshotDigestReceived(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotDigestsReceived++
		peer.lastSnapshotDigestAt = now
	})
}

func (m *Manager) markSnapshotChunkSent(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotChunksSent++
		peer.lastSnapshotChunkAt = now
	})
}

func (m *Manager) markSnapshotChunkReceived(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotChunksReceived++
		peer.lastSnapshotChunkAt = now
	})
}

func (m *Manager) markSnapshotActivity(peerID int64, update func(*peerState, time.Time)) {
	if m == nil || update == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[peerID]
	if !ok {
		return
	}
	update(peer, time.Now().UTC())
}

func (s *session) direction() string {
	if s.outbound {
		return "outbound"
	}
	return "inbound"
}

func (s *session) pendingSnapshotCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingSnapshotParts)
}

func (s *session) snapshotVersion() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteSnapshotVersion
}

func (s *session) messageWindowSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteMessageWindowSize
}

func (s *session) originStatuses() []app.ClusterPeerOriginStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	origins := make([]app.ClusterPeerOriginStatus, 0, len(s.remoteOriginProgress))
	for originNodeID, lastEventID := range s.remoteOriginProgress {
		_, pending := s.pendingPulls[originNodeID]
		origins = append(origins, app.ClusterPeerOriginStatus{
			OriginNodeID:      originNodeID,
			RemoteLastEventID: lastEventID,
			PendingCatchup:    pending,
		})
	}
	sort.Slice(origins, func(i, j int) bool {
		return origins[i].OriginNodeID < origins[j].OriginNodeID
	})
	return origins
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copied := value.UTC()
	return &copied
}
