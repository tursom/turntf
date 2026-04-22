package cluster

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tursom/turntf/internal/mesh"
)

// MeshRuntimeBinding owns a mesh.Runtime that reuses the Manager's
// transports. Inbound connections are pushed via the adapters'
// InjectInbound method; outbound dials reuse the Manager's dialers.
type MeshRuntimeBinding struct {
	runtime  *mesh.Runtime
	store    mesh.TopologyStore
	adapters map[mesh.TransportKind]*meshInboundAdapter

	mu      sync.Mutex
	started bool
}

// StartMeshRuntime builds and starts the mesh runtime, attaching it to
// the Manager so Close also tears it down.
func (m *Manager) StartMeshRuntime(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("mesh: manager is nil")
	}
	m.mu.Lock()
	if m.meshRuntime != nil {
		m.mu.Unlock()
		return fmt.Errorf("mesh: runtime already attached")
	}
	m.mu.Unlock()
	binding, err := m.BuildMeshRuntime()
	if err != nil {
		return err
	}
	if err := binding.Start(ctx); err != nil {
		_ = binding.Close()
		return err
	}
	m.mu.Lock()
	m.meshRuntime = binding
	m.mu.Unlock()
	return nil
}

// MeshRuntime returns the attached mesh runtime binding.
func (m *Manager) MeshRuntime() *MeshRuntimeBinding {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meshRuntime
}

// BuildMeshRuntime constructs a MeshRuntimeBinding reusing the Manager's
// transports. Static peers and currently-dialable discovered peers are
// converted to dial seeds.
func (m *Manager) BuildMeshRuntime() (*MeshRuntimeBinding, error) {
	if m == nil {
		return nil, fmt.Errorf("mesh: manager is nil")
	}
	adapters := m.buildMeshInboundAdapters()
	if len(adapters) == 0 {
		return nil, fmt.Errorf("mesh: no transport adapters configured")
	}
	adapterList := make([]mesh.TransportAdapter, 0, len(adapters))
	for _, adapter := range adapters {
		adapterList = append(adapterList, adapter)
	}
	store := mesh.NewMemoryTopologyStore()
	seeds := m.collectDialSeeds()
	authenticator := newMeshEnvelopeAuthenticator(m.cfg.ClusterSecret)
	if authenticator == nil {
		return nil, fmt.Errorf("mesh: cluster secret cannot be empty")
	}
	runtime, err := mesh.NewRuntime(mesh.RuntimeOptions{
		LocalNodeID:            m.cfg.NodeID,
		Adapters:               adapterList,
		LocalPolicy:            m.cfg.MeshForwardingPolicy(),
		TopologyStore:          store,
		DialSeeds:              seeds,
		Signer:                 authenticator,
		Verifier:               authenticator,
		GenerationPersistence:  newMeshGenerationPersistence(m.store),
		EnvelopeHandler:        m.handleMeshEnvelope,
		QueryHandler:           m.handleMeshQueryEnvelope,
		ForwardedPacketHandler: m.handleMeshForwardedPacket,
		ForwardingObserver:     m.observeMeshForwarding,
		TimeSyncObserver:       m.observeMeshTimeSync,
		AdjacencyObserver:      m.observeMeshAdjacency,
	})
	if err != nil {
		return nil, err
	}
	return &MeshRuntimeBinding{runtime: runtime, store: store, adapters: adapters}, nil
}

func (m *Manager) buildMeshInboundAdapters() map[mesh.TransportKind]*meshInboundAdapter {
	out := make(map[mesh.TransportKind]*meshInboundAdapter, 3)

	wsDialer := func(ctx context.Context, endpoint string) (TransportConn, error) {
		if m.websocket == nil {
			return nil, fmt.Errorf("websocket transport unavailable")
		}
		return m.websocket.Dial(ctx, endpoint)
	}
	out[mesh.TransportWebSocket] = newMeshInboundAdapter(
		mesh.TransportWebSocket,
		&mesh.TransportCapability{
			Transport:           mesh.TransportWebSocket,
			InboundEnabled:      m.cfg.AdvertisePath != "",
			OutboundEnabled:     true,
			AdvertisedEndpoints: []string{m.cfg.AdvertisePath},
		},
		wsDialer,
	)

	if m.cfg.LibP2P.Enabled {
		libp2pCaps := m.cfg.LibP2PTransportCapability()
		libp2pDialer := func(ctx context.Context, endpoint string) (TransportConn, error) {
			if m.libp2p == nil {
				return nil, fmt.Errorf("libp2p transport unavailable")
			}
			return m.libp2p.Dial(ctx, endpoint)
		}
		out[mesh.TransportLibP2P] = newMeshInboundAdapter(mesh.TransportLibP2P, libp2pCaps, libp2pDialer)
	}

	if m.cfg.ZeroMQ.Enabled && m.cfg.ZeroMQForwardingEnabled() {
		zmqCaps := m.cfg.ZeroMQTransportCapability()
		zmqDialer := func(ctx context.Context, endpoint string) (TransportConn, error) {
			dialer := m.dialers[transportZeroMQ]
			if dialer == nil {
				return nil, fmt.Errorf("zeromq dialer unavailable")
			}
			return dialer.Dial(ctx, endpoint)
		}
		out[mesh.TransportZeroMQ] = newMeshInboundAdapter(mesh.TransportZeroMQ, zmqCaps, zmqDialer)
	}
	return out
}

// InboundAdapter returns the inbound adapter for the given transport
// kind, or nil if none.
func (b *MeshRuntimeBinding) InboundAdapter(kind mesh.TransportKind) *meshInboundAdapter {
	if b == nil {
		return nil
	}
	return b.adapters[kind]
}

// routeInboundToMesh forwards a raw inbound connection into the mesh
// runtime if it is attached and has an adapter for the transport kind.
// Returns true when the connection has been handed off; the caller must
// not use the connection afterwards.
func (m *Manager) routeInboundToMesh(kind mesh.TransportKind, conn TransportConn) bool {
	if m == nil || conn == nil {
		return false
	}
	m.mu.Lock()
	binding := m.meshRuntime
	m.mu.Unlock()
	if binding == nil {
		return false
	}
	adapter := binding.InboundAdapter(kind)
	if adapter == nil {
		return false
	}
	return adapter.InjectInbound(conn)
}

// Runtime exposes the underlying mesh.Runtime.
func (b *MeshRuntimeBinding) Runtime() *mesh.Runtime {
	if b == nil {
		return nil
	}
	return b.runtime
}

// TopologyStore exposes the store the runtime writes snapshots into.
func (b *MeshRuntimeBinding) TopologyStore() mesh.TopologyStore {
	if b == nil {
		return nil
	}
	return b.store
}

// Start starts the runtime (once).
func (b *MeshRuntimeBinding) Start(ctx context.Context) error {
	if b == nil {
		return fmt.Errorf("mesh: binding is nil")
	}
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return fmt.Errorf("mesh: runtime already started")
	}
	b.started = true
	b.mu.Unlock()
	return b.runtime.Start(ctx)
}

// Close stops the runtime.
func (b *MeshRuntimeBinding) Close() error {
	if b == nil || b.runtime == nil {
		return nil
	}
	return b.runtime.Close()
}

// AddDialSeed schedules a mesh-runtime dial for a newly discovered peer.
func (b *MeshRuntimeBinding) AddDialSeed(seed mesh.DialSeed) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.AddDialSeed(seed)
}

// RemoveDialSeed stops a previously registered mesh-runtime dial seed.
func (b *MeshRuntimeBinding) RemoveDialSeed(seed mesh.DialSeed) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.RemoveDialSeed(seed)
}

func (b *MeshRuntimeBinding) RouteEnvelope(ctx context.Context, targetNodeID int64, envelope *mesh.ClusterEnvelope) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.RouteEnvelope(ctx, targetNodeID, envelope)
}

func (b *MeshRuntimeBinding) ForwardPacket(ctx context.Context, packet *mesh.ForwardedPacket) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.ForwardPacket(ctx, packet)
}

func (m *Manager) observeMeshTimeSync(observation mesh.TimeSyncObservation) {
	if m == nil || observation.RemoteNodeID <= 0 || observation.RemoteNodeID == m.cfg.NodeID {
		return
	}
	sess := m.meshPeerSession(observation.RemoteNodeID)
	if sess == nil {
		return
	}
	rttMs := maxInt64(observation.RTTMs, 0)
	jitterMs := maxInt64(observation.JitterMs, 0)
	clientReceiveMs := observation.ClientReceiveTimeMs
	if clientReceiveMs == 0 {
		clientReceiveMs = time.Now().UTC().UnixMilli()
	}
	offsetMs := ((observation.ServerReceiveTimeMs - observation.ClientSendTimeMs) + (observation.ServerSendTimeMs - clientReceiveMs)) / 2
	sampledAt := time.Now().UTC()
	if clientReceiveMs > 0 {
		sampledAt = time.UnixMilli(clientReceiveMs).UTC()
	}
	sess.setClockOffset(offsetMs)
	sess.observeRTT(rttMs)
	state, reason := m.recordTimeSyncSample(sess, timeSyncSample{
		offsetMs:      offsetMs,
		rttMs:         rttMs,
		uncertaintyMs: maxInt64(rttMs/2, jitterMs/2) + 50,
		sampledAt:     sampledAt,
		credible:      rttMs <= m.cfg.ClockCredibleRttMs,
	})
	if state == clockStateRejected {
		m.logSessionWarn("mesh_time_sync_rejected", sess, nil).
			Str("clock_reason", reason).
			Int64("offset_ms", offsetMs).
			Int64("rtt_ms", rttMs).
			Msg("mesh time sync sample rejected peer clock")
	}
}

func (m *Manager) observeMeshAdjacency(observation mesh.AdjacencyObservation) {
	if m == nil || observation.RemoteNodeID <= 0 || observation.RemoteNodeID == m.cfg.NodeID {
		return
	}
	now := time.Now().UTC()
	connectedSnapshots := make([]discoveredPeerState, 0)
	failedSnapshots := make([]discoveredPeerState, 0)
	m.mu.Lock()
	for _, peer := range m.configuredPeers {
		if configuredPeerMatchesMeshObservation(peer, observation) {
			peer.nodeID = observation.RemoteNodeID
		}
	}
	for url, peer := range m.dynamicPeers {
		if peer == nil || !configuredPeerMatchesMeshObservation(peer, observation) {
			continue
		}
		peer.nodeID = observation.RemoteNodeID
		discovered := m.discoveredPeers[url]
		if discovered == nil {
			continue
		}
		discovered.nodeID = observation.RemoteNodeID
		discovered.lastSeenAt = now
		discovered.dialing = false
		if observation.Established {
			discovered.state = discoveryStateConnected
			discovered.lastConnectedAt = now
			discovered.lastError = ""
			connectedSnapshots = append(connectedSnapshots, *discovered)
		} else if discovered.state != discoveryStateExpired {
			discovered.state = discoveryStateFailed
			discovered.lastError = "mesh adjacency lost"
			failedSnapshots = append(failedSnapshots, *discovered)
		}
	}
	for _, discovered := range m.discoveredPeers {
		if discovered == nil || !discoveredPeerMatchesMeshObservation(discovered, observation) {
			continue
		}
		discovered.nodeID = observation.RemoteNodeID
		discovered.lastSeenAt = now
		discovered.dialing = false
		if observation.Established {
			discovered.state = discoveryStateConnected
			discovered.lastConnectedAt = now
			discovered.lastError = ""
			connectedSnapshots = append(connectedSnapshots, *discovered)
			continue
		}
		if discovered.state == discoveryStateExpired {
			continue
		}
		discovered.state = discoveryStateFailed
		discovered.lastError = "mesh adjacency lost"
		failedSnapshots = append(failedSnapshots, *discovered)
	}
	m.mu.Unlock()
	if observation.Established {
		m.meshPeerSession(observation.RemoteNodeID)
	}
	for _, snapshot := range connectedSnapshots {
		m.persistDiscoveredPeer(snapshot, true)
	}
	for _, snapshot := range failedSnapshots {
		m.persistDiscoveredPeer(snapshot, false)
	}
}

func (b *MeshRuntimeBinding) DescribeRoute(destinationNodeID int64, trafficClass mesh.TrafficClass) (mesh.RouteDecision, bool) {
	if b == nil || b.runtime == nil {
		return mesh.RouteDecision{}, false
	}
	return b.runtime.DescribeRoute(destinationNodeID, trafficClass)
}

func (m *Manager) startMeshDialSeed(peer *configuredPeer) error {
	if peer == nil {
		return nil
	}
	seed, ok := dialSeedForURL(peer.URL)
	if !ok {
		return nil
	}
	m.mu.Lock()
	binding := m.meshRuntime
	m.mu.Unlock()
	if binding == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return binding.AddDialSeed(seed)
}

func (m *Manager) stopMeshDialSeed(peer *configuredPeer) error {
	if peer == nil {
		return nil
	}
	seed, ok := dialSeedForURL(peer.URL)
	if !ok {
		return nil
	}
	m.mu.Lock()
	binding := m.meshRuntime
	m.mu.Unlock()
	if binding == nil {
		return nil
	}
	return binding.RemoveDialSeed(seed)
}

func (m *Manager) collectDialSeeds() []mesh.DialSeed {
	m.mu.Lock()
	peers := append([]*configuredPeer(nil), m.configuredPeers...)
	discovered := make([]*discoveredPeerState, 0, len(m.discoveredPeers))
	for _, peer := range m.discoveredPeers {
		if peer == nil {
			continue
		}
		if peer.nodeID <= 0 || peer.nodeID == m.cfg.NodeID || peer.state == discoveryStateExpired {
			continue
		}
		if !m.canDialDiscoveredPeer(peer) {
			continue
		}
		discovered = append(discovered, peer)
	}
	m.mu.Unlock()

	seeds := make([]mesh.DialSeed, 0, len(peers)+len(discovered))
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		if seed, ok := dialSeedForURL(peer.URL); ok {
			seeds = append(seeds, seed)
		}
	}
	for _, peer := range discovered {
		if seed, ok := dialSeedForURL(peer.url); ok {
			seeds = append(seeds, seed)
		}
	}
	return seeds
}

func configuredPeerMatchesMeshObservation(peer *configuredPeer, observation mesh.AdjacencyObservation) bool {
	if peer == nil {
		return false
	}
	if peer.nodeID > 0 && peer.nodeID == observation.RemoteNodeID {
		return true
	}
	if transportKindForPeerURL(peer.URL) != observation.Transport {
		return false
	}
	if normalized, ok := normalizedMeshObservationHint(observation.RemoteHint); ok && peer.URL == normalized {
		return true
	}
	if peer.libP2PPeerID != "" && peer.libP2PPeerID == strings.TrimSpace(observation.RemoteHint) {
		return true
	}
	return meshObservationAdvertisesURL(observation, peer.URL)
}

func discoveredPeerMatchesMeshObservation(peer *discoveredPeerState, observation mesh.AdjacencyObservation) bool {
	if peer == nil {
		return false
	}
	if peer.nodeID > 0 && peer.nodeID == observation.RemoteNodeID {
		return true
	}
	if transportKindForPeerURL(peer.url) != observation.Transport {
		return false
	}
	if normalized, ok := normalizedMeshObservationHint(observation.RemoteHint); ok && peer.url == normalized {
		return true
	}
	return meshObservationAdvertisesURL(observation, peer.url)
}

func meshObservationAdvertisesURL(observation mesh.AdjacencyObservation, peerURL string) bool {
	hello := observation.Hello
	if hello == nil {
		return false
	}
	for _, capability := range hello.Transports {
		if capability == nil || capability.Transport != observation.Transport {
			continue
		}
		for _, endpoint := range capability.AdvertisedEndpoints {
			if meshObservationEndpointMatchesPeerURL(observation.Transport, endpoint, peerURL) {
				return true
			}
		}
	}
	return false
}

func meshObservationEndpointMatchesPeerURL(transport mesh.TransportKind, endpoint, peerURL string) bool {
	if normalized, ok := normalizedMeshObservationHint(endpoint); ok && normalized == peerURL {
		return true
	}
	if transport != mesh.TransportWebSocket {
		return false
	}
	endpoint = strings.TrimSpace(endpoint)
	if !strings.HasPrefix(endpoint, "/") {
		return false
	}
	normalizedPeer, err := normalizePeerURL(peerURL)
	if err != nil || transportKindForPeerURL(normalizedPeer) != mesh.TransportWebSocket {
		return false
	}
	parsed, err := url.Parse(normalizedPeer)
	if err != nil {
		return false
	}
	return parsed.Path == endpoint
}

func normalizedMeshObservationHint(raw string) (string, bool) {
	normalized, err := normalizePeerURL(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	return normalized, true
}

func transportKindForPeerURL(peerURL string) mesh.TransportKind {
	switch transportForPeerURL(strings.TrimSpace(peerURL)) {
	case transportWebSocket:
		return mesh.TransportWebSocket
	case transportZeroMQ:
		return mesh.TransportZeroMQ
	case transportLibP2P:
		return mesh.TransportLibP2P
	default:
		return mesh.TransportUnspecified
	}
}

func dialSeedForURL(peerURL string) (mesh.DialSeed, bool) {
	trimmed := strings.TrimSpace(peerURL)
	if trimmed == "" {
		return mesh.DialSeed{}, false
	}
	switch transportForPeerURL(trimmed) {
	case transportWebSocket:
		return mesh.DialSeed{Transport: mesh.TransportWebSocket, Endpoint: trimmed}, true
	case transportZeroMQ:
		return mesh.DialSeed{Transport: mesh.TransportZeroMQ, Endpoint: trimmed}, true
	case transportLibP2P:
		return mesh.DialSeed{Transport: mesh.TransportLibP2P, Endpoint: trimmed}, true
	default:
		return mesh.DialSeed{}, false
	}
}
