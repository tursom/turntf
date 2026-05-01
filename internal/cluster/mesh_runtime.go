package cluster

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
)

// MeshRuntimeBinding 拥有一个mesh.Runtime并复用Manager的传输层。
// 入站连接通过适配器的InjectInbound方法推送；
// 出站拨号复用Manager的拨号器。
type MeshRuntimeBinding struct {
	runtime  *mesh.Runtime
	store    mesh.TopologyStore
	adapters map[mesh.TransportKind]*meshInboundAdapter

	mu      sync.Mutex
	started bool
}

// StartMeshRuntime 构建并启动网格运行时，将其附加到Manager以便Close时一并清理。
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

// MeshRuntime 返回已附加的网格运行时绑定。
func (m *Manager) MeshRuntime() *MeshRuntimeBinding {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meshRuntime
}

// BuildMeshRuntime 构建MeshRuntimeBinding，复用Manager的传输层。
// 将静态对等节点和当前可拨号的发现节点转换为拨号种子。
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
		LocalRuntimeEpoch:      m.localRuntimeEpoch,
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

// buildMeshInboundAdapters 为每种启用的传输类型构建入站适配器。
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

// InboundAdapter 返回指定传输类型的入站适配器。
func (b *MeshRuntimeBinding) InboundAdapter(kind mesh.TransportKind) *meshInboundAdapter {
	if b == nil {
		return nil
	}
	return b.adapters[kind]
}

// routeInboundToMesh 将原始入站连接转发到网格运行时（如果有适配器）。
// 返回true表示连接已被接管，调用者不应再使用该连接。
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

// Runtime 暴露底层的mesh.Runtime。
func (b *MeshRuntimeBinding) Runtime() *mesh.Runtime {
	if b == nil {
		return nil
	}
	return b.runtime
}

// TopologyStore 暴露运行时写入快照的拓扑存储。
func (b *MeshRuntimeBinding) TopologyStore() mesh.TopologyStore {
	if b == nil {
		return nil
	}
	return b.store
}

// Start 启动运行时（仅一次）。
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

// Close 停止运行时。
func (b *MeshRuntimeBinding) Close() error {
	if b == nil || b.runtime == nil {
		return nil
	}
	return b.runtime.Close()
}

// AddDialSeed 为新发现的节点添加一个网格运行时拨号种子。
func (b *MeshRuntimeBinding) AddDialSeed(seed mesh.DialSeed) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.AddDialSeed(seed)
}

// RemoveDialSeed 移除一个之前注册的网格运行时拨号种子。
func (b *MeshRuntimeBinding) RemoveDialSeed(seed mesh.DialSeed) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.RemoveDialSeed(seed)
}

// RouteEnvelope 通过网格路由一个ClusterEnvelope。
func (b *MeshRuntimeBinding) RouteEnvelope(ctx context.Context, targetNodeID int64, envelope *mesh.ClusterEnvelope) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.RouteEnvelope(ctx, targetNodeID, envelope)
}

// ForwardPacket 通过网格转发一个数据包。
func (b *MeshRuntimeBinding) ForwardPacket(ctx context.Context, packet *mesh.ForwardedPacket) error {
	if b == nil || b.runtime == nil {
		return fmt.Errorf("mesh: runtime is not attached")
	}
	return b.runtime.ForwardPacket(ctx, packet)
}

// observeMeshTimeSync 处理网格时间同步观测结果，仅更新RTT信号。
func (m *Manager) observeMeshTimeSync(observation mesh.TimeSyncObservation) {
	if m == nil || observation.RemoteNodeID <= 0 || observation.RemoteNodeID == m.cfg.NodeID {
		return
	}
	sess := m.meshPeerSession(observation.RemoteNodeID)
	if sess == nil {
		return
	}
	rttMs := maxInt64(observation.RTTMs, 0)
	sess.observeRTT(rttMs)
}

// observeMeshAdjacency 处理网格邻接观测（连接建立或断开）。
// 更新发现状态、动态节点信息、邻接计数，并在需要时广播在线状态和连接性传闻。
func (m *Manager) observeMeshAdjacency(observation mesh.AdjacencyObservation) {
	if m == nil || observation.RemoteNodeID <= 0 || observation.RemoteNodeID == m.cfg.NodeID {
		return
	}
	now := time.Now().UTC()
	connectedSnapshots := make([]discoveredPeerState, 0)
	failedSnapshots := make([]discoveredPeerState, 0)
	var rumor *internalproto.NodeConnectivityRumor
	shouldBroadcastPresence := false
	m.mu.Lock()
	if observation.Hello != nil {
		m.rememberRemoteRuntimeEpochLocked(observation.RemoteNodeID, observation.Hello.GetRuntimeEpoch())
	}
	// 更新匹配的配置节点和动态节点
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
	// 更新直接邻接计数
	prevDirectAdjacencyCount := m.directAdjacencyCounts[observation.RemoteNodeID]
	if observation.Established {
		m.directAdjacencyCounts[observation.RemoteNodeID] = prevDirectAdjacencyCount + 1
		shouldBroadcastPresence = true
	} else if prevDirectAdjacencyCount > 0 {
		nextDirectAdjacencyCount := prevDirectAdjacencyCount - 1
		if nextDirectAdjacencyCount == 0 {
			delete(m.directAdjacencyCounts, observation.RemoteNodeID)
			targetRuntimeEpoch := m.currentRuntimeEpochForNodeLocked(observation.RemoteNodeID)
			if targetRuntimeEpoch > 0 {
				rumor = &internalproto.NodeConnectivityRumor{
					TargetNodeId:         observation.RemoteNodeID,
					TargetRuntimeEpoch:   targetRuntimeEpoch,
					ReporterNodeId:       m.cfg.NodeID,
					ReporterRuntimeEpoch: m.localRuntimeEpoch,
					ObservedAtMs:         now.UnixMilli(),
					Reason:               "all_direct_adjacencies_lost",
				}
				m.markConnectivityRumorSeenLocked(rumor, now)
				m.noteDisconnectSuspicionLocked(rumor, now)
			}
		} else {
			m.directAdjacencyCounts[observation.RemoteNodeID] = nextDirectAdjacencyCount
		}
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
	if shouldBroadcastPresence {
		m.broadcastOnlinePresence()
	}
	if rumor != nil {
		m.broadcastConnectivityRumor(rumor)
	}
}

// DescribeRoute 返回到目标节点的路由决策。
func (b *MeshRuntimeBinding) DescribeRoute(destinationNodeID int64, trafficClass mesh.TrafficClass) (mesh.RouteDecision, bool) {
	if b == nil || b.runtime == nil {
		return mesh.RouteDecision{}, false
	}
	return b.runtime.DescribeRoute(destinationNodeID, trafficClass)
}

// startMeshDialSeed 为动态发现的节点启动网格拨号。
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

// stopMeshDialSeed 停止网格拨号。
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

// collectDialSeeds 从配置节点和可拨号的发现节点中收集拨号种子。
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

// configuredPeerMatchesMeshObservation 检查配置节点是否匹配邻接观测。
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

// discoveredPeerMatchesMeshObservation 检查发现节点是否匹配邻接观测。
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

// meshObservationAdvertisesURL 检查观测中的Hello是否通告了给定的对等URL。
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

// meshObservationEndpointMatchesPeerURL 检查端点是否匹配对等URL。
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

// normalizedMeshObservationHint 规范化网格观测的远程提示。
func normalizedMeshObservationHint(raw string) (string, bool) {
	normalized, err := normalizePeerURL(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	return normalized, true
}

// transportKindForPeerURL 将对等URL映射到网格传输类型。
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

// dialSeedForURL 从对等URL构建网格拨号种子。
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
