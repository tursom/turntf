package cluster

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

// TestManagerStartAutoStartsMeshRuntime verifies that Manager.Start()
// brings up the mesh runtime and that inbound WebSocket connections are
// routed into it (Phase 4.5 acceptance criteria).
func TestManagerStartAutoStartsMeshRuntime(t *testing.T) {
	t.Parallel()

	st := newReplicationTestStore(t, "node-a", 1)
	mgrA, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}, st)
	if err != nil {
		t.Fatalf("new manager A: %v", err)
	}
	serverA := newClusterHTTPTestServer(t, mgrA.Handler())
	if err := mgrA.Start(context.Background()); err != nil {
		t.Fatalf("start manager A: %v", err)
	}
	t.Cleanup(func() { _ = mgrA.Close() })

	if mgrA.MeshRuntime() == nil {
		t.Fatalf("expected mesh runtime to be attached after Start")
	}

	stB := newReplicationTestStore(t, "node-b", 2)
	peerURL := strings.Replace(serverA.URL, "http://", "ws://", 1) + websocketPath
	mgrB, err := NewManager(Config{
		NodeID:            testNodeID(2),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers:             []Peer{{URL: peerURL}},
	}, stB)
	if err != nil {
		t.Fatalf("new manager B: %v", err)
	}
	serverB := httptest.NewServer(mgrB.Handler())
	t.Cleanup(serverB.Close)
	if err := mgrB.Start(context.Background()); err != nil {
		t.Fatalf("start manager B: %v", err)
	}
	t.Cleanup(func() { _ = mgrB.Close() })

	if mgrB.MeshRuntime() == nil {
		t.Fatalf("expected mesh runtime to be attached on manager B")
	}

	// Wait for mesh adjacency to form in either direction.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		adjsA := mgrA.MeshRuntime().Runtime().Adjacencies()
		adjsB := mgrB.MeshRuntime().Runtime().Adjacencies()
		if hasAdjacencyFor(adjsA, testNodeID(2)) && hasAdjacencyFor(adjsB, testNodeID(1)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for mesh adjacency; A=%v B=%v",
		mgrA.MeshRuntime().Runtime().Adjacencies(),
		mgrB.MeshRuntime().Runtime().Adjacencies())
}

func hasAdjacencyFor(adjs []mesh.AdjacencySnapshot, nodeID int64) bool {
	for _, adj := range adjs {
		if adj.RemoteNodeID == nodeID {
			return true
		}
	}
	return false
}

func TestManagerStartFailsWhenMeshRuntimeCannotStart(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.cfg.NodeID = 0

	if err := mgr.Start(context.Background()); err == nil {
		t.Fatalf("expected manager start to fail when mesh runtime cannot start")
	}
	if mgr.MeshRuntime() != nil {
		t.Fatalf("mesh runtime should not be attached after failed start")
	}
}

func TestManagerDiscoveredPeersUseMeshRuntimeDialSeed(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	adapter := newRecordingMeshAdapter(mesh.TransportWebSocket)
	runtime, err := mesh.NewRuntime(mesh.RuntimeOptions{
		LocalNodeID:   mgr.cfg.NodeID,
		Adapters:      []mesh.TransportAdapter{adapter},
		LocalPolicy:   mgr.cfg.MeshForwardingPolicy(),
		TopologyStore: mesh.NewMemoryTopologyStore(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	peerURL := "ws://127.0.0.1:9911/internal/cluster/ws"
	mgr.mu.Lock()
	mgr.meshRuntime = &MeshRuntimeBinding{runtime: runtime}
	mgr.discoveredPeers[peerURL] = &discoveredPeerState{
		nodeID:     testNodeID(2),
		url:        peerURL,
		state:      discoveryStateCandidate,
		lastSeenAt: time.Now().UTC(),
	}
	mgr.mu.Unlock()

	mgr.reconcileDiscoveredDialers()

	select {
	case got := <-adapter.dials:
		if got != peerURL {
			t.Fatalf("unexpected mesh dial seed endpoint: got %q want %q", got, peerURL)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected discovered peer to be dialed by mesh runtime")
	}
}

func TestManagerMeshAdjacencyBackfillsConfiguredPeerNodeID(t *testing.T) {
	t.Parallel()

	stA := newReplicationTestStore(t, "node-a", 1)
	mgrA, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}, stA)
	if err != nil {
		t.Fatalf("new manager A: %v", err)
	}
	serverA := newClusterHTTPTestServer(t, mgrA.Handler())
	if err := mgrA.Start(context.Background()); err != nil {
		t.Fatalf("start manager A: %v", err)
	}
	t.Cleanup(func() { _ = mgrA.Close() })

	peerURL := websocketURL(serverA.URL) + websocketPath
	stB := newReplicationTestStore(t, "node-b", 2)
	mgrB, err := NewManager(Config{
		NodeID:            testNodeID(2),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers:             []Peer{{URL: peerURL}},
	}, stB)
	if err != nil {
		t.Fatalf("new manager B: %v", err)
	}
	serverB := httptest.NewServer(mgrB.Handler())
	t.Cleanup(serverB.Close)
	if err := mgrB.Start(context.Background()); err != nil {
		t.Fatalf("start manager B: %v", err)
	}
	t.Cleanup(func() { _ = mgrB.Close() })

	waitForMeshRoute(t, mgrB, testNodeID(1), mesh.TrafficControlCritical)
	waitFor(t, 5*time.Second, func() bool {
		mgrB.mu.Lock()
		defer mgrB.mu.Unlock()
		return len(mgrB.configuredPeers) == 1 && mgrB.configuredPeers[0].nodeID == testNodeID(1)
	})

	status, err := mgrB.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(status.Peers) != 1 {
		t.Fatalf("expected one peer after configured node id backfill, got %+v", status.Peers)
	}
	if status.Peers[0].NodeID != testNodeID(1) || status.Peers[0].ConfiguredURL != peerURL {
		t.Fatalf("unexpected backfilled peer status: %+v", status.Peers[0])
	}
	if !containsNodeID(mgrB.ConfiguredPeerNodeIDs(), testNodeID(1)) {
		t.Fatalf("configured peer ids missing remote node: %+v", mgrB.ConfiguredPeerNodeIDs())
	}
}

func TestManagerMeshAdjacencyBackfillsConfiguredPeerNodeIDFromWebSocketPathCapability(t *testing.T) {
	t.Parallel()

	peerURL := "ws://127.0.0.1:9915" + websocketPath
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers:             []Peer{{URL: peerURL}},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		Established:  true,
		Hello: &mesh.NodeHello{
			NodeId: testNodeID(2),
			Transports: []*mesh.TransportCapability{{
				Transport:           mesh.TransportWebSocket,
				InboundEnabled:      true,
				AdvertisedEndpoints: []string{websocketPath},
			}},
		},
	})

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.configuredPeers) != 1 || mgr.configuredPeers[0].nodeID != testNodeID(2) {
		t.Fatalf("expected configured peer node id backfill from websocket path capability, got %+v", mgr.configuredPeers)
	}
}

func TestManagerObserveMeshAdjacencyUpdatesDiscoveredPeerState(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	peerURL := "ws://127.0.0.1:9912/internal/cluster/ws"
	mgr.mu.Lock()
	mgr.discoveredPeers[peerURL] = &discoveredPeerState{
		nodeID:     testNodeID(2),
		url:        peerURL,
		state:      discoveryStateDialing,
		lastSeenAt: time.Now().UTC(),
		dialing:    true,
	}
	mgr.dynamicPeers[peerURL] = &configuredPeer{
		URL:     peerURL,
		dynamic: true,
	}
	mgr.mu.Unlock()

	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		RemoteHint:   peerURL,
		Established:  true,
	})

	mgr.mu.Lock()
	discovered := mgr.discoveredPeers[peerURL]
	dynamic := mgr.dynamicPeers[peerURL]
	mgr.mu.Unlock()
	if discovered == nil || dynamic == nil {
		t.Fatalf("expected discovered and dynamic peers to remain tracked")
	}
	if discovered.state != discoveryStateConnected || discovered.dialing || discovered.lastConnectedAt.IsZero() {
		t.Fatalf("expected discovered peer to become connected, got %+v", discovered)
	}
	if dynamic.nodeID != testNodeID(2) {
		t.Fatalf("expected dynamic peer node id backfill, got %+v", dynamic)
	}

	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		RemoteHint:   peerURL,
		Established:  false,
	})

	mgr.mu.Lock()
	discovered = mgr.discoveredPeers[peerURL]
	mgr.mu.Unlock()
	if discovered == nil || discovered.state != discoveryStateFailed || discovered.lastError != "mesh adjacency lost" {
		t.Fatalf("expected discovered peer to become failed after adjacency loss, got %+v", discovered)
	}
}

func TestManagerObserveMeshAdjacencyMatchesDiscoveredWebSocketPathCapability(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	peerURL := "ws://127.0.0.1:9916" + websocketPath
	mgr.mu.Lock()
	mgr.discoveredPeers[peerURL] = &discoveredPeerState{
		url:        peerURL,
		state:      discoveryStateDialing,
		lastSeenAt: time.Now().UTC(),
		dialing:    true,
	}
	mgr.mu.Unlock()

	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		Established:  true,
		Hello: &mesh.NodeHello{
			NodeId: testNodeID(2),
			Transports: []*mesh.TransportCapability{{
				Transport:           mesh.TransportWebSocket,
				InboundEnabled:      true,
				AdvertisedEndpoints: []string{websocketPath},
			}},
		},
	})

	mgr.mu.Lock()
	discovered := mgr.discoveredPeers[peerURL]
	mgr.mu.Unlock()
	if discovered == nil {
		t.Fatalf("expected discovered peer to remain tracked")
	}
	if discovered.nodeID != testNodeID(2) || discovered.state != discoveryStateConnected || discovered.dialing {
		t.Fatalf("expected discovered peer to match websocket path capability, got %+v", discovered)
	}
}

func TestManagerReconcileDiscoveredDialersRebalancesExpiredDynamicSlots(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	adapter := newRecordingMeshAdapter(mesh.TransportWebSocket)
	runtime, err := mesh.NewRuntime(mesh.RuntimeOptions{
		LocalNodeID:           mgr.cfg.NodeID,
		Adapters:              []mesh.TransportAdapter{adapter},
		LocalPolicy:           mgr.cfg.MeshForwardingPolicy(),
		TopologyStore:         mesh.NewMemoryTopologyStore(),
		DialRetryInterval:     time.Hour,
		TopologyPublishPeriod: time.Hour,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	now := time.Now().UTC()
	mgr.mu.Lock()
	mgr.meshRuntime = &MeshRuntimeBinding{runtime: runtime}
	for i := 0; i < maxDynamicDiscoveredPeers; i++ {
		url := fmt.Sprintf("ws://127.0.0.1:%d/internal/cluster/ws", 9913+i)
		nodeID := testNodeID(uint16(i + 2))
		mgr.discoveredPeers[url] = &discoveredPeerState{
			nodeID:     nodeID,
			url:        url,
			state:      discoveryStateExpired,
			lastSeenAt: now.Add(-discoveryCandidateTTL - time.Minute),
		}
		mgr.dynamicPeers[url] = &configuredPeer{
			URL:     url,
			nodeID:  nodeID,
			dynamic: true,
		}
	}
	freshURL := "ws://127.0.0.1:9999/internal/cluster/ws"
	mgr.discoveredPeers[freshURL] = &discoveredPeerState{
		nodeID:     testNodeID(50),
		url:        freshURL,
		state:      discoveryStateCandidate,
		lastSeenAt: now,
	}
	mgr.mu.Unlock()

	mgr.reconcileDiscoveredDialers()

	select {
	case got := <-adapter.dials:
		if got != freshURL {
			t.Fatalf("unexpected fresh candidate dialed: got=%q want=%q", got, freshURL)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected fresh discovered peer to be dialed")
	}

	mgr.mu.Lock()
	_, freshDynamic := mgr.dynamicPeers[freshURL]
	staleRemaining := 0
	for i := 0; i < maxDynamicDiscoveredPeers; i++ {
		url := fmt.Sprintf("ws://127.0.0.1:%d/internal/cluster/ws", 9913+i)
		if _, ok := mgr.dynamicPeers[url]; ok {
			staleRemaining++
		}
	}
	mgr.mu.Unlock()
	if !freshDynamic {
		t.Fatalf("expected fresh discovered peer to occupy a dynamic slot")
	}
	if staleRemaining != 0 {
		t.Fatalf("expected expired dynamic slots to be released, stale remaining=%d", staleRemaining)
	}
}

func containsNodeID(values []int64, want int64) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type recordingMeshAdapter struct {
	kind   mesh.TransportKind
	accept chan mesh.TransportConn
	dials  chan string
}

func newRecordingMeshAdapter(kind mesh.TransportKind) *recordingMeshAdapter {
	return &recordingMeshAdapter{
		kind:   kind,
		accept: make(chan mesh.TransportConn),
		dials:  make(chan string, 4),
	}
}

func (a *recordingMeshAdapter) Start(context.Context) error { return nil }

func (a *recordingMeshAdapter) Dial(ctx context.Context, endpoint string) (mesh.TransportConn, error) {
	select {
	case a.dials <- endpoint:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, context.Canceled
}

func (a *recordingMeshAdapter) Accept() <-chan mesh.TransportConn { return a.accept }

func (a *recordingMeshAdapter) Kind() mesh.TransportKind { return a.kind }

func (a *recordingMeshAdapter) LocalCapabilities() *mesh.TransportCapability {
	return &mesh.TransportCapability{
		Transport:       a.kind,
		InboundEnabled:  true,
		OutboundEnabled: true,
	}
}
