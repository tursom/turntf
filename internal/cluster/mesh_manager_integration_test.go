package cluster

import (
	"context"
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
