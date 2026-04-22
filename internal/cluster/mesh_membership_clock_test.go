package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerMeshMembershipUpdatePropagatesDiscoveredPeer(t *testing.T) {
	mgrA, mgrB := startMeshManagerPair(t, false)
	waitForMeshRoute(t, mgrB, testNodeID(1), mesh.TrafficControlCritical)
	mgrB.ensureMeshPeerSessions()

	discoveredURL := "ws://127.0.0.1:9913/internal/cluster/ws"
	mgrB.mu.Lock()
	mgrB.discoveredPeers[discoveredURL] = &discoveredPeerState{
		nodeID:           testNodeID(3),
		url:              discoveredURL,
		sourcePeerNodeID: testNodeID(2),
		state:            discoveryStateConnected,
		firstSeenAt:      time.Now().UTC(),
		lastSeenAt:       time.Now().UTC(),
		lastConnectedAt:  time.Now().UTC(),
		generation:       7,
	}
	mgrB.mu.Unlock()

	mgrB.broadcastMembershipUpdate()

	waitFor(t, 5*time.Second, func() bool {
		mgrA.mu.Lock()
		defer mgrA.mu.Unlock()
		peer := mgrA.discoveredPeers[discoveredURL]
		return peer != nil &&
			peer.nodeID == testNodeID(3) &&
			peer.sourcePeerNodeID == testNodeID(2) &&
			peer.generation == 7
	})
}

func TestManagerMeshMembershipBroadcastDoesNotUseSyntheticSessionQueue(t *testing.T) {
	_, mgrB := startMeshManagerPair(t, false)
	waitForMeshRoute(t, mgrB, testNodeID(1), mesh.TrafficControlCritical)
	sess := mgrB.meshPeerSession(testNodeID(1))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < outboundQueueSize+32; i++ {
			mgrB.sendMembershipUpdate(sess)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("mesh membership broadcast blocked on synthetic session queue")
	}
	if got := len(sess.send); got != 0 {
		t.Fatalf("expected synthetic session send queue to stay unused, got %d item(s)", got)
	}
}

func TestMeshPeerSessionRequiresTimeSyncBeforeTrust(t *testing.T) {
	mgr := newMeshClockTestManager(t)
	peerID := testNodeID(2)
	mgr.meshPeerSession(peerID)

	state, reason := mgr.peerClockState(peerID)
	if state == string(clockStateTrusted) {
		t.Fatalf("mesh peer should not start trusted without a time-sync sample: reason=%s", reason)
	}
	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.WriteGateReady {
		t.Fatalf("expected write gate to stay closed before mesh time-sync sample")
	}

	mgr.observeMeshTimeSync(mesh.TimeSyncObservation{
		RemoteNodeID:        peerID,
		Transport:           mesh.TransportWebSocket,
		ClientSendTimeMs:    1_000,
		ServerReceiveTimeMs: 1_001,
		ServerSendTimeMs:    1_001,
		ClientReceiveTimeMs: 1_002,
		RTTMs:               2,
	})
	state, reason = mgr.peerClockState(peerID)
	if state != string(clockStateTrusted) {
		t.Fatalf("expected mesh time-sync sample to trust peer clock, got state=%s reason=%s", state, reason)
	}
	status, err = mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status after sample: %v", err)
	}
	if !status.WriteGateReady {
		t.Fatalf("expected write gate to open after credible mesh time-sync sample")
	}

	mgr.mu.Lock()
	stale := time.Now().UTC().Add(-2 * mgr.clockTrustedFreshWindow())
	peer := mgr.peers[peerID]
	peer.lastClockSync = stale
	peer.lastCredibleClockSync = stale
	mgr.mu.Unlock()
	state, reason = mgr.peerClockState(peerID)
	if state == string(clockStateTrusted) {
		t.Fatalf("expected stale mesh time-sync sample to leave trusted state: reason=%s", reason)
	}
}

func TestManagerMeshRuntimeTimeSyncMarksPeerTrusted(t *testing.T) {
	mgrA, mgrB := startMeshManagerPair(t, true)
	waitForMeshRoute(t, mgrA, testNodeID(2), mesh.TrafficControlCritical)
	waitForMeshRoute(t, mgrB, testNodeID(1), mesh.TrafficControlCritical)

	waitFor(t, 5*time.Second, func() bool {
		stateA, _ := mgrA.peerClockState(testNodeID(2))
		stateB, _ := mgrB.peerClockState(testNodeID(1))
		return stateA == string(clockStateTrusted) && stateB == string(clockStateTrusted)
	})
}

func startMeshManagerPair(t *testing.T, discoveryDisabled bool) (*Manager, *Manager) {
	t.Helper()

	stA := newReplicationTestStore(t, "mesh-pair-a", 1)
	mgrA, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: discoveryDisabled,
	}, stA)
	if err != nil {
		t.Fatalf("new manager A: %v", err)
	}
	serverA := newClusterHTTPTestServer(t, mgrA.Handler())
	if err := mgrA.Start(context.Background()); err != nil {
		t.Fatalf("start manager A: %v", err)
	}
	t.Cleanup(func() { _ = mgrA.Close() })

	stB := newReplicationTestStore(t, "mesh-pair-b", 2)
	mgrB, err := NewManager(Config{
		NodeID:            testNodeID(2),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: discoveryDisabled,
		Peers: []Peer{
			{URL: websocketURL(serverA.URL) + websocketPath},
		},
	}, stB)
	if err != nil {
		t.Fatalf("new manager B: %v", err)
	}
	serverB := newClusterHTTPTestServer(t, mgrB.Handler())
	_ = serverB
	if err := mgrB.Start(context.Background()); err != nil {
		t.Fatalf("start manager B: %v", err)
	}
	t.Cleanup(func() { _ = mgrB.Close() })

	return mgrA, mgrB
}

func newMeshClockTestManager(t *testing.T) *Manager {
	t.Helper()
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
	return mgr
}
