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

func TestMeshTimeSyncDoesNotTrustPeerClock(t *testing.T) {
	mgr := newMeshClockTestManager(t)
	peerID := testNodeID(2)
	sess := mgr.meshPeerSession(peerID)

	state, reason := mgr.peerClockState(peerID)
	if state == string(clockStateTrusted) {
		t.Fatalf("mesh peer should not start trusted without a time-sync sample: reason=%s", reason)
	}
	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.WriteGateReady {
		t.Fatalf("expected write gate to stay closed before mesh time-sync observation")
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
	if state == string(clockStateTrusted) {
		t.Fatalf("mesh time-sync observation should not trust peer clock: reason=%s", reason)
	}
	status, err = mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status after observation: %v", err)
	}
	if status.WriteGateReady {
		t.Fatalf("expected write gate to stay closed after mesh time-sync observation")
	}
	sess.mu.Lock()
	smoothedRTTMs := sess.smoothedRTTMs
	sess.mu.Unlock()
	if smoothedRTTMs <= 0 {
		t.Fatalf("expected mesh time-sync observation to update RTT, got %d", smoothedRTTMs)
	}
}

func TestManagerMeshRuntimeTimeSyncDoesNotTrustPeerClock(t *testing.T) {
	mgrA, mgrB := startMeshManagerPair(t, true)
	waitForMeshRoute(t, mgrA, testNodeID(2), mesh.TrafficControlCritical)
	waitForMeshRoute(t, mgrB, testNodeID(1), mesh.TrafficControlCritical)

	mgrA.observeMeshTimeSync(mesh.TimeSyncObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		RTTMs:        2,
	})
	mgrB.observeMeshTimeSync(mesh.TimeSyncObservation{
		RemoteNodeID: testNodeID(1),
		Transport:    mesh.TransportWebSocket,
		RTTMs:        2,
	})
	if meshSessionRTT(mgrA, testNodeID(2)) <= 0 || meshSessionRTT(mgrB, testNodeID(1)) <= 0 {
		t.Fatal("expected mesh time-sync observation to update synthetic session RTT")
	}

	stateA, reasonA := mgrA.peerClockState(testNodeID(2))
	stateB, reasonB := mgrB.peerClockState(testNodeID(1))
	if stateA == string(clockStateTrusted) || stateB == string(clockStateTrusted) {
		t.Fatalf("mesh runtime time-sync should not trust peer clocks: stateA=%s reasonA=%s stateB=%s reasonB=%s", stateA, reasonA, stateB, reasonB)
	}
	statusA, err := mgrA.Status(context.Background())
	if err != nil {
		t.Fatalf("manager A status: %v", err)
	}
	statusB, err := mgrB.Status(context.Background())
	if err != nil {
		t.Fatalf("manager B status: %v", err)
	}
	if statusA.WriteGateReady || statusB.WriteGateReady {
		t.Fatalf("mesh runtime time-sync should not open write gates: A=%v B=%v", statusA.WriteGateReady, statusB.WriteGateReady)
	}
}

func TestMeshSyntheticSessionBypassesLegacyClockGate(t *testing.T) {
	mgr := newMeshClockTestManager(t)
	peerID := testNodeID(2)
	sess := mgr.meshPeerSession(peerID)

	mgr.mu.Lock()
	mgr.lastTrustedClockSync = time.Now().UTC().Add(-2 * mgr.clockWriteGateGraceWindow())
	mgr.refreshNodeClockStateLocked()
	legacyEventErr := mgr.allowEventApplyLocked(peerID)
	legacySnapshotErr := mgr.allowSnapshotTrafficLocked(peerID)
	eventErr := mgr.allowEventApplyForSessionLocked(sess)
	snapshotErr := mgr.allowSnapshotTrafficForSessionLocked(sess)
	mgr.mu.Unlock()

	if legacyEventErr == nil {
		t.Fatal("expected legacy event clock gate to reject unwritable node state")
	}
	if legacySnapshotErr == nil {
		t.Fatal("expected legacy snapshot clock gate to reject unwritable node state")
	}
	if eventErr != nil {
		t.Fatalf("expected mesh synthetic event apply to bypass legacy clock gate: %v", eventErr)
	}
	if snapshotErr != nil {
		t.Fatalf("expected mesh synthetic snapshot traffic to bypass legacy clock gate: %v", snapshotErr)
	}
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

func meshSessionRTT(mgr *Manager, peerID int64) int64 {
	mgr.mu.Lock()
	peer := mgr.peers[peerID]
	var sess *session
	if peer != nil {
		sess = peer.active
	}
	mgr.mu.Unlock()
	if sess == nil {
		return 0
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.smoothedRTTMs
}
