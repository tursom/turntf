package cluster

import (
	"math"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
)

func TestMeshClockProcessingTimeValidation(t *testing.T) {
	for _, tc := range []struct {
		name                string
		t1, t2, t3, t4, rtt int64
		valid               bool
	}{
		{"within_round_trip", 10000, 10000, 10002, 10002, 2, true},
		{"quantization", 10000, 10000, 10003, 10002, 2, true},
		{"beyond_tolerance", 10000, 10000, 10004, 10002, 2, false},
		{"symmetric_forgery", 10000, 1, 20001, 10002, 2, false},
		{"extreme_processing", 10000, 1, math.MaxInt64, 10002, 2, false},
		{"negative_server_time", 10000, math.MinInt64, math.MaxInt64, 10002, 2, false},
		{"negative_rtt", 10000, 10001, 10001, 10002, -1, false},
		{"wall_round_trip_bound", 10000, 10000, 10004, 10002, 10, false},
		{"measured_round_trip_bound", 10000, 10000, 10004, 10010, 2, false},
		{"large_round_trip_no_overflow", 1, 1, math.MaxInt64, math.MaxInt64, math.MaxInt64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := newMeshClockTestManager(t)
			id := testNodeID(2)
			mgr.observeMeshAdjacency(mesh.AdjacencyObservation{RemoteNodeID: id, Transport: mesh.TransportWebSocket, Established: true})
			mgr.observeMeshTimeSync(mesh.TimeSyncObservation{RemoteNodeID: id, ClientSendTimeMs: 10000, ClientReceiveTimeMs: 10002, ServerReceiveTimeMs: 10001, ServerSendTimeMs: 10001, RTTMs: 2})
			mgr.mu.Lock()
			peer := mgr.peers[id]
			last, credible, trusted := peer.lastClockSync, peer.lastCredibleClockSync, mgr.lastTrustedClockSync
			count, streak, session := len(peer.clockSamples), peer.clockHealthyStreak, peer.trustedSession
			mgr.mu.Unlock()
			mgr.observeMeshTimeSync(mesh.TimeSyncObservation{RemoteNodeID: id, ClientSendTimeMs: tc.t1, ServerReceiveTimeMs: tc.t2, ServerSendTimeMs: tc.t3, ClientReceiveTimeMs: tc.t4, RTTMs: tc.rtt})
			mgr.mu.Lock()
			defer mgr.mu.Unlock()
			if tc.valid {
				if len(peer.clockSamples) != count+1 {
					t.Fatal("valid sample discarded")
				}
			} else if len(peer.clockSamples) != count || peer.lastClockSync != last || peer.lastCredibleClockSync != credible || mgr.lastTrustedClockSync != trusted || peer.clockHealthyStreak != streak || peer.trustedSession != session {
				t.Fatal("invalid sample changed clock samples or trust")
			}
		})
	}
}

func TestMeshClockOffsetDoesNotOverflow(t *testing.T) {
	mgr := newMeshClockTestManager(t)
	id := testNodeID(2)
	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{RemoteNodeID: id, Transport: mesh.TransportWebSocket, Established: true})
	mgr.observeMeshTimeSync(mesh.TimeSyncObservation{RemoteNodeID: id, ClientSendTimeMs: 1, ClientReceiveTimeMs: 3, ServerReceiveTimeMs: math.MaxInt64, ServerSendTimeMs: math.MaxInt64, RTTMs: 2})
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	peer := mgr.peers[id]
	if peer.clockOffsetMs != math.MaxInt64-2 || peer.trustedSession != nil {
		t.Fatalf("offset=%d trusted=%v", peer.clockOffsetMs, peer.trustedSession != nil)
	}
}

func TestMeshClockProtectionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                string
		offset, rtt, jitter int64
		want                clockState
		reason              string
	}{
		{"healthy", 100, 2, 0, clockStateTrusted, "trusted_sample_available"},
		{"slow", 0, 5000, 0, clockStateObserving, "slow_time_sync_sample"},
		{"near_limit", 960, 2, 0, clockStateObserving, "clock_skew_near_limit"},
		{"skew", 2000, 2, 0, clockStateRejected, "clock_skew_rejected"},
		{"jitter", 0, 2, 2200, clockStateObserving, "clock_skew_near_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := newMeshClockTestManager(t)
			peer := testNodeID(2)
			mgr.observeMeshAdjacency(mesh.AdjacencyObservation{RemoteNodeID: peer, Transport: mesh.TransportWebSocket, Established: true})
			for range 3 {
				mgr.observeMeshTimeSync(mesh.TimeSyncObservation{RemoteNodeID: peer, ClientSendTimeMs: 10000, ClientReceiveTimeMs: 10002, ServerReceiveTimeMs: 10001 + tc.offset, ServerSendTimeMs: 10001 + tc.offset, RTTMs: tc.rtt, JitterMs: tc.jitter})
			}
			state, reason := mgr.peerClockState(peer)
			if state != string(tc.want) || reason != tc.reason {
				t.Fatalf("state=%s reason=%s", state, reason)
			}
			mgr.mu.Lock()
			defer mgr.mu.Unlock()
			if got := mgr.peers[peer].clockUncertaintyMs; tc.rtt <= mgr.cfg.ClockCredibleRttMs && got != maxInt64(tc.rtt/2, tc.jitter/2)+50 {
				t.Fatalf("uncertainty=%d", got)
			}
			if tc.want != clockStateTrusted && mgr.peers[peer].trustedSession != nil {
				t.Fatal("unhealthy peer trusted for offset")
			}
		})
	}
}

func TestMeshClockObservingRecoveryRestartsAfterLastAdjacencyLoss(t *testing.T) {
	mgr := newMeshClockTestManager(t)
	id := testNodeID(2)
	adjacency := mesh.AdjacencyObservation{RemoteNodeID: id, Transport: mesh.TransportWebSocket, Established: true}
	mgr.observeMeshAdjacency(adjacency)
	mgr.observeMeshAdjacency(adjacency)
	sample := mesh.TimeSyncObservation{RemoteNodeID: id, ClientSendTimeMs: 10000, ClientReceiveTimeMs: 10002, ServerReceiveTimeMs: 10001, ServerSendTimeMs: 10001, RTTMs: 5000}
	mgr.observeMeshTimeSync(sample)
	sample.RTTMs = 2
	mgr.observeMeshTimeSync(sample)
	assertRecovery := func(wantStreak int, wantState clockState) {
		t.Helper()
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		peer := mgr.peers[id]
		if peer.clockHealthyStreak != wantStreak || peer.clockState != wantState || (peer.trustedSession != nil) != (wantState == clockStateTrusted) {
			t.Fatalf("streak=%d state=%s trusted=%v; want streak=%d state=%s", peer.clockHealthyStreak, peer.clockState, peer.trustedSession != nil, wantStreak, wantState)
		}
	}
	assertRecovery(1, clockStateObserving)
	adjacency.Established = false
	mgr.observeMeshAdjacency(adjacency)
	assertRecovery(1, clockStateObserving)
	mgr.observeMeshAdjacency(adjacency)
	assertRecovery(0, clockStateObserving)
	mgr.observeMeshTimeSync(sample)
	assertRecovery(0, clockStateObserving)
	adjacency.Established = true
	mgr.observeMeshAdjacency(adjacency)
	for i := 1; i < mgr.cfg.ClockRecoverAfterHealthySamples; i++ {
		mgr.observeMeshTimeSync(sample)
		assertRecovery(i, clockStateObserving)
	}
	mgr.observeMeshTimeSync(sample)
	assertRecovery(mgr.cfg.ClockRecoverAfterHealthySamples, clockStateTrusted)
}

func TestMeshClockLastAdjacencyLossAndRecovery(t *testing.T) {
	mgr := newMeshClockTestManager(t)
	peer := testNodeID(2)
	adjacency := mesh.AdjacencyObservation{RemoteNodeID: peer, Transport: mesh.TransportWebSocket, Established: true}
	mgr.observeMeshAdjacency(adjacency)
	mgr.observeMeshAdjacency(adjacency)
	sample := mesh.TimeSyncObservation{RemoteNodeID: peer, ClientSendTimeMs: 10000, ClientReceiveTimeMs: 10002, ServerReceiveTimeMs: 10101, ServerSendTimeMs: 10101, RTTMs: 2}
	mgr.observeMeshTimeSync(sample)
	adjacency.Established = false
	mgr.observeMeshAdjacency(adjacency)
	if state, _ := mgr.peerClockState(peer); state != "trusted" {
		t.Fatal("lost trust with another direct adjacency alive")
	}
	mgr.observeMeshAdjacency(adjacency)
	mgr.mu.Lock()
	last := mgr.peers[peer].lastClockSync
	if mgr.peers[peer].trustedSession != nil {
		t.Fatal("last adjacency loss retained trust")
	}
	mgr.mu.Unlock()
	mgr.observeMeshTimeSync(sample)
	mgr.mu.Lock()
	if mgr.peers[peer].lastClockSync != last {
		t.Fatal("late disconnected sample renewed trust")
	}
	// No callbacks must also age the ordinary write gate closed, not keep it
	// writable forever just because a synthetic session remains allocated.
	mgr.lastTrustedClockSync = time.Now().Add(-mgr.clockObserveGraceWindow() - time.Second)
	state, _ := mgr.nodeClockStateLocked()
	mgr.mu.Unlock()
	if state != clockStateDegraded {
		t.Fatalf("stale disconnected node=%s", state)
	}
	adjacency.Established = true
	mgr.observeMeshAdjacency(adjacency)
	mgr.observeMeshTimeSync(sample)
	if state, _ := mgr.peerClockState(peer); state != "observing" {
		t.Fatal("recovery must require configured healthy streak")
	}
	mgr.observeMeshTimeSync(sample)
	if state, _ := mgr.peerClockState(peer); state != "trusted" {
		t.Fatal("healthy reconnection did not recover")
	}
}
