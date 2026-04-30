package cluster

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerApplyOnlinePresenceSnapshotAdvancesRuntimeEpochAndClearsOldEphemeralState(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:                     testNodeID(1),
		AdvertisePath:              websocketPath,
		ClusterSecret:              "secret",
		MessageWindowSize:          store.DefaultMessageWindowSize,
		MaxClockSkewMs:             DefaultMaxClockSkewMs,
		DisconnectSuspicionGraceMs: 25,
		DiscoveryDisabled:          true,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	oldUser := store.UserKey{NodeID: testNodeID(2), UserID: 10}
	newUser := store.UserKey{NodeID: testNodeID(2), UserID: 20}

	mgr.mu.Lock()
	mgr.onlinePresenceByUser[oldUser] = map[int64]store.OnlineNodePresence{
		testNodeID(2): {
			User:          oldUser,
			ServingNodeID: testNodeID(2),
			SessionCount:  1,
			TransportHint: "ws",
		},
	}
	mgr.onlinePresenceEpochs[testNodeID(2)] = 100
	mgr.onlinePresenceOrigins[testNodeID(2)] = 3
	mgr.remoteRuntimeEpochs[testNodeID(2)] = 100
	mgr.loggedInUsersByNode[testNodeID(2)] = []app.LoggedInUserSummary{{NodeID: testNodeID(2), UserID: 10, Username: "old"}}
	mgr.disconnectSuspicions[disconnectSuspicionKey{targetNodeID: testNodeID(2), runtimeEpoch: 100}] = disconnectSuspicionState{
		deadline: time.Now().Add(time.Minute),
	}
	mgr.mu.Unlock()

	applied := mgr.applyOnlinePresenceSnapshot(&internalproto.OnlinePresenceSnapshot{
		OriginNodeId: testNodeID(2),
		Generation:   1,
		RuntimeEpoch: 200,
		Items: []*internalproto.ClusterOnlineNodePresence{{
			User:          &internalproto.ClusterUserRef{NodeId: newUser.NodeID, UserId: newUser.UserID},
			ServingNodeId: testNodeID(2),
			SessionCount:  2,
			TransportHint: "mixed",
		}},
		LoggedInUsers: []*internalproto.ClusterLoggedInUser{{
			NodeId:   newUser.NodeID,
			UserId:   newUser.UserID,
			Username: "new-user",
		}},
	})
	if !applied {
		t.Fatalf("expected snapshot to apply")
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if _, ok := mgr.onlinePresenceByUser[oldUser]; ok {
		t.Fatalf("expected old presence to be cleared after runtime epoch advance")
	}
	bucket := mgr.onlinePresenceByUser[newUser]
	if got := bucket[testNodeID(2)].SessionCount; got != 2 {
		t.Fatalf("unexpected new presence session count: got %d want 2", got)
	}
	users, ok := mgr.loggedInUsersByNode[testNodeID(2)]
	if !ok || len(users) != 1 || users[0].UserID != newUser.UserID || users[0].Username != "new-user" {
		t.Fatalf("expected logged-in users mirror to be replaced after runtime epoch advance, got %+v", users)
	}
	if got := mgr.onlinePresenceEpochs[testNodeID(2)]; got != 200 {
		t.Fatalf("unexpected applied runtime epoch: got %d want 200", got)
	}
	if _, ok := mgr.disconnectSuspicions[disconnectSuspicionKey{targetNodeID: testNodeID(2), runtimeEpoch: 100}]; ok {
		t.Fatalf("expected old suspicion to be cleared after authoritative snapshot")
	}
}

func TestManagerObserveMeshAdjacencyLossCreatesDisconnectSuspicion(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:                     testNodeID(1),
		AdvertisePath:              websocketPath,
		ClusterSecret:              "secret",
		MessageWindowSize:          store.DefaultMessageWindowSize,
		MaxClockSkewMs:             DefaultMaxClockSkewMs,
		DisconnectSuspicionGraceMs: 25,
		DiscoveryDisabled:          true,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		Established:  true,
		Hello: &mesh.NodeHello{
			NodeId:           testNodeID(2),
			RuntimeEpoch:     77,
			ProtocolVersion:  mesh.ProtocolVersion,
			ForwardingPolicy: mesh.DefaultForwardingPolicy(1),
			Transports: []*mesh.TransportCapability{{
				Transport:           mesh.TransportWebSocket,
				InboundEnabled:      true,
				OutboundEnabled:     true,
				AdvertisedEndpoints: []string{websocketPath},
			}},
		},
	})
	mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
		RemoteNodeID: testNodeID(2),
		Transport:    mesh.TransportWebSocket,
		Established:  false,
		Hello: &mesh.NodeHello{
			NodeId:           testNodeID(2),
			RuntimeEpoch:     77,
			ProtocolVersion:  mesh.ProtocolVersion,
			ForwardingPolicy: mesh.DefaultForwardingPolicy(1),
			Transports: []*mesh.TransportCapability{{
				Transport:           mesh.TransportWebSocket,
				InboundEnabled:      true,
				OutboundEnabled:     true,
				AdvertisedEndpoints: []string{websocketPath},
			}},
		},
	})

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if got := mgr.remoteRuntimeEpochs[testNodeID(2)]; got != 77 {
		t.Fatalf("unexpected remembered remote runtime epoch: got %d want 77", got)
	}
	if got := mgr.directAdjacencyCounts[testNodeID(2)]; got != 0 {
		t.Fatalf("expected direct adjacency count to drop to zero, got %d", got)
	}
	if _, ok := mgr.disconnectSuspicions[disconnectSuspicionKey{targetNodeID: testNodeID(2), runtimeEpoch: 77}]; !ok {
		t.Fatalf("expected disconnect suspicion to be recorded after all direct adjacencies were lost")
	}
}

func TestManagerDisconnectSuspicionExpiryClearsEphemeralState(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:                     testNodeID(1),
		AdvertisePath:              websocketPath,
		ClusterSecret:              "secret",
		MessageWindowSize:          store.DefaultMessageWindowSize,
		MaxClockSkewMs:             DefaultMaxClockSkewMs,
		DisconnectSuspicionGraceMs: 25,
		DiscoveryDisabled:          true,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	user := store.UserKey{NodeID: testNodeID(2), UserID: 10}
	mgr.mu.Lock()
	mgr.remoteRuntimeEpochs[testNodeID(2)] = 77
	mgr.onlinePresenceEpochs[testNodeID(2)] = 77
	mgr.onlinePresenceByUser[user] = map[int64]store.OnlineNodePresence{
		testNodeID(2): {
			User:          user,
			ServingNodeID: testNodeID(2),
			SessionCount:  1,
			TransportHint: "ws",
		},
	}
	mgr.loggedInUsersByNode[testNodeID(2)] = []app.LoggedInUserSummary{{NodeID: testNodeID(2), UserID: 10, Username: "stale"}}
	mgr.disconnectSuspicions[disconnectSuspicionKey{targetNodeID: testNodeID(2), runtimeEpoch: 77}] = disconnectSuspicionState{
		deadline: time.Now().Add(-time.Millisecond),
		reason:   "all_direct_adjacencies_lost",
	}
	mgr.mu.Unlock()

	mgr.expireDisconnectSuspicions(time.Now().UTC())

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if _, ok := mgr.onlinePresenceByUser[user]; ok {
		t.Fatalf("expected remote presence to be cleared after suspicion expiry")
	}
	if _, ok := mgr.loggedInUsersByNode[testNodeID(2)]; ok {
		t.Fatalf("expected logged-in users mirror to be cleared after suspicion expiry")
	}
	if _, ok := mgr.disconnectSuspicions[disconnectSuspicionKey{targetNodeID: testNodeID(2), runtimeEpoch: 77}]; ok {
		t.Fatalf("expected expired suspicion to be removed")
	}
}

func TestManagerConnectivityRumorDoesNotCreateSuspicionWhenDirectAdjacencyExists(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:                     testNodeID(1),
		AdvertisePath:              websocketPath,
		ClusterSecret:              "secret",
		MessageWindowSize:          store.DefaultMessageWindowSize,
		MaxClockSkewMs:             DefaultMaxClockSkewMs,
		DisconnectSuspicionGraceMs: 25,
		DiscoveryDisabled:          true,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	mgr.mu.Lock()
	mgr.directAdjacencyCounts[testNodeID(2)] = 1
	mgr.remoteRuntimeEpochs[testNodeID(2)] = 77
	mgr.mu.Unlock()

	err = mgr.handleMeshConnectivityRumorEnvelope(&mesh.ForwardedPacket{
		SourceNodeId: testNodeID(3),
	}, &mesh.MeshConnectivityRumor{
		ConnectivityRumor: &internalproto.NodeConnectivityRumor{
			TargetNodeId:         testNodeID(2),
			TargetRuntimeEpoch:   77,
			ReporterNodeId:       testNodeID(4),
			ReporterRuntimeEpoch: 88,
			ObservedAtMs:         time.Now().UnixMilli(),
			Reason:               "all_direct_adjacencies_lost",
		},
	})
	if err != nil {
		t.Fatalf("handle connectivity rumor: %v", err)
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.disconnectSuspicions) != 0 {
		t.Fatalf("expected rumor to be ignored locally while direct adjacency still exists, got %+v", mgr.disconnectSuspicions)
	}
}

func TestLogMeshForwardFailureUsesDebugForNoRoute(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	logOutput := captureClusterLogs(t)
	sess := &session{
		manager:      mgr,
		conn:         testLegacyTransportConn{},
		peerID:       testNodeID(2),
		connectionID: 7,
	}

	mgr.logMeshForwardFailure("mesh_connectivity_rumor_forward_failed", sess, mesh.ErrNoRoute, "failed to forward connectivity rumor over mesh")
	output := logOutput.String()
	if !strings.Contains(output, `"level":"debug"`) {
		t.Fatalf("expected debug log for no-route forward failure, got %q", output)
	}
	if strings.Contains(output, `"level":"warn"`) {
		t.Fatalf("did not expect warn log for no-route forward failure, got %q", output)
	}
	if !strings.Contains(output, `"event":"mesh_connectivity_rumor_forward_failed"`) {
		t.Fatalf("expected event name in log, got %q", output)
	}
}

func TestLogMeshForwardFailureUsesWarnForUnexpectedError(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	logOutput := captureClusterLogs(t)
	sess := &session{
		manager:      mgr,
		conn:         testLegacyTransportConn{},
		peerID:       testNodeID(2),
		connectionID: 8,
	}

	mgr.logMeshForwardFailure("mesh_connectivity_rumor_forward_failed", sess, errors.New("boom"), "failed to forward connectivity rumor over mesh")
	output := logOutput.String()
	if !strings.Contains(output, `"level":"warn"`) {
		t.Fatalf("expected warn log for non-route forward failure, got %q", output)
	}
	if !strings.Contains(output, `"event":"mesh_connectivity_rumor_forward_failed"`) {
		t.Fatalf("expected event name in log, got %q", output)
	}
}
