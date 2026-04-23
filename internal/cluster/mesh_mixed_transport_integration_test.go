package cluster

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerQueryLoggedInUsersUsesWebSocketLibP2PBridge(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startWebSocketLibP2PBridgeManagers(t)
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(3),
			UserID:   4096,
			Username: "mixed-transport-user",
		}}, nil
	})

	waitForMeshRouteDecision(t, mgrA, testNodeID(3), mesh.TrafficControlQuery, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(t, mgrC, testNodeID(1), mesh.TrafficControlQuery, testNodeID(2), mesh.TransportLibP2P)

	users, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("query logged-in users over websocket/libp2p bridge: %v", err)
	}
	if len(users) != 1 || users[0].NodeID != testNodeID(3) || users[0].UserID != 4096 || users[0].Username != "mixed-transport-user" {
		t.Fatalf("unexpected users: %+v", users)
	}
}

func TestManagerRoutesTransientPacketUsesWebSocketLibP2PBridge(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startWebSocketLibP2PBridgeManagers(t)
	delivered := make(chan store.TransientPacket, 1)
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRouteDecision(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)

	packet := store.TransientPacket{
		PacketID:     901,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    store.UserKey{NodeID: testNodeID(3), UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:         []byte("mixed-transport-transient"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      defaultPacketTTLHops,
	}
	if err := mgrA.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route transient packet over websocket/libp2p bridge: %v", err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.SourceNodeID != packet.SourceNodeID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "mixed-transport-transient" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for mixed-transport transient delivery")
	}
}

func TestManagerWebSocketLibP2PBridgeRejectsReplicationAndSnapshot(t *testing.T) {
	t.Parallel()

	mgrA, _, _ := startWebSocketLibP2PBridgeManagers(t)

	waitForMeshRouteDecision(t, mgrA, testNodeID(3), mesh.TrafficControlCritical, testNodeID(2), mesh.TransportWebSocket)

	err := mgrA.routeMeshReplicationBatch(context.Background(), testNodeID(3), 1, "2026-01-01T00:00:00.000000000Z/1/1", &internalproto.EventBatch{
		OriginNodeId: testNodeID(1),
	})
	if !errors.Is(err, mesh.ErrNoRoute) {
		t.Fatalf("expected replication no route across websocket/libp2p bridge, got %v", err)
	}

	err = mgrA.routeMeshSnapshotManifest(context.Background(), testNodeID(3), &internalproto.SnapshotDigest{
		SnapshotVersion: internalproto.SnapshotVersion,
	})
	if !errors.Is(err, mesh.ErrNoRoute) {
		t.Fatalf("expected snapshot no route across websocket/libp2p bridge, got %v", err)
	}

	metrics := mgrA.meshMetricsSnapshot()
	if !hasMeshNoPathMetric(metrics, "replication_stream") {
		t.Fatalf("expected replication no-path metric, got %+v", metrics.RoutingNoPath)
	}
	if !hasMeshNoPathMetric(metrics, "snapshot_bulk") {
		t.Fatalf("expected snapshot no-path metric, got %+v", metrics.RoutingNoPath)
	}
}

func startWebSocketLibP2PBridgeManagers(t *testing.T) (*Manager, *Manager, *Manager) {
	t.Helper()

	stC := newReplicationTestStore(t, "node-c", 3)
	cfgC := mixedTransportBaseConfig(3)
	cfgC.LibP2P = mixedTransportLibP2PConfig(t, "node-c")
	mgrC, _ := startMixedTransportManager(t, cfgC, stC)
	cLibP2PURL := firstLibP2PListenAddr(t, mgrC)

	stA := newReplicationTestStore(t, "node-a", 1)
	cfgA := mixedTransportBaseConfig(1)
	mgrA, serverAURL := startMixedTransportManager(t, cfgA, stA)

	stB := newReplicationTestStore(t, "node-b", 2)
	cfgB := mixedTransportBaseConfig(2)
	cfgB.LibP2P = mixedTransportLibP2PConfig(t, "node-b")
	cfgB.Peers = []Peer{
		{URL: websocketURL(serverAURL) + websocketPath},
		{URL: cLibP2PURL},
	}
	mgrB, _ := startMixedTransportManager(t, cfgB, stB)

	return mgrA, mgrB, mgrC
}

func mixedTransportBaseConfig(nodeSlot uint16) Config {
	return Config{
		NodeID:            testNodeID(nodeSlot),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}
}

func mixedTransportLibP2PConfig(t *testing.T, name string) LibP2PConfig {
	t.Helper()
	return LibP2PConfig{
		Enabled:            true,
		PrivateKeyPath:     filepath.Join(t.TempDir(), name+".key"),
		ListenAddrs:        []string{"/ip4/127.0.0.1/tcp/0"},
		EnableHolePunching: true,
	}
}

func startMixedTransportManager(t *testing.T, cfg Config, st *store.Store) (*Manager, string) {
	t.Helper()
	mgr, err := NewManager(cfg, st)
	if err != nil {
		t.Fatalf("new mixed transport manager: %v", err)
	}
	server := newClusterHTTPTestServer(t, mgr.Handler())
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start mixed transport manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr, server.URL
}

func firstLibP2PListenAddr(t *testing.T, mgr *Manager) string {
	t.Helper()
	if mgr == nil || mgr.libp2p == nil {
		t.Fatalf("expected libp2p manager")
	}
	addrs := mgr.libp2p.ListenAddrs()
	if len(addrs) == 0 {
		t.Fatalf("expected libp2p listen address")
	}
	for _, addr := range addrs {
		if strings.Contains(addr, "/ip4/127.0.0.1/") {
			return addr
		}
	}
	return addrs[0]
}

func waitForMeshRouteDecision(t *testing.T, mgr *Manager, destinationNodeID int64, trafficClass mesh.TrafficClass, nextHopNodeID int64, outboundTransport mesh.TransportKind) mesh.RouteDecision {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		binding := mgr.MeshRuntime()
		if binding == nil {
			return false
		}
		decision, ok := binding.DescribeRoute(destinationNodeID, trafficClass)
		return ok && decision.NextHopNodeID == nextHopNodeID && decision.OutboundTransport == outboundTransport
	})
	binding := mgr.MeshRuntime()
	if binding == nil {
		t.Fatalf("mesh runtime is not attached")
	}
	decision, ok := binding.DescribeRoute(destinationNodeID, trafficClass)
	if !ok {
		t.Fatalf("expected mesh route to node %d for traffic %v", destinationNodeID, trafficClass)
	}
	if decision.NextHopNodeID != nextHopNodeID || decision.OutboundTransport != outboundTransport {
		t.Fatalf("unexpected mesh route decision: got=%+v want next=%d transport=%v", decision, nextHopNodeID, outboundTransport)
	}
	return decision
}
