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

func startWebSocketLibP2PBridgeManagers(tb testing.TB) (*Manager, *Manager, *Manager) {
	tb.Helper()

	stC := newReplicationTestStore(tb, "node-c", 3)
	cfgC := mixedTransportBaseConfig(3)
	cfgC.LibP2P = mixedTransportLibP2PConfig(tb, "node-c")
	mgrC, _ := startMixedTransportManager(tb, cfgC, stC)
	cLibP2PURL := firstLibP2PListenAddr(tb, mgrC)

	stA := newReplicationTestStore(tb, "node-a", 1)
	cfgA := mixedTransportBaseConfig(1)
	mgrA, serverAURL := startMixedTransportManager(tb, cfgA, stA)

	stB := newReplicationTestStore(tb, "node-b", 2)
	cfgB := mixedTransportBaseConfig(2)
	cfgB.LibP2P = mixedTransportLibP2PConfig(tb, "node-b")
	cfgB.Peers = []Peer{
		{URL: websocketURL(serverAURL) + websocketPath},
		{URL: cLibP2PURL},
	}
	mgrB, _ := startMixedTransportManager(tb, cfgB, stB)

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

func mixedTransportLibP2PConfig(tb testing.TB, name string) LibP2PConfig {
	tb.Helper()
	return LibP2PConfig{
		Enabled:            true,
		PrivateKeyPath:     filepath.Join(tb.TempDir(), name+".key"),
		ListenAddrs:        []string{"/ip4/127.0.0.1/tcp/0"},
		EnableHolePunching: true,
	}
}

func startMixedTransportManager(tb testing.TB, cfg Config, st *store.Store) (*Manager, string) {
	tb.Helper()
	mgr, err := NewManager(cfg, st)
	if err != nil {
		tb.Fatalf("new mixed transport manager: %v", err)
	}
	server := newClusterHTTPTestServer(tb, mgr.Handler())
	if err := mgr.Start(context.Background()); err != nil {
		tb.Fatalf("start mixed transport manager: %v", err)
	}
	tb.Cleanup(func() { _ = mgr.Close() })
	return mgr, server.URL
}

func firstLibP2PListenAddr(tb testing.TB, mgr *Manager) string {
	tb.Helper()
	if mgr == nil || mgr.libp2p == nil {
		tb.Fatalf("expected libp2p manager")
	}
	addrs := mgr.libp2p.ListenAddrs()
	if len(addrs) == 0 {
		tb.Fatalf("expected libp2p listen address")
	}
	for _, addr := range addrs {
		if strings.Contains(addr, "/ip4/127.0.0.1/") {
			return addr
		}
	}
	return addrs[0]
}

func waitForMeshRouteDecision(tb testing.TB, mgr *Manager, destinationNodeID int64, trafficClass mesh.TrafficClass, nextHopNodeID int64, outboundTransport mesh.TransportKind) mesh.RouteDecision {
	tb.Helper()
	waitFor(tb, 5*time.Second, func() bool {
		binding := mgr.MeshRuntime()
		if binding == nil {
			return false
		}
		decision, ok := binding.DescribeRoute(destinationNodeID, trafficClass)
		return ok && decision.NextHopNodeID == nextHopNodeID && decision.OutboundTransport == outboundTransport
	})
	binding := mgr.MeshRuntime()
	if binding == nil {
		tb.Fatalf("mesh runtime is not attached")
	}
	decision, ok := binding.DescribeRoute(destinationNodeID, trafficClass)
	if !ok {
		tb.Fatalf("expected mesh route to node %d for traffic %v", destinationNodeID, trafficClass)
	}
	if decision.NextHopNodeID != nextHopNodeID || decision.OutboundTransport != outboundTransport {
		tb.Fatalf("unexpected mesh route decision: got=%+v want next=%d transport=%v", decision, nextHopNodeID, outboundTransport)
	}
	return decision
}
