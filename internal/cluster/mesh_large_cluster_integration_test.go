package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerLargeWebSocketClusterRoutesAcrossLinearTopology(t *testing.T) {
	const nodeCount = 7

	managers := startLinearWebSocketManagers(t, nodeCount)
	source := managers[0]
	target := managers[nodeCount-1]
	target.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(nodeCount),
			UserID:   12288,
			Username: "large-cluster-user",
		}}, nil
	})

	waitForMeshRouteDecision(t, source, testNodeID(nodeCount), mesh.TrafficControlQuery, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(t, target, testNodeID(1), mesh.TrafficControlQuery, testNodeID(nodeCount-1), mesh.TransportWebSocket)

	var users []app.LoggedInUserSummary
	waitFor(t, 5*time.Second, func() bool {
		var err error
		users, err = source.QueryLoggedInUsers(context.Background(), testNodeID(nodeCount))
		return err == nil && len(users) == 1
	})
	if len(users) != 1 || users[0].NodeID != testNodeID(nodeCount) || users[0].UserID != 12288 || users[0].Username != "large-cluster-user" {
		t.Fatalf("unexpected users: %+v", users)
	}

	delivered := make(chan store.TransientPacket, 1)
	source.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})
	waitForMeshRouteDecision(t, target, testNodeID(1), mesh.TrafficTransientInteractive, testNodeID(nodeCount-1), mesh.TransportWebSocket)

	packet := store.TransientPacket{
		PacketID:     1201,
		SourceNodeID: testNodeID(nodeCount),
		TargetNodeID: testNodeID(1),
		Recipient:    store.UserKey{NodeID: testNodeID(1), UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(nodeCount), UserID: 100},
		Body:         []byte("large-cluster-transient"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      defaultPacketTTLHops,
	}
	if err := target.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route transient packet across %d-node websocket cluster: %v", nodeCount, err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.SourceNodeID != packet.SourceNodeID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "large-cluster-transient" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
		if got.TTLHops != defaultPacketTTLHops-(nodeCount-1) {
			t.Fatalf("unexpected delivered ttl: got=%d want=%d", got.TTLHops, defaultPacketTTLHops-(nodeCount-1))
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for large-cluster transient delivery")
	}

	waitFor(t, 5*time.Second, func() bool {
		return countManagersWithForwardedMetric(managers[1:nodeCount-1], "control_critical") == nodeCount-2
	})
	waitFor(t, 5*time.Second, func() bool {
		return countManagersWithForwardedMetric(managers[1:nodeCount-1], "transient_interactive") == nodeCount-2
	})
}

func TestManagerLargeMixedTransportClusterRoutesAcrossAlternatingBridges(t *testing.T) {
	managers := startAlternatingWebSocketLibP2PManagers(t)
	source := managers[0]
	target := managers[len(managers)-1]
	targetNodeID := testNodeID(uint16(len(managers)))
	target.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   targetNodeID,
			UserID:   16384,
			Username: "large-mixed-user",
		}}, nil
	})

	waitForMeshRouteDecision(t, source, targetNodeID, mesh.TrafficControlQuery, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(t, target, testNodeID(1), mesh.TrafficControlQuery, testNodeID(4), mesh.TransportLibP2P)

	var users []app.LoggedInUserSummary
	waitFor(t, 5*time.Second, func() bool {
		var err error
		users, err = source.QueryLoggedInUsers(context.Background(), targetNodeID)
		return err == nil && len(users) == 1
	})
	if len(users) != 1 || users[0].NodeID != targetNodeID || users[0].UserID != 16384 || users[0].Username != "large-mixed-user" {
		t.Fatalf("unexpected users: %+v", users)
	}

	delivered := make(chan store.TransientPacket, 1)
	target.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})
	waitForMeshRouteDecision(t, source, targetNodeID, mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)

	packet := store.TransientPacket{
		PacketID:     1202,
		SourceNodeID: testNodeID(1),
		TargetNodeID: targetNodeID,
		Recipient:    store.UserKey{NodeID: targetNodeID, UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:         []byte("large-mixed-transient"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      defaultPacketTTLHops,
	}
	if err := source.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route transient packet across alternating transport cluster: %v", err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.SourceNodeID != packet.SourceNodeID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "large-mixed-transient" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
		wantTTL := int32(defaultPacketTTLHops - (len(managers) - 1))
		if got.TTLHops != wantTTL {
			t.Fatalf("unexpected delivered ttl: got=%d want=%d", got.TTLHops, wantTTL)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for large mixed-transport transient delivery")
	}

	if err := source.routeMeshReplicationBatch(context.Background(), targetNodeID, 1, "2026-01-01T00:00:00.000000000Z/1/1", &internalproto.EventBatch{
		OriginNodeId: testNodeID(1),
	}); !errors.Is(err, mesh.ErrNoRoute) {
		t.Fatalf("expected replication no route across alternating transport bridges, got %v", err)
	}
	if err := source.routeMeshSnapshotManifest(context.Background(), targetNodeID, &internalproto.SnapshotDigest{
		SnapshotVersion: internalproto.SnapshotVersion,
	}); !errors.Is(err, mesh.ErrNoRoute) {
		t.Fatalf("expected snapshot no route across alternating transport bridges, got %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		return countManagersWithBridgeMetric(managers[1:len(managers)-1], "control_critical") >= 1
	})
	waitFor(t, 5*time.Second, func() bool {
		return countManagersWithBridgeMetric(managers[1:len(managers)-1], "transient_interactive") >= 1
	})
	metrics := source.meshMetricsSnapshot()
	if !hasMeshNoPathMetric(metrics, "replication_stream") {
		t.Fatalf("expected replication no-path metric, got %+v", metrics.RoutingNoPath)
	}
	if !hasMeshNoPathMetric(metrics, "snapshot_bulk") {
		t.Fatalf("expected snapshot no-path metric, got %+v", metrics.RoutingNoPath)
	}
}

func startAlternatingWebSocketLibP2PManagers(t *testing.T) []*Manager {
	t.Helper()
	managers := make([]*Manager, 5)
	serverURLs := make([]string, 5)

	managers[4], _ = startMixedTransportNode(t, 5, true, nil)
	node5LibP2PURL := firstLibP2PListenAddr(t, managers[4])

	managers[2], serverURLs[2] = startMixedTransportNode(t, 3, true, nil)
	node3LibP2PURL := firstLibP2PListenAddr(t, managers[2])

	managers[0], serverURLs[0] = startMixedTransportNode(t, 1, false, nil)

	managers[1], _ = startMixedTransportNode(t, 2, true, []Peer{
		{URL: websocketURL(serverURLs[0]) + websocketPath},
		{URL: node3LibP2PURL},
	})
	managers[3], _ = startMixedTransportNode(t, 4, true, []Peer{
		{URL: websocketURL(serverURLs[2]) + websocketPath},
		{URL: node5LibP2PURL},
	})

	return managers
}

func startMixedTransportNode(t *testing.T, nodeSlot uint16, libp2pEnabled bool, peers []Peer) (*Manager, string) {
	t.Helper()
	st := newReplicationTestStore(t, "large-mixed", nodeSlot)
	cfg := mixedTransportBaseConfig(nodeSlot)
	if libp2pEnabled {
		cfg.LibP2P = mixedTransportLibP2PConfig(t, "large-mixed-node")
	}
	cfg.Peers = peers
	return startMixedTransportManager(t, cfg, st)
}

func countManagersWithForwardedMetric(managers []*Manager, trafficClass string) int {
	count := 0
	for _, mgr := range managers {
		if mgr == nil {
			continue
		}
		if hasForwardedMetric(mgr.meshMetricsSnapshot().ForwardedPackets, trafficClass) {
			count++
		}
	}
	return count
}

func countManagersWithBridgeMetric(managers []*Manager, trafficClass string) int {
	count := 0
	for _, mgr := range managers {
		if mgr == nil {
			continue
		}
		if hasForwardedMetric(mgr.meshMetricsSnapshot().BridgeForwards, trafficClass) {
			count++
		}
	}
	return count
}
