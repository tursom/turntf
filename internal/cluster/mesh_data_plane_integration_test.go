package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerQueryLoggedInUsersUsesMeshMultiHop(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(3),
			UserID:   2048,
			Username: "mesh-user",
		}}, nil
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)

	users, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("query logged-in users via mesh: %v", err)
	}
	if len(users) != 1 || users[0].NodeID != testNodeID(3) || users[0].UserID != 2048 || users[0].Username != "mesh-user" {
		t.Fatalf("unexpected users: %+v", users)
	}
}

func TestManagerRoutesTransientPacketViaMeshMultiHop(t *testing.T) {
	t.Parallel()

	mgrA, mgrB, mgrC := startLinearMeshManagers(t)
	_ = mgrB
	delivered := make(chan store.TransientPacket, 1)
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive)

	packet := store.TransientPacket{
		PacketID:     88,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    store.UserKey{NodeID: testNodeID(3), UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:         []byte("mesh-transient"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      defaultPacketTTLHops,
	}
	if err := mgrA.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route transient packet via mesh: %v", err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.SourceNodeID != packet.SourceNodeID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "mesh-transient" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for transient delivery")
	}
}

func TestManagerRetryQueueClearsAfterSuccessfulMeshForward(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	delivered := make(chan store.TransientPacket, 1)
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive)

	packet := store.TransientPacket{
		PacketID:     89,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    store.UserKey{NodeID: testNodeID(3), UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:         []byte("mesh-retry"),
		DeliveryMode: store.DeliveryModeRouteRetry,
		TTLHops:      defaultPacketTTLHops,
	}
	key := packetCacheKey(packet.SourceNodeID, packet.PacketID)
	mgrA.mu.Lock()
	mgrA.retryQueue[key] = queuedPacket{
		packet:      packet,
		queuedAt:    time.Now().Add(-time.Second),
		nextAttempt: time.Now().Add(-time.Millisecond),
	}
	mgrA.mu.Unlock()

	mgrA.retryTransientPackets()

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.SourceNodeID != packet.SourceNodeID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "mesh-retry" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for retry delivery")
	}

	waitFor(t, time.Second, func() bool {
		mgrA.mu.Lock()
		defer mgrA.mu.Unlock()
		_, ok := mgrA.retryQueue[key]
		return !ok
	})
}

func TestManagerQueryLoggedInUsersMeshTimeoutClearsPending(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	release := make(chan struct{})
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		<-release
		return []app.LoggedInUserSummary{}, nil
	})
	t.Cleanup(func() { close(release) })

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := mgrA.QueryLoggedInUsers(ctx, testNodeID(3)); err == nil {
		t.Fatalf("expected mesh query timeout")
	}

	waitFor(t, time.Second, func() bool {
		mgrA.mu.Lock()
		defer mgrA.mu.Unlock()
		return len(mgrA.pendingLoggedInUsers) == 0
	})
}

func TestManagerTransitNodeRecordsForwardingMetrics(t *testing.T) {
	t.Parallel()

	mgrA, mgrB, mgrC := startLinearMeshManagers(t)
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(3),
			UserID:   4097,
			Username: "metrics-user",
		}}, nil
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)
	if _, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3)); err != nil {
		t.Fatalf("query logged-in users via mesh: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		metrics := mgrB.meshMetricsSnapshot()
		return hasForwardedMetric(metrics.ForwardedPackets, "control_query") && hasForwardedMetric(metrics.ForwardedBytes, "control_query")
	})
}

func startLinearMeshManagers(t *testing.T, configFns ...func(nodeSlot int, cfg *Config)) (*Manager, *Manager, *Manager) {
	t.Helper()
	applyConfig := func(nodeSlot int, cfg *Config) {
		for _, fn := range configFns {
			if fn != nil {
				fn(nodeSlot, cfg)
			}
		}
	}

	stA := newReplicationTestStore(t, "node-a", 1)
	cfgA := Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}
	applyConfig(1, &cfgA)
	mgrA, err := NewManager(cfgA, stA)
	if err != nil {
		t.Fatalf("new manager A: %v", err)
	}
	serverA := newClusterHTTPTestServer(t, mgrA.Handler())
	if err := mgrA.Start(context.Background()); err != nil {
		t.Fatalf("start manager A: %v", err)
	}
	t.Cleanup(func() { _ = mgrA.Close() })

	stB := newReplicationTestStore(t, "node-b", 2)
	cfgB := Config{
		NodeID:            testNodeID(2),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers: []Peer{
			{URL: websocketURL(serverA.URL) + websocketPath},
		},
	}
	applyConfig(2, &cfgB)
	mgrB, err := NewManager(cfgB, stB)
	if err != nil {
		t.Fatalf("new manager B: %v", err)
	}
	serverB := newClusterHTTPTestServer(t, mgrB.Handler())
	if err := mgrB.Start(context.Background()); err != nil {
		t.Fatalf("start manager B: %v", err)
	}
	t.Cleanup(func() { _ = mgrB.Close() })

	stC := newReplicationTestStore(t, "node-c", 3)
	cfgC := Config{
		NodeID:            testNodeID(3),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers: []Peer{
			{URL: websocketURL(serverB.URL) + websocketPath},
		},
	}
	applyConfig(3, &cfgC)
	mgrC, err := NewManager(cfgC, stC)
	if err != nil {
		t.Fatalf("new manager C: %v", err)
	}
	serverC := newClusterHTTPTestServer(t, mgrC.Handler())
	_ = serverC
	if err := mgrC.Start(context.Background()); err != nil {
		t.Fatalf("start manager C: %v", err)
	}
	t.Cleanup(func() { _ = mgrC.Close() })

	return mgrA, mgrB, mgrC
}

func waitForMeshRoute(t *testing.T, mgr *Manager, destinationNodeID int64, trafficClass mesh.TrafficClass) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		binding := mgr.MeshRuntime()
		if binding == nil {
			return false
		}
		planner := mesh.NewPlanner(mgr.cfg.NodeID)
		_, ok := planner.Compute(binding.TopologyStore().Snapshot(), destinationNodeID, trafficClass, mesh.TransportUnspecified)
		return ok
	})
}

func hasForwardedMetric(samples []meshMetricSample, trafficClass string) bool {
	for _, sample := range samples {
		if sample.TrafficClass == trafficClass && sample.Value > 0 {
			return true
		}
	}
	return false
}
