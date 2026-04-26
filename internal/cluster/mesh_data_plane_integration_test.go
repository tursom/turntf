package cluster

import (
	"context"
	"errors"
	"sync/atomic"
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

func TestManagerRoutesTransientPacketPreservesMeshTTL(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	delivered := make(chan store.TransientPacket, 1)
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive)

	packet := store.TransientPacket{
		PacketID:     188,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    store.UserKey{NodeID: testNodeID(3), UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:         []byte("mesh-ttl"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      3,
	}
	if err := mgrA.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route transient packet via mesh: %v", err)
	}

	select {
	case got := <-delivered:
		if got.TTLHops != 1 {
			t.Fatalf("expected delivered transient packet to preserve decremented mesh ttl, got %d", got.TTLHops)
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

func TestManagerQueryLoggedInUsersUsesShortTTLCache(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	var calls atomic.Int64
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		calls.Add(1)
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(3),
			UserID:   3001,
			Username: "cached-user",
		}}, nil
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)

	first, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("first query logged-in users via mesh: %v", err)
	}
	second, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("second query logged-in users via mesh: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0] != second[0] {
		t.Fatalf("unexpected cached query results: first=%+v second=%+v", first, second)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected cached second query to skip remote provider, got %d provider calls", got)
	}
}

func TestManagerQueryLoggedInUsersCacheExpires(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	var calls atomic.Int64
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		call := calls.Add(1)
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(3),
			UserID:   4000 + call,
			Username: "expiring-user",
		}}, nil
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)

	first, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("first query logged-in users via mesh: %v", err)
	}
	time.Sleep(loggedInUsersCacheTTL + 25*time.Millisecond)
	second, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("second query logged-in users via mesh: %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("unexpected logged-in users results: first=%+v second=%+v", first, second)
	}
	if first[0].UserID == second[0].UserID {
		t.Fatalf("expected cache expiry to refresh provider result, got first=%+v second=%+v", first, second)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected cache expiry to trigger a second provider call, got %d", got)
	}
}

func TestManagerQueryLoggedInUsersCachesShortFailures(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	var calls atomic.Int64
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		calls.Add(1)
		return nil, errors.New("upstream overloaded")
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)

	for i := 0; i < 2; i++ {
		if _, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3)); err == nil {
			t.Fatalf("expected query %d to fail", i+1)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected negative cache to suppress repeated provider calls, got %d", got)
	}
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

func hasForwardedMetric(samples []meshMetricSample, trafficClass string) bool {
	for _, sample := range samples {
		if sample.TrafficClass == trafficClass && sample.Value > 0 {
			return true
		}
	}
	return false
}
