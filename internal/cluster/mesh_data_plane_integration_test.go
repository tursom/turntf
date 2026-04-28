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

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlCritical)

	var users []app.LoggedInUserSummary
	waitFor(t, 5*time.Second, func() bool {
		var err error
		users, err = mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
		return err == nil && len(users) == 1
	})
	if len(users) != 1 || users[0].NodeID != testNodeID(3) || users[0].UserID != 2048 || users[0].Username != "mesh-user" {
		t.Fatalf("unexpected users: %+v", users)
	}
}

func TestManagerPresencePropagationAndResolveUserSessionsUsesMeshMultiHop(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	user := store.UserKey{NodeID: testNodeID(3), UserID: 4097}
	session := store.OnlineSession{
		User:             user,
		SessionRef:       store.SessionRef{ServingNodeID: testNodeID(3), SessionID: "sess-c-1"},
		Transport:        "ws",
		TransientCapable: true,
	}

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlCritical)
	waitForMeshRoute(t, mgrC, testNodeID(1), mesh.TrafficControlCritical)

	mgrC.RegisterLocalSession(session)

	waitFor(t, 5*time.Second, func() bool {
		presence, err := mgrA.QueryOnlineUserPresence(context.Background(), user)
		return err == nil && len(presence) == 1 && presence[0].ServingNodeID == testNodeID(3) && presence[0].SessionCount == 1
	})

	sessions, err := mgrA.ResolveUserSessions(context.Background(), user)
	if err != nil {
		t.Fatalf("resolve user sessions via mesh: %v", err)
	}
	if len(sessions) != 1 || sessions[0].SessionRef != session.SessionRef || sessions[0].Transport != "ws" {
		t.Fatalf("unexpected resolved user sessions: %+v", sessions)
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

func TestManagerRoutesTargetedTransientPacketViaMeshMultiHop(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	delivered := make(chan store.TransientPacket, 1)
	targetSession := store.SessionRef{ServingNodeID: testNodeID(3), SessionID: "sess-c-2"}
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive)

	packet := store.TransientPacket{
		PacketID:      288,
		SourceNodeID:  testNodeID(1),
		TargetNodeID:  testNodeID(3),
		Recipient:     store.UserKey{NodeID: testNodeID(3), UserID: 99},
		Sender:        store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:          []byte("mesh-targeted"),
		DeliveryMode:  store.DeliveryModeBestEffort,
		TTLHops:       defaultPacketTTLHops,
		TargetSession: targetSession,
	}
	if err := mgrA.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route targeted transient packet via mesh: %v", err)
	}

	select {
	case got := <-delivered:
		if got.TargetSession != targetSession || string(got.Body) != "mesh-targeted" {
			t.Fatalf("unexpected delivered targeted packet: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for targeted transient delivery")
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

func TestManagerQueryLoggedInUsersEmptyPresenceMirrorReturnsEmptyList(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{}, nil
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlCritical)

	waitFor(t, 5*time.Second, func() bool {
		users, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
		return err == nil && len(users) == 0
	})
}

func TestManagerQueryLoggedInUsersUsesSnapshotMirrorWithoutRemoteRPC(t *testing.T) {
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

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlCritical)

	var first []app.LoggedInUserSummary
	waitFor(t, 5*time.Second, func() bool {
		var err error
		first, err = mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
		return err == nil && len(first) == 1
	})
	callsAfterMirror := calls.Load()

	second, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("second query logged-in users via mirror: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0] != second[0] {
		t.Fatalf("unexpected mirrored query results: first=%+v second=%+v", first, second)
	}
	if got := calls.Load(); got != callsAfterMirror {
		t.Fatalf("expected remote query to read only local mirror, calls before=%d after=%d", callsAfterMirror, got)
	}
}

func TestManagerQueryLoggedInUsersProviderFailureSkipsSnapshotBroadcast(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return nil, errors.New("upstream overloaded")
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlCritical)

	mgrA.clearEphemeralStateForNode(testNodeID(3), 0, "test_reset")
	waitFor(t, time.Second, func() bool {
		_, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
		return err != nil
	})

	user := store.UserKey{NodeID: testNodeID(3), UserID: 6001}
	mgrC.RegisterLocalSession(store.OnlineSession{
		User:             user,
		SessionRef:       store.SessionRef{ServingNodeID: testNodeID(3), SessionID: "sess-provider-fail"},
		Transport:        "ws",
		TransientCapable: true,
	})

	time.Sleep(100 * time.Millisecond)
	if _, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3)); err == nil {
		t.Fatalf("expected query to remain unavailable when provider failure skips snapshot broadcast")
	}

	presence, err := mgrA.QueryOnlineUserPresence(context.Background(), user)
	if err != nil {
		t.Fatalf("query presence: %v", err)
	}
	if len(presence) != 0 {
		t.Fatalf("expected no partial presence update when authoritative snapshot broadcast was skipped, got %+v", presence)
	}
}

func TestManagerTransitNodeRecordsForwardingMetrics(t *testing.T) {
	t.Parallel()

	mgrA, mgrB, mgrC := startLinearMeshManagers(t)
	user := store.UserKey{NodeID: testNodeID(3), UserID: 4097}
	mgrC.RegisterLocalSession(store.OnlineSession{
		User:             user,
		SessionRef:       store.SessionRef{ServingNodeID: testNodeID(3), SessionID: "sess-metrics"},
		Transport:        "ws",
		TransientCapable: true,
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlQuery)
	waitFor(t, 5*time.Second, func() bool {
		sessions, err := mgrA.ResolveUserSessions(context.Background(), user)
		return err == nil && len(sessions) == 1
	})

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
