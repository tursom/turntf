package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

func TestNodeSessionLookupDoesNotQueryOtherPresenceNodes(t *testing.T) {
	m := newMeshClockTestManager(t)
	user := store.UserKey{NodeID: m.cfg.NodeID, UserID: 4097}
	ref := store.SessionRef{ServingNodeID: m.cfg.NodeID, SessionID: "local"}
	m.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: ref, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "local-user"})
	m.mu.Lock()
	m.onlinePresenceByUser[user] = map[int64]store.OnlineNodePresence{testNodeID(2): {ServingNodeID: testNodeID(2)}}
	before := m.nextResolveSessionsQueryID
	m.mu.Unlock()
	items, err := m.ResolveUserSessionsAtNode(context.Background(), user, m.cfg.NodeID)
	if err != nil || len(items) != 1 || items[0].SessionRef != ref {
		t.Fatalf("local lookup: %v %v", items, err)
	}
	m.mu.Lock()
	after := m.nextResolveSessionsQueryID
	m.mu.Unlock()
	if before != after {
		t.Fatal("queried an unrelated node")
	}
	m.UnregisterLocalSession(user, ref)
	items, err = m.ResolveUserSessionsAtNode(context.Background(), user, m.cfg.NodeID)
	if err != nil || len(items) != 0 {
		t.Fatalf("stale session: %v %v", items, err)
	}
	items, err = m.ResolveUserSessionsAtNode(context.Background(), user, testNodeID(99))
	if err != nil || len(items) != 0 {
		t.Fatalf("unknown node: %v %v", items, err)
	}
}

func TestNodeSessionLookupRemoteAndCancellation(t *testing.T) {
	source, target := startMeshManagerPair(t, true)
	waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlQuery)
	waitForMeshRoute(t, target, source.cfg.NodeID, mesh.TrafficControlQuery)
	user := store.UserKey{NodeID: target.cfg.NodeID, UserID: 4097}
	ref := store.SessionRef{ServingNodeID: target.cfg.NodeID, SessionID: "remote"}
	target.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: ref, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "remote-user"})
	items, err := source.ResolveUserSessionsAtNode(context.Background(), user, target.cfg.NodeID)
	if err != nil || len(items) != 1 || items[0].SessionRef != ref {
		t.Fatalf("remote lookup: %v %v", items, err)
	}
	target.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = source.ResolveUserSessionsAtNode(ctx, user, target.cfg.NodeID)
	cancel()
	target.mu.Unlock()
	if !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("timeout must remain unavailable, not an empty success: %v", err)
	}
	source.mu.Lock()
	pending := len(source.pendingResolveSessions)
	source.mu.Unlock()
	if pending != 0 {
		t.Fatal("pending query leaked")
	}
	target.UnregisterLocalSession(user, ref)
	items, err = source.ResolveUserSessionsAtNode(context.Background(), user, target.cfg.NodeID)
	if err != nil || len(items) != 0 {
		t.Fatalf("unregistered remote: %v %v", items, err)
	}
}
