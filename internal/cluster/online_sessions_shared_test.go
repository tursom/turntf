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

func TestSessionLookupSharesOnlyInflightAndIsolatesCancellation(t *testing.T) {
	source, target := startMeshManagerPair(t, true)
	waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlQuery)
	waitForMeshRoute(t, target, source.cfg.NodeID, mesh.TrafficControlQuery)
	user := store.UserKey{NodeID: target.cfg.NodeID, UserID: 9101}
	ref := store.SessionRef{ServingNodeID: target.cfg.NodeID, SessionID: "first"}
	register := func(r store.SessionRef) {
		target.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: r, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "shared-query"})
	}
	register(ref)
	target.mu.Lock()
	locked := true
	defer func() {
		if locked {
			target.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := source.resolveUserSessionsAtNode(ctx, target.cfg.NodeID, user); first <- err }()
	errorsCh := make(chan error, 15)
	for i := 0; i < 15; i++ {
		go func() {
			items, err := source.resolveUserSessionsAtNode(context.Background(), target.cfg.NodeID, user)
			if err == nil && (len(items) != 1 || items[0].SessionRef != ref) {
				err = errors.New("shared result incorrect")
			}
			errorsCh <- err
		}()
	}
	waitFor(t, time.Second, func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		f := source.sessionLookups[sessionLookupKey{nodeID: target.cfg.NodeID, user: user, runtimeEpoch: source.currentRuntimeEpochForNodeLocked(target.cfg.NodeID)}]
		return f != nil && f.waiters == 16 && len(source.pendingResolveSessions) == 1
	})
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter cancellation: %v", err)
	}
	source.mu.Lock()
	count := source.nextResolveSessionsQueryID
	source.mu.Unlock()
	if count != 1 {
		t.Fatalf("16 callers created %d mesh queries", count)
	}
	target.mu.Unlock()
	locked = false
	for i := 0; i < 15; i++ {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	target.UnregisterLocalSession(user, ref)
	next := store.SessionRef{ServingNodeID: target.cfg.NodeID, SessionID: "next"}
	register(next)
	items, err := source.resolveUserSessionsAtNode(context.Background(), target.cfg.NodeID, user)
	if err != nil || len(items) != 1 || items[0].SessionRef != next {
		t.Fatalf("completed lookup was cached: %v %v", items, err)
	}
}
