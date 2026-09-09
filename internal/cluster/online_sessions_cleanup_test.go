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

// A real peer receives the query while its session registry is locked. No
// synthetic timeout/error is returned by a resolver or inserted into pending.
func TestResolveSessionsPendingCleanup(t *testing.T) {
	for _, kind := range []string{"cancel", "deadline", "default_timeout"} {
		t.Run(kind, func(t *testing.T) {
			source, target := startMeshManagerPair(t, true)
			waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlQuery)
			waitForMeshRoute(t, target, source.cfg.NodeID, mesh.TrafficControlQuery)
			user := store.UserKey{NodeID: target.cfg.NodeID, UserID: 4097}
			ref := store.SessionRef{ServingNodeID: target.cfg.NodeID, SessionID: "cleanup"}
			target.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: ref, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "cleanup"})
			// Establish successful end-to-end routing before inducing a stalled registry.
			items, err := source.resolveUserSessionsAtNode(context.Background(), target.cfg.NodeID, user)
			if err != nil || len(items) != 1 || items[0].SessionRef != ref {
				t.Fatalf("preflight: %v %v", items, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if kind == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
			}
			defer cancel()
			target.mu.Lock()
			locked := true
			defer func() {
				if locked {
					target.mu.Unlock()
				}
			}()
			done := make(chan error, 1)
			started := time.Now()
			go func() { _, err := source.resolveUserSessionsAtNode(ctx, target.cfg.NodeID, user); done <- err }()
			var id uint64
			waitFor(t, time.Second, func() bool {
				source.mu.Lock()
				defer source.mu.Unlock()
				for requestID := range source.pendingResolveSessions {
					id = requestID
				}
				return len(source.pendingResolveSessions) == 1
			})
			if kind == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				if kind == "cancel" {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel: %v", err)
					}
				} else if !errors.Is(err, app.ErrServiceUnavailable) {
					t.Fatalf("timeout: %v", err)
				}
				if kind == "default_timeout" && time.Since(started) < queryLoggedInUsersTimeout {
					t.Fatalf("default timeout returned prematurely: %v", err)
				}
				if kind == "deadline" && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("query returned before real deadline: %v", err)
				}
			case <-time.After(queryLoggedInUsersTimeout + 2*time.Second):
				t.Fatal("query did not exit")
			}
			source.mu.Lock()
			pending := len(source.pendingResolveSessions)
			source.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending queries leaked: %d", pending)
			}
			if source.resolveResolveUserSessionsQuery(id, resolveUserSessionsQueryResult{}) {
				t.Fatal("late response claimed canceled query")
			}
			target.mu.Unlock()
			locked = false
			// Releasing the real peer permits its late reply and a fresh query to arrive.
			items, err = source.resolveUserSessionsAtNode(context.Background(), target.cfg.NodeID, user)
			if err != nil || len(items) != 1 || items[0].SessionRef != ref {
				t.Fatalf("query after cleanup: %v %v", items, err)
			}
			source.mu.Lock()
			pending = len(source.pendingResolveSessions)
			source.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending after recovery: %d", pending)
			}
		})
	}
}
