package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

func TestResolveUserSessionsFallback(t *testing.T) {
	for _, tc := range []struct {
		name                                                          string
		candidateFailed, fallbackFailed, localSession, localCandidate bool
		wantCount                                                     int
		wantErr                                                       bool
	}{
		{name: "candidate_failure_empty_fallback", candidateFailed: true, wantErr: true},
		{name: "candidate_failure_successful_fallback", candidateFailed: true, localSession: true, wantCount: 1},
		{name: "normal_empty"},
		{name: "fallback_failure", fallbackFailed: true, wantErr: true},
		{name: "candidate_and_fallback_failure", candidateFailed: true, fallbackFailed: true, wantErr: true},
		{name: "empty_candidate_empty_fallback", localCandidate: true},
		{name: "successful_candidate", localSession: true, localCandidate: true, wantCount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMeshClockTestManager(t)
			user := store.UserKey{NodeID: m.cfg.NodeID, UserID: 4097}
			m.onlinePresenceByUser[user] = make(map[int64]store.OnlineNodePresence)
			if tc.candidateFailed {
				m.onlinePresenceByUser[user][testNodeID(2)] = store.OnlineNodePresence{ServingNodeID: testNodeID(2)}
			}
			if tc.fallbackFailed {
				m.peers[testNodeID(3)] = nil
			}
			if tc.localSession {
				m.localOnlineSessions[user] = map[string]store.OnlineSession{"private-session": {
					User: user, SessionRef: store.SessionRef{ServingNodeID: m.cfg.NodeID, SessionID: "private-session"}, Transport: "ws",
				}}
			}
			if tc.localCandidate {
				m.onlinePresenceByUser[user][m.cfg.NodeID] = store.OnlineNodePresence{ServingNodeID: m.cfg.NodeID}
			}
			items, err := m.ResolveUserSessions(context.Background(), user)
			if len(items) != tc.wantCount || (err != nil) != tc.wantErr {
				t.Fatalf("got count=%d err=%v, want count=%d error=%v", len(items), err, tc.wantCount, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, app.ErrServiceUnavailable) {
				t.Fatalf("expected service unavailable, got %v", err)
			}
			if len(m.pendingResolveSessions) != 0 {
				t.Fatal("pending query leaked")
			}
		})
	}
}

// 先成功解析真实远端会话，再锁住远端 registry，验证空 fallback 不会吞掉超时或取消。
func TestResolveUserSessionsCandidateFailureRealNodes(t *testing.T) {
	for _, kind := range []string{"deadline", "cancel", "pre_canceled"} {
		t.Run(kind, func(t *testing.T) {
			source, target := startMeshManagerPair(t, true)
			waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlQuery)
			waitForMeshRoute(t, target, source.cfg.NodeID, mesh.TrafficControlQuery)
			user := store.UserKey{NodeID: target.cfg.NodeID, UserID: 4097}
			ref := store.SessionRef{ServingNodeID: target.cfg.NodeID, SessionID: "regression-session"}
			target.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: ref, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "regression-user"})
			waitFor(t, 5*time.Second, func() bool {
				for _, id := range source.presenceCandidateNodeIDs(user) {
					if id == target.cfg.NodeID {
						return true
					}
				}
				return false
			})
			items, err := source.ResolveUserSessions(context.Background(), user)
			if err != nil || len(items) != 1 || items[0].SessionRef != ref {
				t.Fatalf("preflight: %v %v", items, err)
			}
			target.mu.Lock()
			defer target.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			if kind == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
			}
			defer cancel()
			if kind == "pre_canceled" {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				items, err := source.ResolveUserSessions(ctx, user)
				if len(items) != 0 {
					err = errors.New("unexpected sessions during stalled query")
				}
				done <- err
			}()
			if kind != "pre_canceled" {
				waitFor(t, time.Second, func() bool {
					source.mu.Lock()
					defer source.mu.Unlock()
					return len(source.pendingResolveSessions) == 1
				})
			}
			if kind == "cancel" {
				cancel()
			}
			select {
			case err = <-done:
			case <-time.After(queryLoggedInUsersTimeout + time.Second):
				t.Fatal("query did not exit")
			}
			want := app.ErrServiceUnavailable
			if kind != "deadline" {
				want = context.Canceled
			} else if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatal("query did not reach real deadline")
			}
			if !errors.Is(err, want) {
				t.Fatalf("empty fallback erased candidate failure: got %v, want %v", err, want)
			}
			source.mu.Lock()
			pending := len(source.pendingResolveSessions)
			source.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending queries leaked: %d", pending)
			}
		})
	}
}

func TestResolveUserSessionsFailureDiagnostic(t *testing.T) {
	var buf bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = previous }()
	m := newMeshClockTestManager(t)
	_, err := m.resolveUserSessionsAtNode(context.Background(), testNodeID(2), store.UserKey{NodeID: m.cfg.NodeID, UserID: 4097})
	if err == nil {
		t.Fatal("expected no route")
	}
	var event map[string]any
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"query_request_id", "target_node_id", "elapsed_ms", "error_kind", "error_type", "error"} {
		if _, ok := event[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	if event["event"] != "session_lookup_failed" || event["error_kind"] != "no_route" {
		t.Fatalf("unexpected event: %v", event)
	}
	for _, forbidden := range []string{"secret", "session_uuid", "private-session", "payload", "4097", "credentials"} {
		if strings.Contains(buf.String(), forbidden) {
			t.Fatalf("unexpected sensitive field: %s", forbidden)
		}
	}
}
