package api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/cluster"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// Keep the real registry and resolver; the wrapper only counts independent lookups.
func TestRealtimeTargetedStateChanges(t *testing.T) {
	s, c, r := realtimeFixture(t, true)
	ctx := context.Background()
	sender, _, err := s.http.service.CreateUser(ctx, store.CreateUserParams{Username: "state-sender", PasswordHash: "hash", Role: store.RoleUser})
	if err != nil {
		t.Fatal(err)
	}
	target, _, err := s.http.service.CreateUser(ctx, store.CreateUserParams{Username: "state-target", PasswordHash: "hash", Role: store.RoleUser})
	if err != nil {
		t.Fatal(err)
	}
	s.principal.User = sender
	mgr, err := cluster.NewManager(cluster.Config{NodeID: r.ref.ServingNodeID, AdvertisePath: "/internal/cluster/ws", ClusterSecret: "test", MessageWindowSize: store.DefaultMessageWindowSize, MaxClockSkewMs: cluster.DefaultMaxClockSkewMs, DiscoveryDisabled: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.http.service.sessionRegistry = mgr
	register := func(ref store.SessionRef) {
		s.http.service.RegisterLocalSession(store.OnlineSession{User: target.Key(), SessionRef: ref, Transport: "ws", TransientCapable: true}, app.LoggedInUserSummary{NodeID: target.NodeID, UserID: target.ID, Username: target.Username})
	}
	register(r.ref)
	var calls atomic.Int32
	s.http.service.sessions = realtimeResolverFunc(func(ctx context.Context, u store.UserKey) ([]store.OnlineSession, error) {
		calls.Add(1)
		return mgr.ResolveUserSessions(ctx, u)
	})
	startRealtimeLoop(t, s, c)
	send := func(id uint64, ref store.SessionRef, code string, wantCalls int32) {
		t.Helper()
		queueRealtimeEnvelope(t, c, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_SendMessage{SendMessage: &internalproto.SendMessageRequest{RequestId: id, Target: &internalproto.UserRef{NodeId: target.NodeID, UserId: target.ID}, TargetSession: clientProtoSessionRef(ref), Body: []byte("state"), DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT}}})
		resp := nextRealtimeResponse(t, c)
		if code == "" {
			if resp.GetSendMessageResponse().GetRequestId() != id || resp.GetSendMessageResponse().GetTransientAccepted() == nil {
				t.Fatalf("acceptance: %v", resp)
			}
		} else if resp.GetError().GetRequestId() != id || resp.GetError().GetCode() != code {
			t.Fatalf("want %s: %v", code, resp)
		}
		if calls.Load() != wantCalls {
			t.Fatalf("resolver calls=%d want=%d", calls.Load(), wantCalls)
		}
	}
	send(1, r.ref, "", 1)
	params := store.BlacklistParams{Owner: target.Key(), Blocked: sender.Key()}
	if _, _, err := s.http.service.BlockUser(ctx, params); err != nil {
		t.Fatal(err)
	}
	send(2, r.ref, "forbidden", 1) // The original blacklist check precedes lookup.
	if _, _, err := s.http.service.UnblockUser(ctx, params); err != nil {
		t.Fatal(err)
	}
	send(3, r.ref, "", 2)
	s.http.service.UnregisterLocalSession(target.Key(), r.ref)
	send(4, r.ref, "not_found", 3)
	replacement := store.SessionRef{ServingNodeID: r.ref.ServingNodeID, SessionID: "replacement"}
	register(replacement)
	send(5, r.ref, "not_found", 4)
	send(6, replacement, "", 5)
}

func TestRealtimeTargetedLegalBarrierSandwich(t *testing.T) {
	for _, kind := range []string{"ack", "resolve_user_sessions"} {
		t.Run(kind, func(t *testing.T) {
			s, c, r := realtimeFixture(t, true)
			entered := make(chan int32, 4)
			releaseFirst, releaseQuery := make(chan struct{}), make(chan struct{})
			var firstOnce, queryOnce sync.Once
			defer firstOnce.Do(func() { close(releaseFirst) })
			defer queryOnce.Do(func() { close(releaseQuery) })
			var calls atomic.Int32
			var ackSeenAtPost atomic.Bool
			s.http.service.sessions = realtimeResolverFunc(func(ctx context.Context, u store.UserKey) ([]store.OnlineSession, error) {
				n := calls.Add(1)
				if kind == "ack" && n == 2 {
					s.seenMu.Lock()
					_, ok := s.seen[clientMessageCursor{nodeID: s.principal.User.NodeID, seq: 77}]
					s.seenMu.Unlock()
					ackSeenAtPost.Store(ok)
				}
				entered <- n
				var gate <-chan struct{}
				if n == 1 {
					gate = releaseFirst
				} else if kind == "resolve_user_sessions" && n == 2 {
					gate = releaseQuery
				}
				if gate != nil {
					select {
					case <-gate:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return []store.OnlineSession{{User: u, SessionRef: r.ref}}, nil
			})
			startRealtimeLoop(t, s, c)
			awaitCall := func(want int32) {
				t.Helper()
				select {
				case got := <-entered:
					if got != want {
						t.Fatalf("lookup=%d want=%d", got, want)
					}
				case <-time.After(time.Second):
					t.Fatal("lookup missing")
				}
			}
			seen := func() bool {
				s.seenMu.Lock()
				defer s.seenMu.Unlock()
				_, ok := s.seen[clientMessageCursor{nodeID: s.principal.User.NodeID, seq: 77}]
				return ok
			}
			enqueueRealtime(t, s, c, r, 1)
			awaitCall(1)
			if kind == "ack" {
				queueRealtimeEnvelope(t, c, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_AckMessage{AckMessage: &internalproto.AckMessage{Cursor: &internalproto.MessageCursor{NodeId: s.principal.User.NodeID, Seq: 77}}}})
			} else {
				queueRealtimeEnvelope(t, c, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_ResolveUserSessions{ResolveUserSessions: &internalproto.ResolveUserSessionsRequest{RequestId: 2, User: &internalproto.UserRef{NodeId: s.principal.User.NodeID, UserId: s.principal.User.ID}}}})
			}
			enqueueRealtime(t, s, c, r, 3)
			select {
			case n := <-entered:
				t.Fatalf("barrier/post-send overtook first handler: %d", n)
			case <-time.After(50 * time.Millisecond):
			}
			if seen() {
				t.Fatal("Ack overtook first handler")
			}
			firstOnce.Do(func() { close(releaseFirst) })
			if nextRealtimeResponse(t, c).GetSendMessageResponse().GetRequestId() != 1 {
				t.Fatal("first response reordered")
			}
			awaitCall(2)
			if kind == "ack" {
				if !ackSeenAtPost.Load() {
					t.Fatal("post-send ran before Ack")
				}
			} else {
				select {
				case n := <-entered:
					t.Fatalf("post-send overtook query: %d", n)
				case <-time.After(50 * time.Millisecond):
				}
				queryOnce.Do(func() { close(releaseQuery) })
				resp := nextRealtimeResponse(t, c).GetResolveUserSessionsResponse()
				if resp.GetRequestId() != 2 || resp.GetCount() != 1 {
					t.Fatalf("query response: %v", resp)
				}
				awaitCall(3)
			}
			if nextRealtimeResponse(t, c).GetSendMessageResponse().GetRequestId() != 3 {
				t.Fatal("post-send response missing")
			}
		})
	}
}

type realtimeBlockingCloseConn struct {
	*realtimeTestConn
	entered, release chan struct{}
	closeOnce        sync.Once
}

func (c *realtimeBlockingCloseConn) Close() error {
	c.closeOnce.Do(func() { close(c.entered) })
	<-c.release
	return c.realtimeTestConn.Close()
}
func TestRealtimeFinishWaitsForCloseCallback(t *testing.T) {
	s, c, _ := realtimeFixture(t, true)
	conn := &realtimeBlockingCloseConn{realtimeTestConn: c, entered: make(chan struct{}), release: make(chan struct{})}
	s.conn = conn
	var once sync.Once
	defer once.Do(func() { close(conn.release) })
	g := newRealtimeSendGroup(context.Background(), s)
	g.cancel()
	awaitRealtime(t, conn.entered)
	done := make(chan struct{})
	go func() { defer close(done); _ = g.finish() }()
	select {
	case <-done:
		t.Fatal("finish returned during Close callback")
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(conn.release) })
	awaitRealtime(t, done)
}
