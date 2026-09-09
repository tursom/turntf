package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	gproto "google.golang.org/protobuf/proto"
)

type realtimeResolverFunc func(context.Context, store.UserKey) ([]store.OnlineSession, error)

func (f realtimeResolverFunc) ResolveUserSessions(ctx context.Context, u store.UserKey) ([]store.OnlineSession, error) {
	return f(ctx, u)
}

func startRealtimeLoop(t *testing.T, s *clientWSSession, c *realtimeTestConn) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.readLoop(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = c.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("readLoop leaked")
		}
	})
	return cancel, done
}
func queueRealtimeEnvelope(t *testing.T, c *realtimeTestConn, e *internalproto.ClientEnvelope) {
	t.Helper()
	b, err := gproto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	c.in <- b
}
func nextRealtimeResponse(t *testing.T, c *realtimeTestConn) *internalproto.ServerEnvelope {
	t.Helper()
	select {
	case e := <-c.out:
		return e
	case <-time.After(time.Second):
		t.Fatal("missing response")
		return nil
	}
}

type realtimeFailConn struct{ *realtimeTestConn }

func (*realtimeFailConn) Send(context.Context, []byte) error { return io.ErrClosedPipe }

func TestRealtimeTargetedWriteFailureClosesAndJoins(t *testing.T) {
	s, c, r := realtimeFixture(t, true)
	s.conn = &realtimeFailConn{c}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.Close()
	go func() { done <- s.readLoop(ctx) }()
	enqueueRealtime(t, s, c, r, 1)
	enqueueRealtime(t, s, c, r, 2)
	awaitRealtime(t, r.entered)
	awaitRealtime(t, r.entered)
	close(r.release)
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("lost write error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write failure did not stop readLoop")
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("transport left open")
	}
}

func TestRealtimeTargetedWebSocketDisconnectCancelsQueries(t *testing.T) {
	s, _, r := realtimeFixture(t, true)
	exited := make(chan struct{}, 2)
	s.http.service.sessions = realtimeResolverFunc(func(ctx context.Context, u store.UserKey) ([]store.OnlineSession, error) {
		defer func() { exited <- struct{}{} }()
		r.entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	server := newIPv4TestServer(t, s.http.Handler())
	defer server.Close()
	conn := dialClientRealtimeWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocketAndRead(t, conn, s.principal.User.Key(), "root-password", false)
	for i := uint64(1); i <= 2; i++ {
		writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_SendMessage{SendMessage: &internalproto.SendMessageRequest{RequestId: i, Target: &internalproto.UserRef{NodeId: s.principal.User.NodeID, UserId: s.principal.User.ID}, TargetSession: clientProtoSessionRef(r.ref), Body: []byte("test"), DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT}}})
	}
	awaitRealtime(t, r.entered)
	awaitRealtime(t, r.entered)
	_ = conn.Close()
	awaitRealtime(t, exited)
	awaitRealtime(t, exited)
}

func TestRealtimeTargetedBoundAndCancelAtCapacity(t *testing.T) {
	s, c, r := realtimeFixture(t, true)
	cancel, done := startRealtimeLoop(t, s, c)
	for i := 0; i < clientRealtimeSendConcurrency+2; i++ {
		enqueueRealtime(t, s, c, r, uint64(i+1))
	}
	for i := 0; i < clientRealtimeSendConcurrency; i++ {
		awaitRealtime(t, r.entered)
	}
	select {
	case <-r.entered:
		t.Fatal("unbounded session query fanout")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	awaitRealtime(t, done)
}

func TestRealtimeTargetedRPCBarriers(t *testing.T) {
	for _, kind := range []string{"ping", "login", "persistent", "get_user", "untargeted"} {
		t.Run(kind, func(t *testing.T) {
			s, c, r := realtimeFixture(t, true)
			startRealtimeLoop(t, s, c)
			enqueueRealtime(t, s, c, r, 1)
			enqueueRealtime(t, s, c, r, 2)
			awaitRealtime(t, r.entered)
			awaitRealtime(t, r.entered)
			e := &internalproto.ClientEnvelope{}
			switch kind {
			case "ping":
				e.Body = &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 3}}
			case "login":
				e.Body = &internalproto.ClientEnvelope_Login{Login: &internalproto.LoginRequest{}}
			case "persistent":
				e.Body = &internalproto.ClientEnvelope_SendMessage{SendMessage: &internalproto.SendMessageRequest{RequestId: 3}}
			case "get_user":
				e.Body = &internalproto.ClientEnvelope_GetUser{GetUser: &internalproto.GetUserRequest{RequestId: 3}}
			case "untargeted":
				e.Body = &internalproto.ClientEnvelope_SendMessage{SendMessage: &internalproto.SendMessageRequest{RequestId: 3, DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT}}
			}
			queueRealtimeEnvelope(t, c, e)
			select {
			case resp := <-c.out:
				t.Fatalf("RPC overtook pending sends: %v", resp)
			case <-time.After(30 * time.Millisecond):
			}
			close(r.release)
			ids := map[uint64]bool{}
			for i := 0; i < 2; i++ {
				resp := nextRealtimeResponse(t, c).GetSendMessageResponse()
				if resp == nil || resp.GetTransientAccepted() == nil {
					t.Fatalf("expected acceptance: %v", resp)
				}
				ids[resp.RequestId] = true
			}
			if !ids[1] || !ids[2] {
				t.Fatalf("request correlation lost: %v", ids)
			}
			resp := nextRealtimeResponse(t, c)
			if kind == "ping" {
				if resp.GetPong().GetRequestId() != 3 {
					t.Fatalf("bad pong: %v", resp)
				}
			} else {
				code := "invalid_request"
				id := uint64(3)
				if kind == "login" {
					code = "already_authenticated"
					id = 0
				}
				if resp.GetError().GetCode() != code || resp.GetError().GetRequestId() != id {
					t.Fatalf("bad RPC response: %v", resp)
				}
			}
		})
	}
}

func TestRealtimeTargetedValidationContract(t *testing.T) {
	for _, kind := range []string{"valid", "missing_session", "wrong_recipient", "malformed_session", "timeout", "blacklist", "permission", "sync_mode"} {
		t.Run(kind, func(t *testing.T) {
			s, c, r := realtimeFixture(t, true)
			target := s.principal.User.Key()
			var calls atomic.Int32
			s.http.service.sessions = realtimeResolverFunc(func(ctx context.Context, u store.UserKey) ([]store.OnlineSession, error) {
				calls.Add(1)
				if kind == "timeout" {
					return nil, fmt.Errorf("%w: timed out resolving user sessions on node %d", app.ErrServiceUnavailable, r.ref.ServingNodeID)
				}
				if kind == "missing_session" || kind == "wrong_recipient" {
					return nil, nil
				}
				return []store.OnlineSession{{User: u, SessionRef: r.ref}}, nil
			})
			if kind == "blacklist" || kind == "permission" || kind == "wrong_recipient" {
				role := store.RoleUser
				if kind == "permission" {
					role = store.RoleChannel
				}
				user, _, err := s.http.service.CreateUser(context.Background(), store.CreateUserParams{Username: "other", PasswordHash: "hash", Role: role})
				if err != nil {
					t.Fatal(err)
				}
				target = user.Key()
				if kind == "blacklist" {
					sender, _, err := s.http.service.CreateUser(context.Background(), store.CreateUserParams{Username: "sender", PasswordHash: "hash", Role: store.RoleUser})
					if err != nil {
						t.Fatal(err)
					}
					s.principal.User = sender
					if _, _, err := s.http.service.BlockUser(context.Background(), store.BlacklistParams{Owner: target, Blocked: s.principal.User.Key()}); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "permission" {
					s.principal.User.Role = store.RoleUser
				}
			}
			req := &internalproto.SendMessageRequest{RequestId: 123, Target: &internalproto.UserRef{NodeId: target.NodeID, UserId: target.UserID}, TargetSession: clientProtoSessionRef(r.ref), Body: []byte("test"), DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT}
			if kind == "malformed_session" {
				req.TargetSession.SessionId = ""
			}
			if kind == "sync_mode" {
				req.SyncMode = internalproto.ClientMessageSyncMode(1)
			}
			startRealtimeLoop(t, s, c)
			queueRealtimeEnvelope(t, c, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_SendMessage{SendMessage: req}})
			resp := nextRealtimeResponse(t, c)
			if kind == "valid" {
				if resp.GetSendMessageResponse().GetRequestId() != 123 || resp.GetSendMessageResponse().GetTransientAccepted() == nil {
					t.Fatalf("bad acceptance: %v", resp)
				}
			} else {
				code := "not_found"
				switch kind {
				case "malformed_session", "sync_mode":
					code = "invalid_request"
				case "timeout":
					code = "service_unavailable"
				case "blacklist", "permission":
					code = "forbidden"
				}
				if resp.GetError().GetCode() != code || resp.GetError().GetRequestId() != 123 {
					t.Fatalf("bad validation response: %v", resp)
				}
			}
			want := int32(1)
			switch kind {
			case "malformed_session", "sync_mode", "blacklist", "permission":
				want = 0
			}
			if calls.Load() != want {
				t.Fatalf("session validations=%d want=%d", calls.Load(), want)
			}
			// 业务错误不应终止登录会话，后续 RPC 仍可关联并完成。
			queueRealtimeEnvelope(t, c, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 124}}})
			if nextRealtimeResponse(t, c).GetPong().GetRequestId() != 124 {
				t.Fatal("session closed after business error")
			}
		})
	}
}
