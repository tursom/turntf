package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// A channel keeps discovery/Relay acceptance pending without relying on network
// timing. Existing streams and heartbeat must progress before it is released.
func TestRealtimeSlowRequestsDoNotBlockStreamAndPing(t *testing.T) {
	for _, kind := range []string{"lookup", "targeted_relay", "lookup_at_capacity"} {
		t.Run(kind, func(t *testing.T) {
			s, conn, resolver := realtimeFixture(t, true)
			s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
				return nil
			}))
			cancel, done := startRealtimeLoop(t, s, conn)
			count := 1
			if kind == "lookup_at_capacity" {
				count = clientRealtimeSendConcurrency
			}
			for i := 1; i <= count; i++ {
				if kind == "targeted_relay" {
					enqueueRealtime(t, s, conn, resolver, uint64(i))
				} else {
					queueSessionLookup(t, s, conn, uint64(i))
				}
			}
			for i := 0; i < count; i++ {
				awaitRealtime(t, resolver.entered)
			}

			data := testStreamFrameRequest(s, 1001)
			data.Kind = 4 // DATA
			queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: data}})
			queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 1002}}})
			ack := testStreamFrameRequest(s, 1003)
			ack.Kind, ack.Payload = 5, nil // ACK on an independent stream.
			ack.StreamId = []byte("fedcba9876543210")
			queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: ack}})

			seen := make(map[uint64]bool)
			for range 3 {
				response := nextRealtimeResponse(t, conn)
				id := response.GetStreamFrameResult().GetRequestId()
				if response.GetPong() != nil {
					id = response.GetPong().GetRequestId()
					if id != 1002 {
						t.Fatalf("wrong heartbeat correlation: %v", response)
					}
				} else if id != 1001 && id != 1003 {
					t.Fatalf("wrong stream correlation: %v", response)
				}
				if seen[id] {
					t.Fatalf("duplicate response: %v", response)
				}
				seen[id] = true
			}
			// Cancellation must still stop the unfinished requests and join the
			// group even though stream/Ping processing bypassed its barrier.
			cancel()
			awaitRealtime(t, done)
		})
	}
}

func TestRealtimeLookupErrorAfterStreamKeepsRequestID(t *testing.T) {
	s, conn, resolver := realtimeFixture(t, true)
	s.http.service.sessions = realtimeResolverFunc(func(ctx context.Context, _ store.UserKey) ([]store.OnlineSession, error) {
		resolver.entered <- struct{}{}
		select {
		case <-resolver.release:
			return nil, store.ErrNotFound
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
		return store.ErrStreamSessionUnavailable
	}))
	startRealtimeLoop(t, s, conn)
	queueSessionLookup(t, s, conn, 1)
	awaitRealtime(t, resolver.entered)
	queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: testStreamFrameRequest(s, 2)}})
	response := nextRealtimeResponse(t, conn).GetError()
	if response.GetRequestId() != 2 || response.GetCode() != "stream_session_unavailable" {
		t.Fatalf("stream error lost correlation: %v", response)
	}
	close(resolver.release)
	response = nextRealtimeResponse(t, conn).GetError()
	if response.GetRequestId() != 1 || response.GetCode() != "not_found" {
		t.Fatalf("lookup error lost correlation: %v", response)
	}
}

func TestOrdinarySlowLookupStillOrdersStreamAndPing(t *testing.T) {
	s, conn, resolver := realtimeFixture(t, false)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
		return nil
	}))
	startRealtimeLoop(t, s, conn)
	queueSessionLookup(t, s, conn, 1)
	awaitRealtime(t, resolver.entered)
	queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: testStreamFrameRequest(s, 2)}})
	queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 3}}})
	select {
	case response := <-conn.out:
		t.Fatalf("ordinary connection bypassed pending lookup: %v", response)
	case <-time.After(30 * time.Millisecond):
	}
	close(resolver.release)
	if nextRealtimeResponse(t, conn).GetResolveUserSessionsResponse().GetRequestId() != 1 ||
		nextRealtimeResponse(t, conn).GetStreamFrameResult().GetRequestId() != 2 ||
		nextRealtimeResponse(t, conn).GetPong().GetRequestId() != 3 {
		t.Fatal("ordinary connection order changed")
	}
}

func TestRealtimeWebSocketSlowLookupAllowsStreamAndPing(t *testing.T) {
	s, _, resolver := realtimeFixture(t, true)
	release := sync.OnceFunc(func() { close(resolver.release) })
	routed := make(chan store.StreamFrame, 2)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, frame store.StreamFrame) error {
		select {
		case routed <- frame:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	handlerDone := make(chan struct{})
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(handlerDone)
		s.http.Handler().ServeHTTP(w, req)
	}))
	defer server.Close()
	conn := dialClientRealtimeWebSocket(t, server.URL)
	defer conn.Close()
	// A websocket handler is hijacked from httptest.Server's connection set.
	// Close alone cannot unblock its pending.Wait on the unfixed code: always
	// release discovery and join the handler, including on assertion failure.
	t.Cleanup(func() {
		release()
		_ = conn.Close()
		select {
		case <-handlerDone:
		case <-time.After(2 * time.Second):
			t.Error("websocket handler did not stop after releasing lookup")
		}
	})
	loginClientWebSocketAndRead(t, conn, s.principal.User.Key(), "root-password", false)
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_ResolveUserSessions{
		ResolveUserSessions: &internalproto.ResolveUserSessionsRequest{RequestId: 1, User: &internalproto.UserRef{NodeId: s.principal.User.NodeID, UserId: s.principal.User.ID}},
	}})
	awaitRealtime(t, resolver.entered)
	for index, kind := range []uint32{4, 5} {
		frame := testStreamFrameRequest(s, uint64(index+2))
		frame.Kind, frame.Offset = kind, uint64(index)
		if kind == 5 {
			frame.Payload = nil
		}
		writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: frame}})
	}
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 4}}})
	seen := make(map[uint64]bool)
	for range 3 {
		response := readServerEnvelope(t, conn)
		id := response.GetStreamFrameResult().GetRequestId()
		if response.GetPong() != nil {
			id = response.GetPong().GetRequestId()
			if id != 4 {
				t.Fatalf("wrong heartbeat response: %v", response)
			}
		} else if id != 2 && id != 3 {
			t.Fatalf("wrong frame result: %v", response)
		}
		if seen[id] || (id == 3 && !seen[2]) {
			t.Fatalf("duplicate or reordered same-stream response: %v", response)
		}
		seen[id] = true
	}
	for index, kind := range []uint32{4, 5} {
		select {
		case frame := <-routed:
			if frame.Kind != kind || frame.Offset != uint64(index) {
				t.Fatalf("stream frames reordered: %v", frame)
			}
		default:
			t.Fatal("frame result returned before routing")
		}
	}
	release()
	if response := readServerEnvelope(t, conn); response.GetResolveUserSessionsResponse().GetRequestId() != 1 {
		t.Fatalf("lookup response lost after stream progress: %v", response)
	}
}

func TestRealtimeIndependentTransportFailureCancelsPending(t *testing.T) {
	for _, failure := range []string{"stream_result", "pong", "background_result"} {
		t.Run(failure, func(t *testing.T) {
			s, conn, resolver := realtimeFixture(t, true)
			s.conn = &realtimeFailConn{conn}
			lookupExited := make(chan struct{})
			s.http.service.sessions = realtimeResolverFunc(func(ctx context.Context, user store.UserKey) ([]store.OnlineSession, error) {
				defer close(lookupExited)
				resolver.entered <- struct{}{}
				select {
				case <-resolver.release:
					return []store.OnlineSession{{User: user, SessionRef: resolver.ref}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			routeEntered := make(chan struct{})
			s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, _ store.StreamFrame) error {
				close(routeEntered)
				if failure == "background_result" {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}))
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- s.readLoop(ctx); close(done) }()
			t.Cleanup(func() {
				cancel()
				_ = conn.Close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("transport failure leaked readLoop")
				}
			})
			queueSessionLookup(t, s, conn, 1)
			awaitRealtime(t, resolver.entered)
			if failure == "pong" {
				queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 2}}})
			} else {
				queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: testStreamFrameRequest(s, 2)}})
				awaitRealtime(t, routeEntered)
			}
			if failure == "background_result" {
				// A failing background response must cancel a synchronous stream
				// route that is already waiting on the shared connection context.
				close(resolver.release)
			}
			select {
			case err := <-done:
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("lost transport failure: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("write failure did not join pending work")
			}
			awaitRealtime(t, lookupExited)
			awaitRealtime(t, conn.closed)
		})
	}
}
