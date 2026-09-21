package api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	gproto "google.golang.org/protobuf/proto"
)

type streamFrameRouterFunc func(context.Context, store.StreamFrame) error

func (f streamFrameRouterFunc) RouteStreamFrame(ctx context.Context, frame store.StreamFrame) error {
	return f(ctx, frame)
}

func testStreamFrameRequest(s *clientWSSession, requestID uint64) *internalproto.StreamFrameRequest {
	return &internalproto.StreamFrameRequest{
		RequestId: requestID,
		Target: &internalproto.UserRef{
			NodeId: s.principal.User.NodeID,
			UserId: s.principal.User.ID,
		},
		TargetSession: &internalproto.SessionRef{
			ServingNodeId: s.principal.User.NodeID,
			SessionId:     "target-session",
		},
		StreamId: []byte("0123456789abcdef"),
		Epoch:    1,
		Payload:  []byte("frame"),
	}
}

func TestHandleStreamFrameReturnsMatchingRequestID(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
		return nil
	}))

	const requestID = 41
	if err := s.handleStreamFrame(context.Background(), testStreamFrameRequest(s, requestID)); err != nil {
		t.Fatal(err)
	}
	result := <-conn.out
	if result.GetStreamFrameResult().GetRequestId() != requestID {
		t.Fatalf("stream frame result request_id = %d, want %d", result.GetStreamFrameResult().GetRequestId(), requestID)
	}
}

func TestHandleStreamFrameUnavailablePreservesRequestID(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
		return store.ErrStreamSessionUnavailable
	}))

	const requestID = 42
	if err := s.handleStreamFrame(context.Background(), testStreamFrameRequest(s, requestID)); err != nil {
		t.Fatal(err)
	}
	result := <-conn.out
	if result.GetError().GetCode() != "stream_session_unavailable" {
		t.Fatalf("error code = %q, want stream_session_unavailable", result.GetError().GetCode())
	}
	if result.GetError().GetRequestId() != requestID {
		t.Fatalf("error request_id = %d, want %d", result.GetError().GetRequestId(), requestID)
	}
}

func TestHandleStreamFrameZeroRequestIDDoesNotReply(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	var calls atomic.Int32
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
		calls.Add(1)
		return nil
	}))

	if err := s.handleStreamFrame(context.Background(), testStreamFrameRequest(s, 0)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("route calls = %d, want 1", calls.Load())
	}
	select {
	case result := <-conn.out:
		t.Fatalf("unexpected response for request_id 0: %T", result.Body)
	default:
	}
}

func TestRequestIDForStreamFrame(t *testing.T) {
	const requestID = 43
	body := &internalproto.ClientEnvelope_StreamFrame{
		StreamFrame: &internalproto.StreamFrameRequest{RequestId: requestID},
	}
	if got := requestIDForClientEnvelopeBody(body); got != requestID {
		t.Fatalf("requestIDForClientEnvelopeBody() = %d, want %d", got, requestID)
	}
}

type blockingStreamFrameRouter struct {
	entered chan uint64
	release chan struct{}
}

func (r *blockingStreamFrameRouter) RouteStreamFrame(ctx context.Context, frame store.StreamFrame) error {
	select {
	case r.entered <- frame.Offset:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRealtimeStreamFramesPreserveConnectionOrder(t *testing.T) {
	const requestCount = 2
	s, conn, _ := realtimeFixture(t, true)
	router := &blockingStreamFrameRouter{
		entered: make(chan uint64, requestCount),
		release: make(chan struct{}, requestCount),
	}
	s.http.service.SetStreamFrameRouter(router)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.readLoop(ctx)
	}()
	defer func() {
		cancel()
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("readLoop did not stop")
		}
	}()

	for id := uint64(1); id <= requestCount; id++ {
		request := testStreamFrameRequest(s, id)
		request.Offset = id
		envelope := &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{
			StreamFrame: request,
		}}
		data, err := gproto.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		conn.in <- data
	}
	select {
	case offset := <-router.entered:
		if offset != 1 {
			t.Fatalf("first routed offset = %d, want 1", offset)
		}
	case <-time.After(time.Second):
		t.Fatal("first stream frame was not routed")
	}
	select {
	case offset := <-router.entered:
		t.Fatalf("second stream frame overtook blocked first frame: offset=%d", offset)
	case <-time.After(50 * time.Millisecond):
	}
	router.release <- struct{}{}
	select {
	case offset := <-router.entered:
		if offset != 2 {
			t.Fatalf("second routed offset = %d, want 2", offset)
		}
	case <-time.After(time.Second):
		t.Fatal("second stream frame was not routed after first completed")
	}
	router.release <- struct{}{}

	for want := uint64(1); want <= requestCount; want++ {
		select {
		case result := <-conn.out:
			requestID := result.GetStreamFrameResult().GetRequestId()
			if requestID != want {
				t.Fatalf("result request ID = %d, want %d", requestID, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for stream frame result")
		}
	}
}
