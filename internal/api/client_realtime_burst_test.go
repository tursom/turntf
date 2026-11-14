package api

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// The deployed TUN uses MTU 1400 and a 4096-packet send queue. This entire
// bounded burst fits the server's 8MiB encoded-byte budget, even when one
// temporary route stall prevents the lane from draining during admission.
func TestRealtimeStreamTunBurstWithinByteBudget(t *testing.T) {
	const packets, mtu = 4096, 1400
	s, conn, _ := realtimeFixture(t, true)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var delivered atomic.Int64
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, frame store.StreamFrame) error {
		if frame.Offset == 0 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		delivered.Add(1)
		return nil
	}))
	g := newRealtimeSendGroup(context.Background(), s)
	t.Cleanup(func() {
		unblock()
		g.cancel()
		_ = conn.Close()
		done := make(chan struct{})
		go func() { defer close(done); _ = g.finish() }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("burst workers did not join")
		}
	})
	payload := make([]byte, mtu)
	encoded := 0
	for i := range packets {
		req := testStreamFrameRequest(s, 0)
		req.Kind, req.Offset, req.Payload = 4, uint64(i*mtu), payload
		encoded += gproto.Size(req)
		if encoded > clientRealtimeStreamBytes {
			t.Fatal("test burst no longer fits the encoded-byte budget")
		}
		if err := pipelineAdmit(t, g, func() error { return g.submitStream(req) }); err != nil {
			t.Fatalf("normal TUN burst rejected at packet %d with only %d encoded bytes: %v", i+1, encoded, err)
		}
		if i == 0 {
			awaitRealtime(t, entered)
		}
	}
	g.streamMu.Lock()
	count, bytes := g.streamCount, g.streamBytes
	g.streamMu.Unlock()
	if count != packets || bytes != encoded {
		t.Fatalf("burst accounting=%d/%d, want %d/%d", count, bytes, packets, encoded)
	}
	unblock()
	drained := make(chan struct{})
	go func() { defer close(drained); g.drain() }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("bounded burst did not drain")
	}
	if got := delivered.Load(); got != packets {
		t.Fatalf("delivered %d packets, want %d", got, packets)
	}
	select {
	case <-conn.closed:
		t.Fatal("bounded valid burst closed its transport")
	default:
	}
}

func TestRealtimeStreamLaneQuotaIndependentOfFrameQuota(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	entered := make(chan struct{}, clientRealtimeStreamLanes)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, _ store.StreamFrame) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	g := pipelineGroup(t, s)
	for i := range clientRealtimeStreamLanes {
		req := testStreamFrameRequest(s, 0)
		req.StreamId = make([]byte, 16)
		binary.LittleEndian.PutUint64(req.StreamId, uint64(i+1))
		if err := pipelineAdmit(t, g, func() error { return g.submitStream(req) }); err != nil {
			t.Fatal(err)
		}
	}
	for range clientRealtimeStreamLanes {
		awaitRealtime(t, entered)
	}
	req := testStreamFrameRequest(s, 0)
	req.StreamId = make([]byte, 16)
	binary.LittleEndian.PutUint64(req.StreamId, uint64(clientRealtimeStreamLanes+1))
	if err := pipelineAdmit(t, g, func() error { return g.submitStream(req) }); !errors.Is(err, errRealtimeStreamFull) {
		t.Fatalf("extra lane returned %v, want stream quota failure", err)
	}
	g.streamMu.Lock()
	lanes, count, bytes := len(g.streams), g.streamCount, g.streamBytes
	g.streamMu.Unlock()
	if lanes > clientRealtimeStreamLanes || count >= clientRealtimeStreamRequests || bytes >= clientRealtimeStreamBytes {
		t.Fatalf("lane rejection was not independent of frame/byte bounds: lanes=%d requests=%d bytes=%d", lanes, count, bytes)
	}
	awaitRealtime(t, conn.closed)
}

func TestRealtimeWebSocketTunBurstWithinByteBudget(t *testing.T) {
	const packets, mtu = 4096, 1400
	s, _, _ := realtimeFixture(t, true)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var delivered atomic.Int64
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, _ store.StreamFrame) error {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		delivered.Add(1)
		return nil
	}))
	handlerDone := make(chan struct{})
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(handlerDone)
		s.http.Handler().ServeHTTP(w, req)
	}))
	defer server.Close()
	conn := dialClientRealtimeWebSocket(t, server.URL)
	t.Cleanup(func() {
		unblock()
		_ = conn.Close()
		select {
		case <-handlerDone:
		case <-time.After(2 * time.Second):
			t.Error("burst websocket handler did not join")
		}
	})
	loginClientWebSocketAndRead(t, conn, s.principal.User.Key(), "root-password", false)
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	encoded := 0
	for i := range packets {
		req := testStreamFrameRequest(s, 0)
		req.Kind, req.Offset, req.Payload = 4, uint64(i*mtu), make([]byte, mtu)
		encoded += gproto.Size(req)
		if encoded > clientRealtimeStreamBytes {
			t.Fatal("wire burst exceeds encoded-byte budget")
		}
		writeClientEnvelope(t, conn, pipelineFrame(req))
	}
	const pingID = 9001
	writeClientEnvelope(t, conn, pipelinePing(pingID))
	if response := readServerEnvelope(t, conn); response.GetPong().GetRequestId() != pingID {
		t.Fatalf("burst closed connection or blocked Ping: %v", response)
	}
	unblock()
	// This retained control barrier waits for all preceding zero-ID frames.
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_AckMessage{
		AckMessage: &internalproto.AckMessage{Cursor: &internalproto.MessageCursor{NodeId: s.principal.User.NodeID, Seq: 1}},
	}})
	writeClientEnvelope(t, conn, pipelinePing(pingID+1))
	if response := readServerEnvelope(t, conn); response.GetPong().GetRequestId() != pingID+1 || delivered.Load() != packets {
		t.Fatalf("wire burst not fully delivered before barrier: delivered=%d response=%v", delivered.Load(), response)
	}
}
