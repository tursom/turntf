package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	gproto "google.golang.org/protobuf/proto"
)

// pipelineConn observes actual Receive calls, rather than inferring reader
// progress from replies that cannot get through a deliberately stalled Send.
type pipelineConn struct {
	*realtimeTestConn
	received    chan *internalproto.ClientEnvelope
	sendEntered chan *internalproto.ServerEnvelope
	sendExited  chan struct{}
	sendRelease chan struct{}
	blockSend   bool
}

func newPipelineConn(c *realtimeTestConn, blockSend bool) *pipelineConn {
	return &pipelineConn{
		realtimeTestConn: c,
		received:         make(chan *internalproto.ClientEnvelope, 256),
		sendEntered:      make(chan *internalproto.ServerEnvelope, 256),
		sendExited:       make(chan struct{}, 256),
		sendRelease:      make(chan struct{}),
		blockSend:        blockSend,
	}
}

func (c *pipelineConn) Receive(ctx context.Context) ([]byte, error) {
	b, err := c.realtimeTestConn.Receive(ctx)
	if err == nil {
		e := new(internalproto.ClientEnvelope)
		if err := gproto.Unmarshal(b, e); err != nil {
			return nil, err
		}
		select {
		case c.received <- e:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b, err
}

func (c *pipelineConn) Send(ctx context.Context, b []byte) error {
	e := new(internalproto.ServerEnvelope)
	if err := gproto.Unmarshal(b, e); err != nil {
		return err
	}
	c.sendEntered <- e
	defer func() { c.sendExited <- struct{}{} }()
	if c.blockSend {
		// Ignore ctx here on purpose: cancellation must Close the original
		// transport to interrupt a Send that is already inside transport code.
		select {
		case <-c.sendRelease:
		case <-c.closed:
			return io.ErrClosedPipe
		}
	}
	return c.realtimeTestConn.Send(ctx, b)
}

func pipelineAwait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func pipelineStart(t *testing.T, s *clientWSSession) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	conn := s.conn
	done := make(chan struct{})
	go func() { defer close(done); _ = s.readLoop(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("readLoop did not join pipeline workers")
		}
	})
	return cancel, done
}

func pipelineGroup(t *testing.T, s *clientWSSession) *realtimeSendGroup {
	t.Helper()
	g := newRealtimeSendGroup(context.Background(), s)
	conn := s.conn
	t.Cleanup(func() {
		g.cancel()
		_ = conn.Close()
		done := make(chan struct{})
		go func() { defer close(done); _ = g.finish() }()
		select {
		case <-done:
			g.streamMu.Lock()
			count, bytes := g.streamCount, g.streamBytes
			g.streamMu.Unlock()
			g.replyMu.Lock()
			replies, replyBytes := g.replyCount, g.replyBytes
			g.replyMu.Unlock()
			if count != 0 || bytes != 0 || replies != 0 || replyBytes != 0 {
				t.Errorf("finish retained quotas: stream=%d/%d reply=%d/%d", count, bytes, replies, replyBytes)
			}
		case <-time.After(2 * time.Second):
			t.Error("finish did not join pipeline workers")
		}
	})
	return g
}

// Admission itself must not wait for a lane or the physical writer. Bound that
// assertion too, so a backpressure regression still gets cancellation/cleanup.
func pipelineAdmit(t *testing.T, g *realtimeSendGroup, call func() error) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { defer close(result); result <- call() }()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		g.cancel()
		_ = g.session.conn.Close()
		select {
		case <-result:
		case <-time.After(2 * time.Second):
			t.Error("admission did not join after transport close")
		}
		t.Fatal("admission blocked on unfinished work or physical Send")
		return nil
	}
}

func pipelineFrame(req *internalproto.StreamFrameRequest) *internalproto.ClientEnvelope {
	return &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_StreamFrame{StreamFrame: req}}
}

func pipelinePing(id uint64) *internalproto.ClientEnvelope {
	return &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: id}}}
}

// Cross-lane results and Pong have no total order. Reject duplicates and
// unexpected IDs as well as missing responses, without imposing such an order.
func pipelineCollect(t *testing.T, next func() *internalproto.ServerEnvelope, ids ...uint64) map[uint64]*internalproto.ServerEnvelope {
	t.Helper()
	wanted := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}
	got := make(map[uint64]*internalproto.ServerEnvelope, len(ids))
	for range ids {
		e := next()
		var id uint64
		switch {
		case e.GetError() != nil:
			id = e.GetError().GetRequestId()
		case e.GetPong() != nil:
			id = e.GetPong().GetRequestId()
		case e.GetStreamFrameResult() != nil:
			id = e.GetStreamFrameResult().GetRequestId()
		default:
			t.Fatalf("unexpected response: %v", e)
		}
		if !wanted[id] || got[id] != nil {
			t.Fatalf("unexpected or duplicate response id=%d: %v", id, e)
		}
		got[id] = e
	}
	return got
}

func pipelineReceivedThrough(t *testing.T, c *pipelineConn, id uint64) {
	t.Helper()
	for {
		e := pipelineAwait(t, c.received, "Receive of subsequent request")
		if requestIDForClientEnvelopeBody(e.Body) == id {
			return
		}
	}
}

func TestRealtimePipelineBackgroundSaturation(t *testing.T) {
	s, c, r := realtimeFixture(t, true)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error { return nil }))
	pipelineStart(t, s)
	for i := 0; i < clientRealtimeSendConcurrency; i++ {
		if i%2 == 0 {
			queueSessionLookup(t, s, c, uint64(i+1))
		} else {
			enqueueRealtime(t, s, c, r, uint64(i+1))
		}
	}
	for i := 0; i < clientRealtimeSendConcurrency; i++ {
		pipelineAwait(t, r.entered, "background slot occupancy")
	}
	queueSessionLookup(t, s, c, 701)
	enqueueRealtime(t, s, c, r, 702)
	queueRealtimeEnvelope(t, c, pipelineFrame(testStreamFrameRequest(s, 703)))
	queueRealtimeEnvelope(t, c, pipelinePing(704))
	got := pipelineCollect(t, func() *internalproto.ServerEnvelope { return nextRealtimeResponse(t, c) }, 701, 702, 703, 704)
	for _, id := range []uint64{701, 702} {
		if got[id].GetError().GetCode() != "service_unavailable" {
			t.Fatalf("saturated request %d did not receive service_unavailable: %v", id, got[id])
		}
	}
	if got[703].GetStreamFrameResult() == nil || got[704].GetPong() == nil {
		t.Fatalf("healthy stream/heartbeat failed after overload: %v", got)
	}
	select {
	case <-r.entered:
		t.Fatal("rejected lookup/Relay executed session resolution")
	default:
	}
}

func TestRealtimePipelineLaneFIFOAcrossEpochs(t *testing.T) {
	for _, independent := range []string{"stream_id", "target_session", "target_user"} {
		t.Run(independent, func(t *testing.T) {
			s, c, _ := realtimeFixture(t, true)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			entered := make(chan uint64, 3)
			s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, frame store.StreamFrame) error {
				entered <- frame.Offset
				if frame.Offset == 1 {
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}))
			a1, a2, b := testStreamFrameRequest(s, 1), testStreamFrameRequest(s, 2), testStreamFrameRequest(s, 3)
			a1.Offset, a2.Offset, b.Offset = 1, 2, 3
			a2.Epoch = a1.Epoch + 1 // epoch must not create an independent lane.
			switch independent {
			case "stream_id":
				b.StreamId = []byte("fedcba9876543210")
			case "target_session":
				b.TargetSession.SessionId = "another-target-session"
			case "target_user":
				user, _, err := s.http.service.CreateUser(context.Background(), store.CreateUserParams{Username: "pipeline-other", PasswordHash: "hash", Role: store.RoleUser})
				if err != nil {
					t.Fatal(err)
				}
				b.Target = &internalproto.UserRef{NodeId: user.NodeID, UserId: user.ID}
			}
			pipelineStart(t, s)
			t.Cleanup(unblock)
			queueRealtimeEnvelope(t, c, pipelineFrame(a1))
			if offset := pipelineAwait(t, entered, "stream A entry"); offset != 1 {
				t.Fatalf("first route offset=%d", offset)
			}
			queueRealtimeEnvelope(t, c, pipelineFrame(a2))
			queueRealtimeEnvelope(t, c, pipelineFrame(b))
			queueRealtimeEnvelope(t, c, pipelinePing(4))
			got := pipelineCollect(t, func() *internalproto.ServerEnvelope { return nextRealtimeResponse(t, c) }, 3, 4)
			if got[3].GetStreamFrameResult() == nil || got[4].GetPong() == nil {
				t.Fatalf("independent stream/Ping did not succeed: %v", got)
			}
			if offset := pipelineAwait(t, entered, "stream B entry"); offset != 3 {
				t.Fatalf("same-lane epoch overtook blocked A: offset=%d", offset)
			}
			select {
			case offset := <-entered:
				t.Fatalf("A2 started before A1 completed: offset=%d", offset)
			default:
			}
			unblock()
			if offset := pipelineAwait(t, entered, "second epoch on A"); offset != 2 {
				t.Fatalf("second route offset=%d", offset)
			}
			for _, id := range []uint64{1, 2} {
				if e := nextRealtimeResponse(t, c); e.GetStreamFrameResult().GetRequestId() != id {
					t.Fatalf("same-lane result order: want %d, got %v", id, e)
				}
			}
		})
	}
}

func TestRealtimePipelineBlockedBackgroundWriteStillReads(t *testing.T) {
	s, base, r := realtimeFixture(t, true)
	c := newPipelineConn(base, true)
	s.conn = c
	routed := make(chan struct{}, 1)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(context.Context, store.StreamFrame) error {
		routed <- struct{}{}
		return nil
	}))
	cancel, done := pipelineStart(t, s)
	queueSessionLookup(t, s, base, 1)
	pipelineAwait(t, r.entered, "background lookup")
	close(r.release)
	if e := pipelineAwait(t, c.sendEntered, "blocked background response"); e.GetResolveUserSessionsResponse().GetRequestId() != 1 {
		t.Fatalf("wrong blocked response: %v", e)
	}
	queueRealtimeEnvelope(t, base, pipelineFrame(testStreamFrameRequest(s, 2)))
	queueRealtimeEnvelope(t, base, pipelinePing(3))
	queueRealtimeEnvelope(t, base, pipelinePing(4))
	// Observing Ping 4 proves dispatch of Ping 3 did not wait for its physical
	// write, even though no Pong can reach the peer while Send is stalled.
	pipelineReceivedThrough(t, c, 4)
	pipelineAwait(t, routed, "stream routing despite blocked physical write")
	cancel()
	pipelineAwait(t, base.closed, "Close of original transport")
	pipelineAwait(t, done, "reader and all pending workers")
	pipelineAwait(t, c.sendExited, "blocked Send exit")
}

func TestRealtimePipelineStreamQuotaCloses(t *testing.T) {
	for _, limit := range []string{"requests", "bytes"} {
		for _, zeroID := range []bool{false, true} {
			name := limit + "/correlated"
			if zeroID {
				name = limit + "/zero_id"
			}
			t.Run(name, func(t *testing.T) {
				s, c, _ := realtimeFixture(t, true)
				entered := make(chan struct{}, 1)
				s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, _ store.StreamFrame) error {
					select {
					case entered <- struct{}{}:
					case <-ctx.Done():
						return ctx.Err()
					}
					<-ctx.Done()
					return ctx.Err()
				}))
				g := pipelineGroup(t, s)
				accepted := clientRealtimeStreamRequests
				var payload []byte
				if limit == "bytes" {
					accepted = 63
					payload = make([]byte, 128<<10)
				}
				encoded := 0
				for i := 0; i < accepted; i++ {
					req := testStreamFrameRequest(s, uint64(i+1))
					if zeroID {
						req.RequestId = 0
					}
					req.Payload = payload
					encoded += gproto.Size(req)
					if err := pipelineAdmit(t, g, func() error { return g.submitStream(req) }); err != nil {
						t.Fatalf("request %d rejected below quota: %v", i+1, err)
					}
					if i == 0 {
						pipelineAwait(t, entered, "active stream frame")
					}
				}
				g.streamMu.Lock()
				count, bytes := g.streamCount, g.streamBytes
				g.streamMu.Unlock()
				if count != accepted || bytes < encoded || bytes > clientRealtimeStreamBytes {
					t.Fatalf("active+queued accounting: count=%d want=%d bytes=%d encoded>=%d", count, accepted, bytes, encoded)
				}
				overflow := testStreamFrameRequest(s, 999)
				if zeroID {
					overflow.RequestId = 0
				}
				overflow.Payload = payload
				if limit == "bytes" && encoded+gproto.Size(overflow) <= clientRealtimeStreamBytes {
					t.Fatal("test did not exceed encoded byte quota")
				}
				if err := pipelineAdmit(t, g, func() error { return g.submitStream(overflow) }); err == nil {
					t.Fatal("stream overflow silently accepted/dropped instead of failing")
				}
				pipelineAwait(t, c.closed, "stream overflow transport close")
				pipelineAwait(t, g.ctx.Done(), "stream overflow cancellation")
			})
		}
	}
}

func TestRealtimePipelineZeroIDFramesAreDelivered(t *testing.T) {
	s, c, _ := realtimeFixture(t, true)
	routed := make(chan uint64, 3)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(_ context.Context, frame store.StreamFrame) error {
		routed <- frame.Offset
		return nil
	}))
	g := pipelineGroup(t, s)
	for offset := uint64(1); offset <= 3; offset++ {
		req := testStreamFrameRequest(s, 0)
		req.Offset = offset
		if err := pipelineAdmit(t, g, func() error { return g.submitStream(req) }); err != nil {
			t.Fatal(err)
		}
	}
	for offset := uint64(1); offset <= 3; offset++ {
		if got := pipelineAwait(t, routed, "zero-ID frame delivery"); got != offset {
			t.Fatalf("zero-ID frame reordered/dropped: got=%d want=%d", got, offset)
		}
	}
	drained := make(chan struct{})
	go func() { defer close(drained); g.drain() }()
	t.Cleanup(func() {
		g.cancel()
		_ = c.Close()
		pipelineAwait(t, drained, "zero-ID drain cleanup")
	})
	pipelineAwait(t, drained, "zero-ID frame drain")
	select {
	case e := <-c.out:
		t.Fatalf("successful zero-ID frame unexpectedly replied: %v", e)
	default:
	}
}

func TestRealtimePipelineReaderReplyQuotaCloses(t *testing.T) {
	for _, limit := range []string{"requests", "bytes", "single_oversized"} {
		t.Run(limit, func(t *testing.T) {
			s, base, _ := realtimeFixture(t, true)
			c := newPipelineConn(base, true)
			s.conn = c
			g := pipelineGroup(t, s)
			response := func(id uint64, message string) *internalproto.ServerEnvelope {
				return &internalproto.ServerEnvelope{Body: &internalproto.ServerEnvelope_Error{Error: clientWSError("service_unavailable", message, id)}}
			}
			accepted, message := clientRealtimeReplyRequests, "busy"
			if limit == "bytes" {
				accepted, message = 7, strings.Repeat("x", 128<<10)
			} else if limit == "single_oversized" {
				accepted, message = 0, strings.Repeat("x", clientRealtimeReplyBytes)
			}
			encoded := 0
			for i := 0; i < accepted; i++ {
				e := response(uint64(i+1), message)
				encoded += gproto.Size(e)
				if err := pipelineAdmit(t, g, func() error { return g.reply(e) }); err != nil {
					t.Fatalf("reader reply %d rejected below quota: %v", i+1, err)
				}
				if i == 0 {
					pipelineAwait(t, c.sendEntered, "active reader reply Send")
				}
			}
			g.replyMu.Lock()
			count, bytes := g.replyCount, g.replyBytes
			g.replyMu.Unlock()
			if count != accepted || bytes < encoded || bytes > clientRealtimeReplyBytes {
				t.Fatalf("active+queued reply accounting: count=%d want=%d bytes=%d encoded>=%d", count, accepted, bytes, encoded)
			}
			e := response(999, message)
			if limit != "requests" && encoded+gproto.Size(e) <= clientRealtimeReplyBytes {
				t.Fatal("test did not exceed encoded reply byte quota")
			}
			if err := pipelineAdmit(t, g, func() error { return g.reply(e) }); err == nil {
				t.Fatal("reader reply overflow did not fail")
			}
			pipelineAwait(t, base.closed, "reader reply overflow transport close")
			pipelineAwait(t, g.ctx.Done(), "reader reply overflow cancellation")
		})
	}
}

func TestRealtimePipelineWriteDeadlineClosesOriginalTransport(t *testing.T) {
	for _, phase := range []string{"waiting_for_session_lock", "inside_send"} {
		t.Run(phase, func(t *testing.T) {
			s, base, _ := realtimeFixture(t, true)
			c := newPipelineConn(base, true)
			s.conn = c
			var holder <-chan error
			if phase == "waiting_for_session_lock" {
				done := make(chan error, 1)
				holder = done
				go func() {
					defer close(done)
					done <- s.writeEnvelope(&internalproto.ServerEnvelope{Body: &internalproto.ServerEnvelope_Pong{Pong: &internalproto.Pong{RequestId: 1}}})
				}()
			}
			// Exercise the real deadline/Close helper with a shorter budget;
			// no production timeout is changed and the test need not sleep 10s.
			var cancel context.CancelFunc
			waiter := make(chan error, 1)
			t.Cleanup(func() {
				if cancel != nil {
					cancel()
				}
				_ = c.Close()
				if holder != nil {
					pipelineAwait(t, holder, "session-lock holder cleanup")
				}
				if cancel != nil {
					pipelineAwait(t, waiter, "deadline writer cleanup")
				}
			})
			if holder != nil {
				pipelineAwait(t, c.sendEntered, "physical writer holding session lock")
			}
			ctx, cancelWrite := context.WithTimeout(context.Background(), 100*time.Millisecond)
			cancel = cancelWrite
			go func() { defer close(waiter); waiter <- s.writeRealtimeEncodedEnvelope(ctx, nil) }()
			if holder == nil {
				pipelineAwait(t, c.sendEntered, "Send with shared write deadline")
			}
			pipelineAwait(t, base.closed, "deadline Close of original transport")
			err := pipelineAwait(t, waiter, "deadline writer join")
			if phase == "waiting_for_session_lock" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expired lock waiter returned %v", err)
				}
				select {
				case e := <-c.sendEntered:
					t.Fatalf("expired lock waiter entered Send: %v", e)
				default:
				}
			} else if err == nil {
				t.Fatal("Send succeeded after transport was closed by deadline")
			}
		})
	}
}

func TestRealtimePipelineStateBarriers(t *testing.T) {
	for _, work := range []string{"background", "stream", "reader_reply"} {
		for _, barrier := range []string{"ack", "state_rpc"} {
			t.Run(work+"/"+barrier, func(t *testing.T) {
				s, base, resolver := realtimeFixture(t, true)
				c := newPipelineConn(base, work == "reader_reply")
				s.conn = c
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				entered := make(chan struct{}, 1)
				s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, _ store.StreamFrame) error {
					entered <- struct{}{}
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}))
				pipelineStart(t, s)
				t.Cleanup(unblock)
				var finishWork func()
				switch work {
				case "background":
					queueSessionLookup(t, s, base, 1)
					pipelineAwait(t, resolver.entered, "background before barrier")
					finishWork = sync.OnceFunc(func() { close(resolver.release) })
				case "stream":
					queueRealtimeEnvelope(t, base, pipelineFrame(testStreamFrameRequest(s, 1)))
					pipelineAwait(t, entered, "stream before barrier")
					finishWork = unblock
				case "reader_reply":
					queueRealtimeEnvelope(t, base, pipelinePing(1))
					pipelineAwait(t, c.sendEntered, "reader reply before barrier")
					finishWork = sync.OnceFunc(func() { close(c.sendRelease) })
				}
				t.Cleanup(finishWork)
				cursor := clientMessageCursor{nodeID: s.principal.User.NodeID, seq: 77}
				seen := func() bool {
					s.seenMu.Lock()
					defer s.seenMu.Unlock()
					_, ok := s.seen[cursor]
					return ok
				}
				if barrier == "ack" {
					queueRealtimeEnvelope(t, base, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_AckMessage{AckMessage: &internalproto.AckMessage{Cursor: &internalproto.MessageCursor{NodeId: cursor.nodeID, Seq: cursor.seq}}}})
				} else {
					// This state RPC is unsupported on realtime, but even its
					// rejection must wait for all earlier work and replies.
					queueRealtimeEnvelope(t, base, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_GetUser{GetUser: &internalproto.GetUserRequest{RequestId: 2}}})
				}
				queueRealtimeEnvelope(t, base, pipelinePing(3))
				for {
					e := pipelineAwait(t, c.received, "barrier Receive")
					if e.GetAckMessage() != nil || e.GetGetUser() != nil {
						break
					}
				}
				select {
				case e := <-c.received:
					t.Fatalf("reader passed %s barrier with %s unfinished: %v", barrier, work, e)
				case <-time.After(40 * time.Millisecond):
				}
				if seen() {
					t.Fatal("Ack mutated seen before prior work completed")
				}
				select {
				case e := <-base.out:
					t.Fatalf("barrier replied before prior work completed: %v", e)
				default:
				}
				finishWork()
				pipelineReceivedThrough(t, c, 3)
				if barrier == "ack" && !seen() {
					t.Fatal("Ack was not applied before following Ping was read")
				}
				// The one completed prior response must precede a barrier's
				// response and the following Ping on the physical connection.
				prior := nextRealtimeResponse(t, base)
				if prior.GetResolveUserSessionsResponse().GetRequestId() != 1 && prior.GetStreamFrameResult().GetRequestId() != 1 && prior.GetPong().GetRequestId() != 1 {
					t.Fatalf("prior work did not finish before barrier: %v", prior)
				}
				if barrier == "state_rpc" {
					if e := nextRealtimeResponse(t, base); e.GetError().GetRequestId() != 2 || e.GetError().GetCode() != "invalid_request" {
						t.Fatalf("wrong state RPC rejection after drain: %v", e)
					}
				}
				if e := nextRealtimeResponse(t, base); e.GetPong().GetRequestId() != 3 {
					t.Fatalf("following Ping did not complete after barrier: %v", e)
				}
			})
		}
	}
}

func TestRealtimePipelineWebSocketSlowLane(t *testing.T) {
	s, _, _ := realtimeFixture(t, true)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	entered := make(chan uint64, 3)
	s.http.service.SetStreamFrameRouter(streamFrameRouterFunc(func(ctx context.Context, frame store.StreamFrame) error {
		entered <- frame.Offset
		if frame.Offset == 1 {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}))
	handlerDone := make(chan struct{})
	handlerCtx, stopHandler := context.WithCancel(context.Background())
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(handlerDone)
		s.http.Handler().ServeHTTP(w, req.WithContext(handlerCtx))
	}))
	// Register this before dialing too: a failed login/assertion must release
	// the router and join the hijacked handler, which Server.Close cannot join.
	t.Cleanup(func() {
		stopHandler()
		unblock()
		server.CloseClientConnections()
		server.Close()
	})
	conn := dialClientRealtimeWebSocket(t, server.URL)
	t.Cleanup(func() {
		stopHandler()
		unblock()
		_ = conn.Close()
		select {
		case <-handlerDone:
		case <-time.After(2 * time.Second):
			t.Error("hijacked realtime WebSocket handler did not join")
		}
	})
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	loginClientWebSocketAndRead(t, conn, s.principal.User.Key(), "root-password", false)
	a1, a2, b := testStreamFrameRequest(s, 1), testStreamFrameRequest(s, 2), testStreamFrameRequest(s, 3)
	a1.Offset, a2.Offset, b.Offset = 1, 2, 3
	a2.Epoch = a1.Epoch + 1
	b.StreamId = []byte("fedcba9876543210")
	writeClientEnvelope(t, conn, pipelineFrame(a1))
	if offset := pipelineAwait(t, entered, "WebSocket A entry"); offset != 1 {
		t.Fatalf("first WebSocket route offset=%d", offset)
	}
	writeClientEnvelope(t, conn, pipelineFrame(a2))
	writeClientEnvelope(t, conn, pipelineFrame(b))
	writeClientEnvelope(t, conn, pipelinePing(4))
	got := pipelineCollect(t, func() *internalproto.ServerEnvelope { return readServerEnvelope(t, conn) }, 3, 4)
	if got[3].GetStreamFrameResult() == nil || got[4].GetPong() == nil {
		t.Fatalf("WebSocket B/Ping failed: %v", got)
	}
	if offset := pipelineAwait(t, entered, "WebSocket B entry"); offset != 3 {
		t.Fatalf("WebSocket A2 overtook blocked A1: offset=%d", offset)
	}
	unblock()
	if offset := pipelineAwait(t, entered, "WebSocket A2 entry"); offset != 2 {
		t.Fatalf("WebSocket A2 offset=%d", offset)
	}
	for _, id := range []uint64{1, 2} {
		if e := readServerEnvelope(t, conn); e.GetStreamFrameResult().GetRequestId() != id {
			t.Fatalf("same-lane WebSocket result order: want %d, got %v", id, e)
		}
	}
}
