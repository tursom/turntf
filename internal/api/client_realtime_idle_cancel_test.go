package api

import (
	"context"
	"io"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// WebSocket Receive checks ctx before reading, but an in-progress socket read
// needs Close to return. A context-aware test transport would hide that gap.
type realtimeIdleConn struct {
	*realtimeTestConn
	reading chan struct{}
}

func (c *realtimeIdleConn) Receive(context.Context) ([]byte, error) {
	c.reading <- struct{}{}
	select {
	case payload := <-c.in:
		return payload, nil
	case <-c.closed:
		return nil, io.EOF
	}
}

func TestRealtimeIdleCancellationClosesTransport(t *testing.T) {
	for _, pingFirst := range []bool{false, true} {
		name := "before_first_request"
		if pingFirst {
			name = "after_ping_without_query"
		}
		t.Run(name, func(t *testing.T) {
			s, conn, _ := realtimeFixture(t, true)
			transport := &realtimeIdleConn{realtimeTestConn: conn, reading: make(chan struct{}, 4)}
			s.conn = transport
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = s.readLoop(ctx)
			}()
			t.Cleanup(func() {
				cancel()
				_ = transport.Close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("idle transport did not release readLoop during cleanup")
				}
			})
			awaitRealtime(t, transport.reading)
			if pingFirst {
				queueRealtimeEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_Ping{
					Ping: &internalproto.Ping{RequestId: 1},
				}})
				if nextRealtimeResponse(t, conn).GetPong().GetRequestId() != 1 {
					t.Fatal("missing initial Pong")
				}
				awaitRealtime(t, transport.reading)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancel did not interrupt an idle realtime socket read")
			}
			select {
			case <-conn.closed:
			default:
				t.Fatal("readLoop returned without closing the idle transport")
			}
		})
	}
}
