package api

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// Ignore Send's context like a stuck transport; only Close releases it. This
// exercises the watchdog rather than letting a cooperative fake hide it.
type realtimeDeadlineConn struct {
	*realtimeTestConn
	entered chan context.Context
	block   bool
}

func (c *realtimeDeadlineConn) Send(ctx context.Context, _ []byte) error {
	c.entered <- ctx
	if !c.block {
		return nil
	}
	<-c.closed
	return io.ErrClosedPipe
}

func TestRealtimeWriteUsesFiniteTotalBudget(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	transport := &realtimeDeadlineConn{realtimeTestConn: conn, entered: make(chan context.Context, 1)}
	s.conn = transport
	started := time.Now()
	if err := s.writeEncodedEnvelope([]byte("test")); err != nil {
		t.Fatal(err)
	}
	ctx := <-transport.entered
	deadline, ok := ctx.Deadline()
	if !ok || deadline.Before(started) || deadline.After(started.Add(clientWSWriteWait+time.Second)) {
		t.Fatalf("realtime Send did not receive its finite write budget: %v, %v", deadline, ok)
	}
	select {
	case <-conn.closed:
		t.Fatal("successful write closed its transport")
	default:
	}
}

func TestRealtimeWriteDeadlineClosesBlockedSend(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	transport := &realtimeDeadlineConn{realtimeTestConn: conn, entered: make(chan context.Context, 1), block: true}
	s.conn = transport
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		result = s.writeRealtimeEncodedEnvelope(ctx, []byte("test"))
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("write did not join after cleanup Close")
		}
	})
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("write did not enter transport")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("write deadline did not Close the uncooperative transport")
	}
	if result == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("write did not fail at its deadline: %v, %v", result, ctx.Err())
	}
	select {
	case <-conn.closed:
	default:
		t.Fatal("watchdog returned without closing the transport")
	}
}

func TestRealtimeWriteDeadlineIncludesLockWait(t *testing.T) {
	s, conn, _ := realtimeFixture(t, true)
	transport := &realtimeDeadlineConn{realtimeTestConn: conn, entered: make(chan context.Context, 2), block: true}
	s.conn = transport
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(firstDone)
		_ = s.writeRealtimeEncodedEnvelope(firstCtx, []byte("first"))
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		for _, done := range []chan struct{}{firstDone, secondDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("write lock waiter did not join")
			}
		}
	})
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		// Ensure the second completion is still observable by cleanup.
		close(secondDone)
		t.Fatal("first writer did not acquire the lock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var result error
	go func() {
		defer close(secondDone)
		result = s.writeRealtimeEncodedEnvelope(ctx, []byte("second"))
	}()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("waiting for the session lock escaped the write deadline")
	}
	if !errors.Is(result, context.DeadlineExceeded) {
		t.Fatalf("lock waiter returned %v, want deadline exceeded", result)
	}
	select {
	case <-transport.entered:
		t.Fatal("expired lock waiter attempted a second physical Send")
	default:
	}
}
