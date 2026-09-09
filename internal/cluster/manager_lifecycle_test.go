package cluster

import (
	"context"
	"io"
	"sync"
	"testing"
)

func TestManagerAcceptZeroMQConnClosesWhenUnavailable(t *testing.T) {
	t.Parallel()

	t.Run("manager not started", func(t *testing.T) {
		mgr := newHandshakeTestManager(t)
		conn := &recordingTransportConn{transport: transportZeroMQ}
		mgr.AcceptZeroMQConn(conn)
		if got := conn.closeReason(); got != "shutdown" {
			t.Fatalf("unexpected close reason: got=%q want=%q", got, "shutdown")
		}
	})

	t.Run("mesh runtime unavailable", func(t *testing.T) {
		mgr := newHandshakeTestManager(t)
		mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
		t.Cleanup(mgr.cancel)
		conn := &recordingTransportConn{transport: transportZeroMQ}
		mgr.AcceptZeroMQConn(conn)
		if got := conn.closeReason(); got != "mesh runtime unavailable" {
			t.Fatalf("unexpected close reason: got=%q want=%q", got, "mesh runtime unavailable")
		}
	})
}

func TestManagerZeroMQListenerStateIsVisibleInStatus(t *testing.T) {
	t.Parallel()

	var unavailable *Manager
	unavailable.SetZeroMQListenerRunning(true)

	mgr := newHandshakeTestManager(t)
	for _, running := range []bool{true, false} {
		mgr.SetZeroMQListenerRunning(running)
		status, err := mgr.Status(context.Background())
		if err != nil {
			t.Fatalf("manager status: %v", err)
		}
		if status.Discovery.ZeroMQListenerRunning != running {
			t.Fatalf("unexpected listener state: got=%t want=%t", status.Discovery.ZeroMQListenerRunning, running)
		}
	}
}

func TestManagerTransportSelectionRejectsUnsupportedAndMissingDialer(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	if _, err := mgr.transportForPeerURL("smtp://127.0.0.1:25"); err == nil {
		t.Fatal("expected unsupported transport error")
	}
	delete(mgr.dialers, transportWebSocket)
	if _, err := mgr.dialerForPeerURL("ws://127.0.0.1:9082/internal/cluster/ws"); err == nil {
		t.Fatal("expected missing WebSocket dialer error")
	}
}

type recordingTransportConn struct {
	mu        sync.Mutex
	transport string
	reason    string
}

func (*recordingTransportConn) Send(context.Context, []byte) error      { return nil }
func (*recordingTransportConn) Receive(context.Context) ([]byte, error) { return nil, io.EOF }
func (*recordingTransportConn) Close() error                            { return nil }
func (*recordingTransportConn) LocalAddr() string                       { return "local" }
func (*recordingTransportConn) RemoteAddr() string                      { return "remote" }
func (*recordingTransportConn) Direction() string                       { return "inbound" }
func (c *recordingTransportConn) Transport() string                     { return c.transport }

func (c *recordingTransportConn) CloseWithReason(reason string) error {
	c.mu.Lock()
	c.reason = reason
	c.mu.Unlock()
	return nil
}

func (c *recordingTransportConn) closeReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

var _ TransportConn = (*recordingTransportConn)(nil)
