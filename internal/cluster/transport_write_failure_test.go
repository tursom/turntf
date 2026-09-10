package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type injectedWriteFailureConn struct {
	net.Conn
	fail atomic.Bool
}

func (c *injectedWriteFailureConn) Write(p []byte) (int, error) {
	if c.fail.Load() {
		return 0, errors.New("injected permanent write failure")
	}
	return c.Conn.Write(p)
}

func writeFailurePair(t *testing.T) (*webSocketTransportConn, TransportConn, *injectedWriteFailureConn) {
	t.Helper()
	transport := newWebSocketTransport()
	accepted := make(chan TransportConn, 1)
	errs := make(chan error, 1)
	server := newIPv4WebSocketTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := transport.Upgrade(w, r)
		if err != nil {
			errs <- err
			return
		}
		accepted <- c
	}))
	t.Cleanup(server.Close)
	var raw *injectedWriteFailureConn
	transport.dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		raw = &injectedWriteFailureConn{Conn: c}
		return raw, nil
	}
	client, err := transport.Dial(context.Background(), websocketURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	peer := waitForAcceptedTransport(t, accepted, errs)
	t.Cleanup(func() { _ = peer.Close() })
	return client.(*webSocketTransportConn), peer, raw
}

func TestWebSocketWriteFailureRetiresConnectionImmediately(t *testing.T) {
	client, _, raw := writeFailurePair(t)
	received := make(chan error, 1)
	go func() { _, err := client.Receive(context.Background()); received <- err }()
	raw.fail.Store(true)
	if err := client.Send(context.Background(), []byte("frame")); err == nil {
		t.Fatal("expected injected error")
	}
	select {
	case <-client.done:
	default:
		t.Error("failed connection remains selectable until next ping")
	}
	select {
	case err := <-received:
		if err == nil {
			t.Error("receive succeeded on failed connection")
		}
	case <-time.After(time.Second):
		t.Error("read loop was not released to remove failed adjacency")
	}
}

func TestWebSocketQueuedCancellationDoesNotWaitForWriter(t *testing.T) {
	client, peer, _ := writeFailurePair(t)
	func() {
		client.writeMu.Lock()
		defer client.writeMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- client.Send(ctx, []byte("canceled")) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled queued send waits for active writer")
		}
	}()
	if err := client.Send(context.Background(), []byte("healthy")); err != nil {
		t.Fatal(err)
	}
	p, err := peer.Receive(context.Background())
	if err != nil || string(p) != "healthy" {
		t.Fatalf("healthy connection affected: %q %v", p, err)
	}
}

func TestTCPMTLSQueuedCancellationPreservesConnection(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c := &tcpMTLSConn{Conn: left, maxFrame: 1024}
	defer c.Close()
	func() {
		c.sendMu.Lock()
		defer c.sendMu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Send(ctx, []byte("canceled")) }()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("queued cancellation blocks on send lock")
		}
	}()
	receiver := &tcpMTLSConn{Conn: right, maxFrame: 1024}
	sent := make(chan error, 1)
	go func() { sent <- c.Send(context.Background(), []byte("healthy")) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := receiver.Receive(ctx)
	if err != nil || string(p) != "healthy" {
		t.Fatalf("queued cancellation closed shared connection: %q %v", p, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
}
