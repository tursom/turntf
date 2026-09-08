package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
)

// Keep reading real TLS frames while suppressing all responses after Hello.
type silentTCPAdapter struct {
	*TCPMTLSMeshTransportAdapter
	silent   atomic.Bool
	dropped  atomic.Int64
	accepted chan mesh.TransportConn
	done     chan struct{}
}

func (a *silentTCPAdapter) Start(ctx context.Context) error {
	if err := a.TCPMTLSMeshTransportAdapter.Start(ctx); err != nil {
		return err
	}
	go func() {
		defer close(a.done)
		for {
			select {
			case <-a.ctx.Done():
				return
			case c := <-a.TCPMTLSMeshTransportAdapter.Accept():
				wrapped := &silentTCPConn{tcpMTLSConn: c.(*tcpMTLSConn), adapter: a}
				select {
				case a.accepted <- wrapped:
				case <-a.ctx.Done():
					_ = c.Close()
					return
				}
			}
		}
	}()
	return nil
}
func (a *silentTCPAdapter) Accept() <-chan mesh.TransportConn { return a.accepted }

type silentTCPConn struct {
	*tcpMTLSConn
	adapter *silentTCPAdapter
}

func (c *silentTCPConn) Send(ctx context.Context, p []byte) error {
	if c.adapter.silent.Load() {
		c.adapter.dropped.Add(1)
		return nil
	}
	return c.tcpMTLSConn.Send(ctx, p)
}

func TestTCPMTLSHalfAliveFallbackRecovery(t *testing.T) {
	ca := newTCPTestCA(t)
	cfgA, cfgB := ca.config(t, 1, nil), ca.config(t, 2, nil)
	a := NewTCPMTLSMeshTransportAdapter(cfgA)
	b := &silentTCPAdapter{TCPMTLSMeshTransportAdapter: NewTCPMTLSMeshTransportAdapter(cfgB), accepted: make(chan mesh.TransportConn), done: make(chan struct{})}
	wsA, wsB := NewWebSocketMeshTransportAdapter(cfgA), NewWebSocketMeshTransportAdapter(cfgB)
	received := make(chan string, 8)
	// 对端自己的应答期限远大于测试窗口，不能靠对端主动超时制造回退。
	auth := newMeshEnvelopeAuthenticator("tcp-test-secret")
	rb, err := mesh.NewRuntime(mesh.RuntimeOptions{
		LocalNodeID: 2, Adapters: []mesh.TransportAdapter{b, wsB},
		Signer: auth, Verifier: auth, PingInterval: time.Hour,
		ForwardedPacketHandler: func(_ context.Context, p *mesh.ForwardedPacket) error {
			received <- string(p.GetTransientPacket().GetBody())
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	defer cancelPeer()
	t.Cleanup(func() { _ = rb.Close() })
	if err := rb.Start(peerCtx); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(wsB.Handler())
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	wsA.transport.dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	r := startTCPTestRuntime(t, 1, []mesh.TransportAdapter{a, wsA}, []mesh.DialSeed{{Transport: mesh.TransportTCPMTLS, Endpoint: tcpTestEndpoint(b.TCPMTLSMeshTransportAdapter)}, {Transport: mesh.TransportWebSocket, Endpoint: "wss" + strings.TrimPrefix(server.URL, "https")}}, nil)
	waitFor(t, 4*time.Second, func() bool { return len(r.Adjacencies()) == 2 })
	waitTCPTestRoute(t, r, mesh.TransportTCPMTLS)
	b.silent.Store(true)
	waitTCPTestRoute(t, r, mesh.TransportWebSocket)
	if b.dropped.Load() == 0 {
		t.Fatal("did not exercise silent responses")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.ForwardPacket(ctx, &mesh.ForwardedPacket{SourceNodeId: 1, TargetNodeId: 2, TrafficClass: mesh.TrafficTransientInteractive, TtlHops: 8, TransientPacket: &mesh.TransientPacket{Body: []byte("backup")}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got != "backup" {
			t.Fatal(got)
		}
	case <-ctx.Done():
		t.Fatal("healthy WSS failed to deliver")
	}
	b.silent.Store(false)
	waitTCPTestRoute(t, r, mesh.TransportTCPMTLS)
	// Close during another silent period must join blocked readers and measurement loops.
	b.silent.Store(true)
	done := make(chan struct{})
	go func() { _ = r.Close(); cancelPeer(); _ = rb.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime close leaked")
	}
	select {
	case <-b.done:
	case <-time.After(time.Second):
		t.Fatal("adapter forwarder leaked")
	}
	if len(r.Adjacencies()) != 0 || len(rb.Adjacencies()) != 0 {
		t.Fatal("adjacencies retained after close")
	}
	a.mu.Lock()
	n := len(a.conns)
	a.mu.Unlock()
	if n != 0 {
		t.Fatalf("retained TCP connections: %d", n)
	}
}
