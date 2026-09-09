package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"google.golang.org/protobuf/proto"
)

type tcpTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newTCPTestCA(t *testing.T) tcpTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test cluster CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	return tcpTestCA{cert, key, file}
}
func (ca tcpTestCA) config(t *testing.T, id int64, mutate func(*x509.Certificate)) Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(fmt.Sprintf("urn:turntf:node:%d", id))
	cert := &x509.Certificate{SerialNumber: big.NewInt(id + 10), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}}
	if mutate != nil {
		mutate(cert)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	for file, block := range map[string]*pem.Block{certFile: {Type: "CERTIFICATE", Bytes: der}, keyFile: {Type: "PRIVATE KEY", Bytes: keyDER}} {
		if err := os.WriteFile(file, pem.EncodeToMemory(block), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return Config{NodeID: id, AdvertisePath: WebSocketPath, ClusterSecret: "tcp-test-secret", TCPMTLS: TCPMTLSConfig{Enabled: true, ListenAddr: "127.0.0.1:0", CAFile: ca.file, CertFile: certFile, KeyFile: keyFile, AllowedNodeIDs: []int64{1, 2, 3}, HandshakeTimeoutMs: 500, MaxFrameBytes: 1 << 20}}
}
func startTCPTestAdapter(t *testing.T, cfg Config) *TCPMTLSMeshTransportAdapter {
	t.Helper()
	a := NewTCPMTLSMeshTransportAdapter(cfg)
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}
func tcpTestEndpoint(a *TCPMTLSMeshTransportAdapter) string {
	return fmt.Sprintf("tcp+tls://%s/%d", a.listener.Addr(), a.nodeID)
}
func receiveTCPTestConn(t *testing.T, a *TCPMTLSMeshTransportAdapter) mesh.TransportConn {
	t.Helper()
	select {
	case c := <-a.Accept():
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("no accepted TCP connection")
		return nil
	}
}

func TestTCPMTLSRealConnectionAndFrames(t *testing.T) {
	ca := newTCPTestCA(t)
	a := startTCPTestAdapter(t, ca.config(t, 1, nil))
	b := startTCPTestAdapter(t, ca.config(t, 2, nil))
	out, err := a.Dial(context.Background(), tcpTestEndpoint(b))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	in := receiveTCPTestConn(t, b)
	defer in.Close()
	if out.(interface{ AuthenticatedNodeID() int64 }).AuthenticatedNodeID() != 2 || in.(interface{ AuthenticatedNodeID() int64 }).AuthenticatedNodeID() != 1 {
		t.Fatal("certificate identity not exposed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, payload := range []string{"first", "second"} {
		if err := out.Send(ctx, []byte(payload)); err != nil {
			t.Fatal(err)
		}
		got, err := in.Receive(ctx)
		if err != nil || string(got) != payload {
			t.Fatalf("frame: %q %v", got, err)
		}
	}
	if err := out.Send(ctx, make([]byte, a.cfg.MaxFrameBytes+1)); err == nil {
		t.Fatal("oversized send accepted")
	}
	if err := out.Send(ctx, nil); err == nil {
		t.Fatal("empty frame accepted")
	}
	if err := in.Send(ctx, []byte("reply")); err != nil {
		t.Fatal(err)
	}
	got, err := out.Receive(ctx)
	if err != nil || string(got) != "reply" {
		t.Fatalf("reply: %q %v", got, err)
	}
}

func TestTCPMTLSRejectsClientCertificates(t *testing.T) {
	ca := newTCPTestCA(t)
	other := newTCPTestCA(t)
	cases := []struct {
		name   string
		ca     tcpTestCA
		id     int64
		mutate func(*x509.Certificate)
		absent bool
	}{
		{name: "missing", ca: ca, id: 1, absent: true},
		{name: "untrusted CA", ca: other, id: 1},
		{name: "expired", ca: ca, id: 1, mutate: func(c *x509.Certificate) {
			c.NotBefore = time.Now().Add(-2 * time.Hour)
			c.NotAfter = time.Now().Add(-time.Hour)
		}},
		{name: "wrong EKU", ca: ca, id: 1, mutate: func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} }},
		{name: "missing node identity", ca: ca, id: 1, mutate: func(c *x509.Certificate) { c.URIs = nil }},
		{name: "ambiguous identity", ca: ca, id: 1, mutate: func(c *x509.Certificate) { c.URIs = append(c.URIs, c.URIs[0]) }},
		{name: "unallowed node", ca: ca, id: 99},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := startTCPTestAdapter(t, ca.config(t, 2, nil))
			clientCfg := tc.ca.config(t, tc.id, tc.mutate)
			roots := x509.NewCertPool()
			roots.AddCert(ca.cert)
			cfg := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "127.0.0.1"}
			if !tc.absent {
				pair, err := tls.LoadX509KeyPair(clientCfg.TCPMTLS.CertFile, clientCfg.TCPMTLS.KeyFile)
				if err != nil {
					t.Fatal(err)
				}
				cfg.Certificates = []tls.Certificate{pair}
			}
			conn, err := tls.Dial("tcp", server.listener.Addr().String(), cfg)
			if err == nil {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, _ = conn.Write([]byte("invalid"))
				_, err = conn.Read(make([]byte, 1))
				if err == nil {
					t.Fatal("rejected TLS peer read succeeded")
				}
			}
			waitFor(t, 2*time.Second, func() bool { return server.Stats().HandshakeRejected > 0 })
			select {
			case c := <-server.Accept():
				_ = c.Close()
				t.Fatal("invalid certificate reached mesh")
			default:
			}
		})
	}
}

func TestTCPMTLSRejectsServerCertificates(t *testing.T) {
	ca := newTCPTestCA(t)
	other := newTCPTestCA(t)
	for _, name := range []string{"wrong host", "wrong node", "untrusted CA", "expired", "wrong EKU"} {
		t.Run(name, func(t *testing.T) {
			client := startTCPTestAdapter(t, ca.config(t, 1, nil))
			signing := ca
			id := int64(2)
			var mutate func(*x509.Certificate)
			switch name {
			case "wrong host":
				mutate = func(c *x509.Certificate) { c.IPAddresses = nil }
			case "wrong node":
				id = 3
			case "untrusted CA":
				signing = other
			case "expired":
				mutate = func(c *x509.Certificate) {
					c.NotBefore = time.Now().Add(-2 * time.Hour)
					c.NotAfter = time.Now().Add(-time.Hour)
				}
			case "wrong EKU":
				mutate = func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} }
			}
			cfg := signing.config(t, id, mutate)
			pair, err := tls.LoadX509KeyPair(cfg.TCPMTLS.CertFile, cfg.TCPMTLS.KeyFile)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				raw, err := listener.Accept()
				if err != nil {
					return
				}
				defer raw.Close()
				conn := tls.Server(raw, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}})
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = conn.HandshakeContext(ctx)
			}()
			c, err := client.Dial(context.Background(), fmt.Sprintf("tcp+tls://%s/2", listener.Addr()))
			if err == nil {
				_ = c.Close()
				t.Fatal("invalid server accepted")
			}
			<-done
		})
	}
}

func TestTCPMTLSFrameLimitAndCancellation(t *testing.T) {
	for _, size := range []uint32{0, 1025, ^uint32(0)} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			c := &tcpMTLSConn{Conn: left, maxFrame: 1024}
			defer c.Close()
			go func() { var header [4]byte; binary.BigEndian.PutUint32(header[:], size); _, _ = right.Write(header[:]) }()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := c.Receive(ctx); err == nil {
				t.Fatal("invalid frame allocated/accepted")
			}
		})
	}
	for _, send := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel send=%t", send), func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			c := &tcpMTLSConn{Conn: left, maxFrame: 1024}
			defer c.Close()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				if send {
					done <- c.Send(ctx, []byte("blocked"))
				} else {
					_, err := c.Receive(ctx)
					done <- err
				}
			}()
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancellation ignored")
				}
			case <-time.After(time.Second):
				t.Fatal("I/O cancellation blocked")
			}
		})
	}
}

func TestTCPMTLSHandshakeTimeoutAndClose(t *testing.T) {
	ca := newTCPTestCA(t)
	cfg := ca.config(t, 1, nil)
	cfg.TCPMTLS.HandshakeTimeoutMs = 50
	a := startTCPTestAdapter(t, cfg)
	raw, err := net.Dial("tcp", a.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	waitFor(t, time.Second, func() bool { return a.Stats().HandshakeRejected > 0 })
	raw2, err := net.Dial("tcp", a.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw2.Close()
	done := make(chan struct{})
	go func() { _ = a.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close blocked by handshake")
	}
	if _, err := a.Dial(context.Background(), "tcp+tls://127.0.0.1:1/2"); err == nil {
		t.Fatal("closed adapter dialed")
	}
}

func startTCPTestRuntime(t *testing.T, id int64, adapters []mesh.TransportAdapter, seeds []mesh.DialSeed, handler mesh.LocalForwardedPacketHandler) *mesh.Runtime {
	t.Helper()
	auth := newMeshEnvelopeAuthenticator("tcp-test-secret")
	r, err := mesh.NewRuntime(mesh.RuntimeOptions{LocalNodeID: id, Adapters: adapters, DialSeeds: seeds, Signer: auth, Verifier: auth, HelloTimeout: time.Second, DialRetryInterval: 50 * time.Millisecond, PingInterval: 100 * time.Millisecond, ForwardedPacketHandler: handler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}
func waitTCPTestRoute(t *testing.T, r *mesh.Runtime, kind mesh.TransportKind) {
	t.Helper()
	waitFor(t, 4*time.Second, func() bool {
		d, ok := r.DescribeRoute(2, mesh.TrafficTransientInteractive)
		return ok && d.OutboundTransport == kind
	})
}

func TestTCPMTLSWSSFallbackRecoveryAndMultipleAddresses(t *testing.T) {
	ca := newTCPTestCA(t)
	cfgA := ca.config(t, 1, nil)
	cfgB := ca.config(t, 2, nil)
	a := NewTCPMTLSMeshTransportAdapter(cfgA)
	b := NewTCPMTLSMeshTransportAdapter(cfgB)
	wsA := NewWebSocketMeshTransportAdapter(cfgA)
	wsB := NewWebSocketMeshTransportAdapter(cfgB)
	received := make(chan string, 16)
	startTCPTestRuntime(t, 2, []mesh.TransportAdapter{b, wsB}, nil, func(_ context.Context, p *mesh.ForwardedPacket) error {
		received <- string(p.GetTransientPacket().GetBody())
		return nil
	})
	server := httptest.NewTLSServer(wsB.Handler())
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	wsA.transport.dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	endpoint := tcpTestEndpoint(b)
	alias := strings.Replace(endpoint, "127.0.0.1", "localhost", 1)
	r := startTCPTestRuntime(t, 1, []mesh.TransportAdapter{a, wsA}, []mesh.DialSeed{{Transport: mesh.TransportTCPMTLS, Endpoint: endpoint}, {Transport: mesh.TransportTCPMTLS, Endpoint: alias}, {Transport: mesh.TransportWebSocket, Endpoint: "wss" + strings.TrimPrefix(server.URL, "https")}}, nil)
	waitFor(t, 4*time.Second, func() bool { return len(r.Adjacencies()) == 3 })
	send := func(body string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.ForwardPacket(ctx, &mesh.ForwardedPacket{PacketId: uint64(len(body)), SourceNodeId: 1, TargetNodeId: 2, TrafficClass: mesh.TrafficTransientInteractive, TtlHops: 8, TransientPacket: &mesh.TransientPacket{Body: []byte(body)}}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-received:
			if got != body {
				t.Fatalf("message %q", got)
			}
		case <-ctx.Done():
			t.Fatal("message not delivered")
		}
	}
	waitTCPTestRoute(t, r, mesh.TransportTCPMTLS)
	send("tcp-primary")
	// 移除同节点的一个地址，另一 TCP 邻接必须保持可达。
	if err := r.RemoveDialSeed(mesh.DialSeed{Transport: mesh.TransportTCPMTLS, Endpoint: alias}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { return len(r.Adjacencies()) == 2 })
	waitTCPTestRoute(t, r, mesh.TransportTCPMTLS)
	send("tcp-second-address-removed")
	addr := b.listener.Addr().String()
	b.mu.Lock()
	_ = b.listener.Close()
	for c := range b.conns {
		_ = c.Close()
	}
	b.mu.Unlock()
	b.wg.Wait()
	waitTCPTestRoute(t, r, mesh.TransportWebSocket)
	send("wss-fallback")
	attempts := a.Stats().DialAttempts
	waitFor(t, time.Second, func() bool { return a.Stats().DialAttempts > attempts })
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.listener = listener
	b.wg.Add(1)
	b.mu.Unlock()
	go b.acceptLoop()
	waitTCPTestRoute(t, r, mesh.TransportTCPMTLS)
	send("tcp-recovered")
}

func TestTCPMTLSHelloIdentityBinding(t *testing.T) {
	ca := newTCPTestCA(t)
	b := NewTCPMTLSMeshTransportAdapter(ca.config(t, 2, nil))
	r := startTCPTestRuntime(t, 2, []mesh.TransportAdapter{b}, nil, nil)
	a := startTCPTestAdapter(t, ca.config(t, 1, nil))
	conn, err := a.Dial(context.Background(), tcpTestEndpoint(b))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// 持有节点 1 合法证书与集群 HMAC，仍不允许在 Hello 冒充节点 3。
	envelope := &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_NodeHello{NodeHello: &mesh.NodeHello{NodeId: 3, ProtocolVersion: mesh.ProtocolVersion, Transports: []*mesh.TransportCapability{{Transport: mesh.TransportTCPMTLS, OutboundEnabled: true}}, ForwardingPolicy: mesh.DefaultForwardingPolicy(1)}}}
	raw, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = newMeshEnvelopeAuthenticator("tcp-test-secret").Sign(envelope, raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Send(ctx, raw); err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Receive(ctx) // 服务器 Hello
	if _, err := conn.Receive(ctx); err == nil {
		t.Fatal("spoofed identity remained connected")
	}
	if len(r.Adjacencies()) != 0 {
		t.Fatal("spoofed identity registered")
	}
}

func TestTCPMTLSDisabledDiscoveryNeverDials(t *testing.T) {
	mgr := newDiscoveryTestManager(t, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	endpoint := fmt.Sprintf("tcp+tls://%s/%d", listener.Addr(), testNodeID(2))
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	if err := mgr.observePeerAdvertisement(testNodeID(3), &internalproto.PeerAdvertisement{NodeId: testNodeID(2), Url: endpoint}); err != nil {
		t.Fatal(err)
	}
	mgr.reconcileDiscoveredDialers()
	if mgr.canDialPeerURL(endpoint) || len(mgr.collectDialSeeds()) != 0 {
		t.Fatal("disabled node scheduled TCP")
	}
	if err := mgr.startMeshDialSeed(&configuredPeer{URL: endpoint}); err != nil {
		t.Fatal(err)
	}
	_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(150 * time.Millisecond))
	if conn, err := listener.Accept(); err == nil {
		_ = conn.Close()
		t.Fatal("disabled node dialed advertised TCP address")
	}
	if mgr.MeshRuntime().tcp != nil {
		t.Fatal("disabled node registered TCP adapter")
	}
	if err := mgr.observePeerAdvertisement(3, &internalproto.PeerAdvertisement{NodeId: testNodeID(3), Url: endpoint}); err == nil {
		t.Fatal("mismatched advertisement accepted")
	}
}
