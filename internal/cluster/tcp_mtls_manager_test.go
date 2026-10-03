package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
)

func TestTCPMTLSManagerDiscoversUpgradeFromWSS(t *testing.T) {
	ca := newTCPTestCA(t)
	cfgA := ca.config(t, testNodeID(1), nil)
	cfgB := ca.config(t, testNodeID(2), nil)
	ids := []int64{testNodeID(1), testNodeID(2)}
	cfgA.TCPMTLS.AllowedNodeIDs = ids
	cfgB.TCPMTLS.AllowedNodeIDs = ids
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserve.Addr().String()
	_ = reserve.Close()
	endpoint := fmt.Sprintf("tcp+tls://%s/%d", addr, cfgB.NodeID)
	cfgB.TCPMTLS.ListenAddr = addr
	cfgB.TCPMTLS.AdvertisedEndpoints = []string{endpoint}
	b, err := NewManager(cfgB, newReplicationTestStore(t, "tcp-mgr-b", 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(b.Handler())
	defer server.Close()
	cfgA.Peers = []Peer{{URL: "wss" + strings.TrimPrefix(server.URL, "https") + WebSocketPath}}
	a, err := NewManager(cfgA, newReplicationTestStore(t, "tcp-mgr-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	a.websocket.dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		for _, adj := range a.MeshRuntime().Runtime().Adjacencies() {
			if adj.Transport == mesh.TransportWebSocket {
				return true
			}
		}
		return false
	})
	// 在同节点已有 WSS 连接后收到 TCP 广告，仍应添加独立拨号种子。
	if err := a.observePeerAdvertisement(cfgB.NodeID, &internalproto.PeerAdvertisement{NodeId: cfgB.NodeID, Url: endpoint, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	a.reconcileDiscoveredDialers()
	waitForMeshRouteDecision(t, a, cfgB.NodeID, mesh.TrafficControlQuery, cfgB.NodeID, mesh.TransportTCPMTLS)
	for _, class := range []mesh.TrafficClass{mesh.TrafficControlCritical, mesh.TrafficTransientInteractive, mesh.TrafficReplicationStream, mesh.TrafficSnapshotBulk} {
		waitForMeshRouteDecision(t, a, cfgB.NodeID, class, cfgB.NodeID, mesh.TransportTCPMTLS)
	}
	status := a.meshStatusSnapshot()
	if !status.TCPMTLS.Enabled || status.TCPMTLS.ActiveAdjacencies < 1 || status.TCPMTLS.DialAttempts < 1 {
		t.Fatalf("TCP observability: %+v", status.TCPMTLS)
	}
	found := false
	for _, item := range b.buildMembershipEnvelope().GetMembershipUpdate().GetPeers() {
		if item.Url == endpoint && item.NodeId == cfgB.NodeID {
			found = true
		}
	}
	if !found {
		t.Fatal("local TCP endpoint missing from membership")
	}
	if len(a.MeshRuntime().Runtime().Adjacencies()) < 2 {
		t.Fatal("WSS backup was discarded")
	}
}

func TestTCPMTLSInboundOnlyAcceptsButNeverDials(t *testing.T) {
	ca := newTCPTestCA(t)
	cfgA := ca.config(t, testNodeID(1), nil)
	cfgB := ca.config(t, testNodeID(2), nil)
	ids := []int64{testNodeID(1), testNodeID(2)}
	cfgA.TCPMTLS.AllowedNodeIDs = ids
	cfgB.TCPMTLS.AllowedNodeIDs = ids
	reserve := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		return addr
	}
	addrA, addrB := reserve(), reserve()
	endpointA := fmt.Sprintf("tcp+tls://%s/%d", addrA, cfgA.NodeID)
	endpointB := fmt.Sprintf("tcp+tls://%s/%d", addrB, cfgB.NodeID)
	cfgA.TCPMTLS.ListenAddr = addrA
	cfgA.TCPMTLS.AdvertisedEndpoints = []string{endpointA}
	cfgB.TCPMTLS.ListenAddr = addrB
	cfgB.TCPMTLS.AdvertisedEndpoints = []string{endpointB}
	cfgB.TCPMTLS.InboundOnly = true

	bad := cfgB
	bad.Peers = []Peer{{URL: endpointA}}
	if _, err := NewManager(bad, newReplicationTestStore(t, "tcp-inbound-only-bad", 2)); err == nil || !strings.Contains(err.Error(), "inbound_only") {
		t.Fatalf("inbound-only static TCP peer accepted: %v", err)
	}
	noListen := cfgB.TCPMTLS
	noListen.ListenAddr = ""
	noListen.AdvertisedEndpoints = nil
	if err := noListen.withDefaults().validate(); err == nil || !strings.Contains(err.Error(), "inbound_only requires listen_addr") {
		t.Fatalf("inbound-only without listener accepted: %v", err)
	}

	b, err := NewManager(cfgB, newReplicationTestStore(t, "tcp-inbound-only-b", 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfgA.Peers = []Peer{{URL: endpointB}}
	a, err := NewManager(cfgA, newReplicationTestStore(t, "tcp-inbound-only-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 入站专用节点收到对端 TCP 广告后也不得生成拨号种子。
	if err := b.observePeerAdvertisement(cfgA.NodeID, &internalproto.PeerAdvertisement{NodeId: cfgA.NodeID, Url: endpointA, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	b.reconcileDiscoveredDialers()
	if b.canDialPeerURL(endpointA) {
		t.Fatal("inbound-only node may dial TCP endpoint")
	}
	for _, seed := range b.collectDialSeeds() {
		if seed.Transport == mesh.TransportTCPMTLS {
			t.Fatalf("inbound-only node produced TCP dial seed %+v", seed)
		}
	}
	if _, err := b.MeshRuntime().tcp.Dial(context.Background(), endpointA); err == nil || !strings.Contains(err.Error(), "inbound_only") {
		t.Fatalf("inbound-only adapter dialed: %v", err)
	}
	waitForMeshRouteDecision(t, a, cfgB.NodeID, mesh.TrafficControlQuery, cfgB.NodeID, mesh.TransportTCPMTLS)
	// 入站邻接同样承载本节点发往对端的流量。
	waitForMeshRouteDecision(t, b, cfgA.NodeID, mesh.TrafficControlQuery, cfgA.NodeID, mesh.TransportTCPMTLS)
	status := b.meshStatusSnapshot()
	if status.TCPMTLS.DialAttempts != 0 || status.TCPMTLS.ActiveAdjacencies < 1 {
		t.Fatalf("inbound-only TCP observability: %+v", status.TCPMTLS)
	}
}

func TestTCPMTLSStartupFailureClosesRuntimeWithSeeds(t *testing.T) {
	ca := newTCPTestCA(t)
	cfg := ca.config(t, 1, nil)
	cfg.NodeID = 2
	tcp := NewTCPMTLSMeshTransportAdapter(cfg)
	r, err := mesh.NewRuntime(mesh.RuntimeOptions{LocalNodeID: 2, Adapters: []mesh.TransportAdapter{tcp}, DialSeeds: []mesh.DialSeed{{Transport: mesh.TransportTCPMTLS, Endpoint: "tcp+tls://127.0.0.1:1/3"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "local certificate node ID mismatch") {
		t.Fatalf("startup accepted wrong certificate: %v", err)
	}
	done := make(chan struct{})
	go func() { _ = r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed startup leaked dial seed WaitGroup")
	}
}

func TestTCPMTLSEndpointValidation(t *testing.T) {
	for _, endpoint := range []string{"tcp+tls://host:443/2", "tcp+tls://[::1]:443/2"} {
		if _, _, err := parseTCPMTLSEndpoint(endpoint); err != nil {
			t.Fatal(err)
		}
	}
	for _, endpoint := range []string{"tcp://host:443/2", "tcp+tls://host/2", "tcp+tls://host:0/2", "tcp+tls://host:65536/2", "tcp+tls://host:443/0", "tcp+tls://host:443/02", "tcp+tls://host:443/2?x=1", "tcp+tls://host:443/2?", "tcp+tls://user@host:443/2", "tcp+tls://0.0.0.0:443/2", "tcp+tls://host:443/2#x", "tcp+tls://host:443/%32"} {
		t.Run(endpoint, func(t *testing.T) {
			if _, _, err := parseTCPMTLSEndpoint(endpoint); err == nil {
				t.Fatal("invalid endpoint accepted")
			}
		})
	}
}

func TestMeshTCPCongestionAndInfoExposed(t *testing.T) {
	ca := newTCPTestCA(t)
	cfgA := ca.config(t, testNodeID(1), nil)
	cfgB := ca.config(t, testNodeID(2), nil)
	ids := []int64{testNodeID(1), testNodeID(2)}
	cfgA.TCPMTLS.AllowedNodeIDs = ids
	cfgB.TCPMTLS.AllowedNodeIDs = ids
	// reno 总是内置且对非特权进程开放，测试不依赖 bbr 模块。
	cfgA.TCPCongestionControl = "reno"
	cfgB.TCPCongestionControl = "reno"
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserve.Addr().String()
	_ = reserve.Close()
	endpoint := fmt.Sprintf("tcp+tls://%s/%d", addr, cfgB.NodeID)
	cfgB.TCPMTLS.ListenAddr = addr
	cfgB.TCPMTLS.AdvertisedEndpoints = []string{endpoint}
	b, err := NewManager(cfgB, newReplicationTestStore(t, "tcp-info-b", 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(b.Handler())
	defer server.Close()
	cfgA.Peers = []Peer{{URL: "wss" + strings.TrimPrefix(server.URL, "https") + WebSocketPath}, {URL: endpoint}}
	a, err := NewManager(cfgA, newReplicationTestStore(t, "tcp-info-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	a.websocket.dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	find := func(m *Manager, transport string, inbound bool) *app.ClusterMeshAdjacency {
		for _, adj := range m.meshStatusSnapshot().Adjacencies {
			if adj.Transport == transport && adj.Inbound == inbound && adj.Established {
				return &adj
			}
		}
		return nil
	}
	waitFor(t, 5*time.Second, func() bool {
		return find(a, "tcp_mtls", false) != nil && find(a, "websocket", false) != nil && find(b, "tcp_mtls", true) != nil && find(b, "websocket", true) != nil
	})
	for _, c := range []struct {
		name string
		adj  *app.ClusterMeshAdjacency
	}{{"dialed tcp", find(a, "tcp_mtls", false)}, {"accepted tcp", find(b, "tcp_mtls", true)}, {"dialed wss", find(a, "websocket", false)}} {
		if c.adj.TCP == nil || c.adj.TCP.Congestion != "reno" || c.adj.TCP.SegsOut == 0 || c.adj.TCP.SndMSS == 0 {
			t.Fatalf("%s TCP info: %+v", c.name, c.adj.TCP)
		}
	}
	// 入站 WSS 可能经本机代理接入，socket 只反映本地一跳，不输出。
	if adj := find(b, "websocket", true); adj.TCP != nil {
		t.Fatalf("inbound WSS exposed TCP info: %+v", adj.TCP)
	}
}

func TestValidTCPCongestionName(t *testing.T) {
	for name, want := range map[string]bool{"": true, "bbr": true, "cubic": true, "bbr_2": true, "BBR": false, "bbr;rm": false, "a234567890123456": false} {
		if got := validTCPCongestionName(name); got != want {
			t.Fatalf("validTCPCongestionName(%q)=%v want %v", name, got, want)
		}
	}
}
