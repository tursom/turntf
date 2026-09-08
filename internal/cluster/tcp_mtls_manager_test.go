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
