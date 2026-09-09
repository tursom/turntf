package cluster

import (
	"context"
	"testing"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

func TestMeshObservationEndpointMatchesPeerURL(t *testing.T) {
	const peerURL = "ws://home.example/internal/cluster/ws"
	for _, tc := range []struct {
		name     string
		endpoint string
		want     bool
	}{
		{"relative path", websocketPath, false},
		{"different host", "ws://cn.example/internal/cluster/ws", false},
		{"different port", "ws://home.example:9915/internal/cluster/ws", false},
		{"different scheme", "wss://home.example/internal/cluster/ws", false},
		{"absolute URL", peerURL, true},
		{"normalized absolute URL", " WS://HOME.EXAMPLE/internal/cluster/ws ", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := meshObservationEndpointMatchesPeerURL(mesh.TransportWebSocket, tc.endpoint, peerURL); got != tc.want {
				t.Fatalf("match %q with %q = %t, want %t", tc.endpoint, peerURL, got, tc.want)
			}
		})
	}
}

func TestManagerMeshAdjacencyKeepsSamePathPeersDistinct(t *testing.T) {
	for _, kind := range []string{"configured", "dynamic", "discovered"} {
		t.Run(kind, func(t *testing.T) {
			urls := []string{
				"ws://home.example/internal/cluster/ws",
				"ws://cn.example/internal/cluster/ws",
				"ws://kr.example/internal/cluster/ws",
			}
			cfg := Config{
				NodeID: testNodeID(1), AdvertisePath: websocketPath,
				ClusterSecret: "secret", MessageWindowSize: store.DefaultMessageWindowSize,
				MaxClockSkewMs: DefaultMaxClockSkewMs, DiscoveryDisabled: true,
			}
			if kind == "configured" {
				for _, url := range urls {
					cfg.Peers = append(cfg.Peers, Peer{URL: url})
				}
			}
			mgr, err := NewManager(cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			if kind != "configured" {
				for _, url := range urls {
					mgr.discoveredPeers[url] = &discoveredPeerState{url: url, state: discoveryStateDialing, dialing: true}
					if kind == "dynamic" {
						mgr.dynamicPeers[url] = &configuredPeer{URL: url, dynamic: true}
					}
				}
			}
			capability := mgr.buildMeshInboundAdapters()[mesh.TransportWebSocket].LocalCapabilities()
			if len(capability.AdvertisedEndpoints) != 1 || capability.AdvertisedEndpoints[0] != websocketPath {
				t.Fatalf("unexpected advertised endpoints: %v", capability.AdvertisedEndpoints)
			}
			for i, url := range urls {
				nodeID := testNodeID(uint16(i + 2))
				mgr.observeMeshAdjacency(mesh.AdjacencyObservation{
					RemoteNodeID: nodeID, Transport: mesh.TransportWebSocket,
					RemoteHint: url, Established: true,
					Hello: &mesh.NodeHello{NodeId: nodeID, Transports: []*mesh.TransportCapability{capability}},
				})
				if kind == "configured" {
					status, err := mgr.Status(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					for j := 0; j <= i; j++ {
						found := false
						for _, peer := range status.Peers {
							if peer.NodeID == testNodeID(uint16(j+2)) && peer.ConfiguredURL == urls[j] && peer.Transport == transportWebSocket {
								found = true
							}
						}
						if !found {
							t.Errorf("hello %s: status missing correct metadata for %s: %+v", url, urls[j], status.Peers)
						}
					}
				}
				for j, peerURL := range urls {
					var want int64
					if j <= i {
						want = testNodeID(uint16(j + 2))
					}
					var got int64
					switch kind {
					case "configured":
						got = mgr.configuredPeers[j].nodeID
					case "dynamic":
						got = mgr.dynamicPeers[peerURL].nodeID
					case "discovered":
						got = mgr.discoveredPeers[peerURL].nodeID
					}
					if got != want {
						t.Errorf("hello %s: peer %s nodeID = %d, want %d", url, peerURL, got, want)
					}
					if kind != "configured" {
						peer := mgr.discoveredPeers[peerURL]
						wantState := discoveryStateDialing
						if j <= i {
							wantState = discoveryStateConnected
						}
						if peer.nodeID != want || peer.state != wantState || peer.dialing != (j > i) {
							t.Errorf("hello %s: discovered peer %s = %+v, want nodeID %d state %s", url, peerURL, peer, want, wantState)
						}
					}
				}
			}
		})
	}
}
