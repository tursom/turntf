package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
)

func TestTCPMTLSDiscoverySaturatedDiversity(t *testing.T) {
	for _, initial := range []string{"wss", "tcp"} {
		t.Run(initial, func(t *testing.T) {
			m := newDiscoveryTestManager(t, nil)
			m.cfg.TCPMTLS.Enabled = true
			m.cfg.TCPMTLS.AllowedNodeIDs = []int64{2, 3, 4}
			binding, err := m.BuildMeshRuntime()
			if err != nil {
				t.Fatal(err)
			}
			m.meshRuntime = binding
			defer binding.Close()
			add := func(id int64, kind string, port int) string {
				u := fmt.Sprintf("wss://localhost:%d/internal/cluster/ws", port)
				if kind == "tcp" {
					u = fmt.Sprintf("tcp+tls://localhost:%d/%d", port, id)
				}
				m.discoveredPeers[u] = &discoveredPeerState{nodeID: id, url: u, state: discoveryStateCandidate, lastSeenAt: time.Now()}
				return u
			}
			for i := 0; i < 8; i++ {
				add(2, initial, 10000+i)
			}
			m.reconcileDiscoveredDialers()
			if len(m.dynamicPeers) != 8 {
				t.Fatalf("initial saturation: %d", len(m.dynamicPeers))
			}
			other := "tcp"
			if initial == "tcp" {
				other = "wss"
			}
			wanted := []string{add(2, other, 11000), add(3, "tcp", 11001), add(3, "wss", 11002), add(4, "wss", 11003)}
			forbidden := add(99, "tcp", 11004)
			for i := 0; i < 3; i++ {
				m.reconcileDiscoveredDialers()
				if len(m.dynamicPeers) != 8 {
					t.Fatalf("budget: %d", len(m.dynamicPeers))
				}
				for _, u := range wanted {
					if m.dynamicPeers[u] == nil {
						t.Fatalf("saturated %s starved %s", initial, u)
					}
				}
				if m.dynamicPeers[forbidden] != nil {
					t.Fatal("whitelist bypassed")
				}
			}
			m.cfg.TCPMTLS.Enabled = false
			m.reconcileDiscoveredDialers()
			for u := range m.dynamicPeers {
				if transportKindForPeerURL(u) == mesh.TransportTCPMTLS {
					t.Fatal("disabled TCP retained")
				}
			}
		})
	}
}
