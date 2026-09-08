package mesh

import (
	"context"
	"testing"
	"time"
)

func TestTCPMTLSPingTimeoutBoundsPendingAndCleansAdjacency(t *testing.T) {
	for _, kind := range []TransportKind{TransportTCPMTLS, TransportWebSocket} {
		t.Run(kind.String(), func(t *testing.T) {
			r, err := NewRuntime(RuntimeOptions{LocalNodeID: 1, Adapters: []TransportAdapter{newFakeAdapter(kind)}, PingInterval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			c, peer := newFakeConnPair(kind, "a", "b")
			defer c.Close()
			adj := r.registerAdjacency(c, kind, &NodeHello{NodeId: 2}, false)
			for i := 0; i < 100; i++ {
				r.sendPing(context.Background(), adj)
			}
			if len(adj.inflightPings) != 1 || len(peer.inbox) != 1 {
				t.Fatalf("unbounded probes: %d queued %d", len(adj.inflightPings), len(peer.inbox))
			}
			r.handleTimeSyncResponse(adj, &TimeSyncResponse{RequestId: 99999})
			if len(adj.inflightPings) != 1 {
				t.Fatal("unmatched response cleared probe")
			}
			time.Sleep(40 * time.Millisecond)
			r.sendPing(context.Background(), adj)
			if kind == TransportTCPMTLS {
				select {
				case <-c.closeCh:
				default:
					t.Fatal("silent TCP remained established")
				}
				if len(adj.inflightPings) != 0 {
					t.Fatal("expired TCP ping retained")
				}
			} else {
				select {
				case <-c.closeCh:
					t.Fatal("changed legacy transport liveness")
				default:
				}
				if len(adj.inflightPings) != 1 {
					t.Fatal("legacy probe not renewed")
				}
			}
			r.onAdjacencyLost(adj)
			if len(adj.inflightPings) != 0 || adj.established || len(r.Adjacencies()) != 0 {
				t.Fatal("lost adjacency retained measurement state")
			}
		})
	}
}
