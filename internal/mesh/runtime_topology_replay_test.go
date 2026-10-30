package mesh

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Replay the same measurements and publisher events without public-network
// noise. Each group represents the probe responses received in one five-second
// window; writers drain between events, so mailbox congestion cannot create an
// artificial bandwidth improvement. The test also runs unchanged on the old
// implementation to quantify the effect of immediate measurement flooding.
func TestTopologyTrafficProbeReplay(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	defer r.Close()
	var frames, bytes atomic.Int64
	var adjacencies []*Adjacency
	var peers []*fakeConn
	for peer := int64(2); peer <= 6; peer++ {
		for parallel := 0; parallel < 2; parallel++ {
			conn, receiver := newFakeConnPair(TransportTCPMTLS, "source", "target")
			peers = append(peers, receiver)
			conn.sendHook = func(raw []byte) error {
				frames.Add(1)
				bytes.Add(int64(len(raw)))
				return nil
			}
			adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: peer}, false)
			setTestAdjacencyScore(adj, 100, 0)
			adjacencies = append(adjacencies, adj)
		}
	}
	drainFloods := func() {
		r.wg.Wait()
		for _, peer := range peers {
			for len(peer.inbox) > 0 {
				<-peer.inbox
			}
		}
	}
	r.publishLocalTopology(context.Background())
	drainFloods()
	frames.Store(0)
	bytes.Store(0)
	const windows, probesPerWindow = 6, 80
	for window := 0; window < windows; window++ {
		for sample := 0; sample < probesPerWindow; sample++ {
			adj := adjacencies[sample%len(adjacencies)]
			id := uint64(window*probesPerWindow + sample + 1)
			rtt := time.Duration([]int{100, 300, 800, 200, 600}[(sample/len(adjacencies)+window)%5]) * time.Millisecond
			start := time.Now().Add(-rtt).Truncate(time.Millisecond)
			adj.mu.Lock()
			adj.inflightPings[id] = start
			adj.mu.Unlock()
			r.handleTimeSyncResponse(adj, &TimeSyncResponse{RequestId: id,
				ClientSendTimeMs: start.UnixMilli(), ServerReceiveTimeMs: start.UnixMilli(), ServerSendTimeMs: start.UnixMilli()})
			// Service an immediate signal just as topologyPublishLoop does.
			select {
			case <-r.qualitySignal:
				r.publishQualityIfChanged(context.Background())
			default:
			}
			drainFloods()
		}
		// Service the quality window tick, keeping the same event schedule
		// for the baseline and optimized implementations.
		r.publishQualityIfChanged(context.Background())
		drainFloods()
	}
	t.Logf("probe_responses=%d quality_windows=%d connections=%d topology_frames=%d topology_bytes=%d",
		windows*probesPerWindow, windows, len(adjacencies), frames.Load(), bytes.Load())
	if frames.Load() > int64(windows*len(adjacencies)) {
		t.Fatal("ordinary quality announcements exceeded one batch per window")
	}
}
