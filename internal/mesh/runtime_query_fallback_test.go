package mesh

import (
	"context"
	"testing"
	"time"
)

type queryFallbackHarness struct {
	r                      *Runtime
	native, backup         *Adjacency
	nativePeer, backupPeer *fakeConn
	snapshot               TopologySnapshot
	base                   RouteDecision
}

func newQueryFallbackHarness(t *testing.T) *queryFallbackHarness {
	t.Helper()
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS), func(o *RuntimeOptions) { o.Adapters = append(o.Adapters, newFakeAdapter(TransportWebSocket)) })
	t.Cleanup(func() { _ = r.Close() })
	a, ap := newFakeConnPair(TransportTCPMTLS, "tcp", "tcp-peer")
	b, bp := newFakeConnPair(TransportWebSocket, "ws", "ws-peer")
	h := &queryFallbackHarness{r: r, nativePeer: ap, backupPeer: bp}
	h.native = r.registerAdjacency(a, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	h.backup = r.registerAdjacency(b, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	h.snapshot = TopologySnapshot{TopologyGeneration: 71, Nodes: map[int64]NodeState{
		1: {NodeID: 1, TransportCaps: map[TransportKind]*TransportCapability{
			TransportTCPMTLS: {Transport: TransportTCPMTLS, OutboundEnabled: true}, TransportWebSocket: {Transport: TransportWebSocket, OutboundEnabled: true},
		}}, 2: {NodeID: 2},
	}, Links: []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportTCPMTLS, PathClass: PathClassDirect, CostMs: 500, Established: true},
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 100, Established: true},
	}}
	h.base, _ = NewPlanner(1).Compute(h.snapshot, 2, TrafficControlQuery, TransportUnspecified)
	return h
}

func (h *queryFallbackHarness) sample(now time.Time, nativeCost float64, increment bool) {
	for _, adj := range []*Adjacency{h.native, h.backup} {
		adj.mu.Lock()
		if adj.samples < 8 {
			adj.samples = 8
		}
		if increment {
			adj.samples++
		}
		adj.lastProbeSuccess = now
		adj.rttEWMA = 100
		adj.jitterEWMA = 0
		if adj == h.native {
			adj.rttEWMA = nativeCost
		}
		adj.mu.Unlock()
	}
	h.r.refreshControlQueryFallback(2, now)
}

func (h *queryFallbackHarness) route(now time.Time) RouteDecision {
	return h.r.controlQueryRoute(h.snapshot, 2, h.base, now)
}

func (h *queryFallbackHarness) activate(now time.Time) {
	h.sample(now.Add(-12*time.Second), 500, true)
	h.sample(now.Add(-6*time.Second), 500, true)
	h.sample(now, 500, true)
}

func TestControlQueryFallbackRequiresSustainedSamplesAndRecoversWithHysteresis(t *testing.T) {
	h := newQueryFallbackHarness(t)
	now := time.Now()
	h.sample(now, 500, true)
	// Repeated route calls and elapsed time do not turn one bad probe into
	// evidence of a persistently poor native connection.
	for i := 0; i < 50; i++ {
		if h.route(now.Add(time.Duration(i)*time.Millisecond)).OutboundTransport != TransportTCPMTLS {
			t.Fatal("single-sample switch")
		}
	}
	h.sample(now.Add(11*time.Second), 500, false)
	if h.route(now.Add(11*time.Second)).OutboundTransport != TransportTCPMTLS {
		t.Fatal("no new probe samples")
	}
	h.sample(now.Add(12*time.Second), 500, true)
	h.sample(now.Add(13*time.Second), 500, true)
	if got := h.route(now.Add(13 * time.Second)); got.OutboundTransport != TransportWebSocket || got.NextHopNodeID != 2 || got.PathClass != PathClassDirect || got.TopologyGeneration != 71 {
		t.Fatalf("no healthy backup: %+v", got)
	}
	// Moderate improvement remains on the backup; recovery must reach the
	// tighter threshold and persist over multiple new native samples.
	h.sample(now.Add(14*time.Second), 180, true)
	if h.route(now.Add(14*time.Second)).OutboundTransport != TransportWebSocket {
		t.Fatal("flapped at intermediate quality")
	}
	h.sample(now.Add(15*time.Second), 120, true)
	h.sample(now.Add(20*time.Second), 120, true)
	if h.route(now.Add(20*time.Second)).OutboundTransport != TransportWebSocket {
		t.Fatal("recovered too early")
	}
	h.sample(now.Add(26*time.Second), 120, true)
	if h.route(now.Add(26*time.Second)).OutboundTransport != TransportTCPMTLS {
		t.Fatal("did not recover native preference")
	}
}

func TestControlQueryFallbackRejectsUnhealthyOrUnknownBackup(t *testing.T) {
	for _, condition := range []string{"few_samples", "stale", "jitter", "failures", "probe_degraded", "pending_probe", "disconnected"} {
		t.Run(condition, func(t *testing.T) {
			h := newQueryFallbackHarness(t)
			now := time.Now()
			h.activate(now)
			h.backup.mu.Lock()
			switch condition {
			case "few_samples":
				h.backup.samples = 1
			case "stale":
				h.backup.lastProbeSuccess = now.Add(-20 * time.Second)
			case "jitter":
				h.backup.jitterEWMA = 100
			case "failures":
				h.backup.sendFailures = 1
			case "probe_degraded":
				h.backup.probeDegraded = true
			case "pending_probe":
				h.backup.pingStarted = now.Add(-7 * time.Second)
			case "disconnected":
				h.backup.established = false
			}
			h.backup.mu.Unlock()
			if h.route(now).OutboundTransport != TransportTCPMTLS {
				t.Fatal("selected unhealthy backup")
			}
		})
	}
}

func TestControlQueryFallbackResetsInterruptedDegradation(t *testing.T) {
	h := newQueryFallbackHarness(t)
	now := time.Now()
	for i, cost := range []float64{500, 500, 120, 500, 500} {
		at := now.Add(time.Duration(i) * 4 * time.Second)
		h.sample(at, cost, true)
		if h.route(at).OutboundTransport != TransportTCPMTLS {
			t.Fatal("non-continuous poor samples triggered fallback")
		}
	}
}

func TestControlQueryFallbackPreservesNativePreferenceAndOtherTraffic(t *testing.T) {
	h := newQueryFallbackHarness(t)
	now := time.Now()
	for i := 0; i < 4; i++ {
		h.sample(now.Add(time.Duration(i)*5*time.Second), 180, true)
	}
	if h.route(now.Add(15*time.Second)).OutboundTransport != TransportTCPMTLS {
		t.Fatal("healthy native preference changed")
	}
	h.activate(time.Now())
	for _, class := range []TrafficClass{TrafficControlCritical, TrafficReplicationStream, TrafficSnapshotBulk, TrafficPointToPointStream, TrafficConsensus} {
		got, ok := h.r.planner.Compute(h.snapshot, 2, class, TransportUnspecified)
		if !ok || got.OutboundTransport != TransportTCPMTLS {
			t.Fatalf("class %v changed route: %+v", class, got)
		}
	}
	got, ok := h.r.planner.Compute(h.snapshot, 2, TrafficControlQuery, TransportTCPMTLS)
	want, wantOK := NewPlanner(1).Compute(h.snapshot, 2, TrafficControlQuery, TransportTCPMTLS)
	if ok != wantOK || got != want {
		t.Fatal("transit query differed from ordinary planner")
	}
	if got := h.r.bestDirectStreamAdjacencyLocked(2, TransportUnspecified); got != h.native {
		t.Fatal("stream pin preference changed")
	}
}

func TestControlQueryFallbackUsesPublishedPermittedDirectBackup(t *testing.T) {
	for _, condition := range []string{"disabled", "missing_link", "unestablished", "relay"} {
		t.Run(condition, func(t *testing.T) {
			h := newQueryFallbackHarness(t)
			now := time.Now()
			h.activate(now)
			switch condition {
			case "disabled":
				h.snapshot.Nodes[1].TransportCaps[TransportWebSocket].OutboundEnabled = false
			case "missing_link":
				h.snapshot.Links = h.snapshot.Links[:1]
			case "unestablished":
				h.snapshot.Links[1].Established = false
			case "relay":
				h.snapshot.Links[1].PathClass = PathClassNativeRelay
			}
			if h.route(now).OutboundTransport != TransportTCPMTLS {
				t.Fatal("bypassed snapshot capability/link constraints")
			}
		})
	}
}

func TestControlQueryFallbackAffectsActualQuerySendAndResetsOnDisconnect(t *testing.T) {
	h := newQueryFallbackHarness(t)
	now := time.Now()
	h.activate(now)
	caps := []*TransportCapability{{Transport: TransportTCPMTLS, OutboundEnabled: true}, {Transport: TransportWebSocket, OutboundEnabled: true}}
	h.r.store.ApplyHello(2, &NodeHello{NodeId: 2, Transports: caps})
	h.r.store.ApplyTopologyUpdate(&TopologyUpdate{OriginNodeId: 1, Generation: 71, Transports: caps, Links: []*LinkAdvertisement{
		{FromNodeId: 1, ToNodeId: 2, Transport: TransportTCPMTLS, PathClass: PathClassDirect, CostMs: 500, Established: true},
		{FromNodeId: 1, ToNodeId: 2, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 100, Established: true},
	}})
	if err := h.r.engine.Forward(context.Background(), &ForwardedPacket{SourceNodeId: 1, TargetNodeId: 2, PacketId: 1, TtlHops: 8, TrafficClass: TrafficControlQuery, Payload: []byte("query")}); err != nil {
		t.Fatal(err)
	}
	if len(h.backupPeer.inbox) != 1 || len(h.nativePeer.inbox) != 0 {
		t.Fatal("actual query did not use backup")
	}
	h.r.onAdjacencyLost(h.backup)
	if len(h.r.queryFallbacks) != 0 {
		t.Fatal("connection-specific fallback state leaked")
	}
	if got, ok := h.r.DescribeRoute(2, TrafficControlQuery); !ok || got.OutboundTransport != TransportTCPMTLS {
		t.Fatalf("did not reset: %+v", got)
	}
}
