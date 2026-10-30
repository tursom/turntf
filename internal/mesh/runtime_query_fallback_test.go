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
	h.sampleDelays(now, nativeCost, 0, 100, 0, increment)
}

func (h *queryFallbackHarness) sampleDelays(now time.Time, nativeRTT, nativeJitter, backupRTT, backupJitter float64, increment bool) {
	for _, adj := range []*Adjacency{h.native, h.backup} {
		adj.mu.Lock()
		if adj.samples < 8 {
			adj.samples = 8
		}
		if increment {
			adj.samples++
		}
		adj.lastProbeSuccess = now
		adj.rttEWMA = backupRTT
		adj.jitterEWMA = backupJitter
		if adj == h.native {
			adj.rttEWMA = nativeRTT
			adj.jitterEWMA = nativeJitter
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
				h.backup.jitterEWMA = 500
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

func TestControlQueryFallbackAcceptsBoundedJitterOnBetterBackup(t *testing.T) {
	for _, sample := range []struct {
		name                    string
		nativeRTT, nativeJitter float64
		backupRTT, backupJitter float64
	}{
		{"cc_to_home", 1365, 1251, 193, 68},
		{"home_to_cc", 1107, 1021, 221, 139},
	} {
		t.Run(sample.name, func(t *testing.T) {
			h := newQueryFallbackHarness(t)
			now := time.Now()
			for i := 0; i < 3; i++ {
				at := now.Add(time.Duration(i) * 6 * time.Second)
				h.sampleDelays(at, sample.nativeRTT, sample.nativeJitter, sample.backupRTT, sample.backupJitter, true)
				got := h.route(at)
				if i < 2 && got.OutboundTransport != TransportTCPMTLS {
					t.Fatal("switched before sustained degradation")
				}
			}
			if got := h.route(now.Add(12 * time.Second)); got.OutboundTransport != TransportWebSocket {
				t.Fatalf("bounded, substantially faster backup was rejected: %+v", got)
			}
		})
	}
}

func TestControlQueryFallbackRequiresBudgetAndRelativeAdvantage(t *testing.T) {
	for _, sample := range []struct {
		name                    string
		nativeRTT, nativeJitter float64
		backupRTT, backupJitter float64
	}{
		// Lower latency alone is insufficient when both paths use too much of
		// the query budget, or the improvement does not justify a path change.
		{"slow_backup", 2800, 200, 1100, 0},
		{"excessive_variation", 2800, 200, 100, 500},
		{"small_relative_gain", 600, 0, 350, 50},
	} {
		t.Run(sample.name, func(t *testing.T) {
			h := newQueryFallbackHarness(t)
			now := time.Now()
			for i := 0; i < 4; i++ {
				at := now.Add(time.Duration(i) * 6 * time.Second)
				h.sampleDelays(at, sample.nativeRTT, sample.nativeJitter, sample.backupRTT, sample.backupJitter, true)
				if got := h.route(at); got.OutboundTransport != TransportTCPMTLS {
					t.Fatalf("selected unsuitable backup: %+v", got)
				}
			}
		})
	}
}

func TestControlQueryFallbackBudgetBoundaryAndRecovery(t *testing.T) {
	h := newQueryFallbackHarness(t)
	now := time.Now()
	for i := 0; i < 3; i++ {
		h.sampleDelays(now.Add(time.Duration(i)*6*time.Second), 3000, 0, 900, 50, true)
	}
	if h.route(now.Add(12*time.Second)).OutboundTransport != TransportWebSocket {
		t.Fatal("backup at the budget boundary was rejected")
	}
	h.backup.mu.Lock()
	h.backup.rttEWMA = 901
	h.backup.mu.Unlock()
	if h.route(now.Add(12*time.Second)).OutboundTransport != TransportTCPMTLS {
		t.Fatal("backup above the budget remained active")
	}
	for i := 0; i < 3; i++ {
		h.sampleDelays(now.Add(time.Duration(13+i*6)*time.Second), 3000, 0, 900, 0, true)
	}
	// Native is within 25% of the backup, but still above the absolute budget.
	for i := 0; i < 4; i++ {
		at := now.Add(time.Duration(26+i*6) * time.Second)
		h.sampleDelays(at, 1050, 0, 900, 0, true)
		if h.route(at).OutboundTransport != TransportWebSocket {
			t.Fatal("native recovered above the budget")
		}
	}
	for i := 0; i < 3; i++ {
		at := now.Add(time.Duration(45+i*6) * time.Second)
		h.sampleDelays(at, 1000, 0, 900, 0, true)
	}
	if h.route(now.Add(57*time.Second)).OutboundTransport != TransportTCPMTLS {
		t.Fatal("native did not recover at the budget boundary")
	}
}

func TestControlQueryFallbackSendRechecksNewAndDegradedConnections(t *testing.T) {
	h := newQueryFallbackHarness(t)
	now := time.Now()
	h.activate(now)
	decision := h.route(now)
	newConn, newPeer := newFakeConnPair(TransportWebSocket, "new-ws", "new-ws-peer")
	h.r.registerAdjacency(newConn, TransportWebSocket, &NodeHello{NodeId: 2}, true)
	if h.r.bestAdjacency(2, TransportWebSocket).Conn != newConn {
		t.Fatal("fixture did not prefer the unmeasured connection for ordinary traffic")
	}
	packet := &ForwardedPacket{SourceNodeId: 1, TargetNodeId: 2, PacketId: 1, TtlHops: 8, TrafficClass: TrafficControlQuery}
	if err := h.r.SendPacket(context.Background(), 2, decision.OutboundTransport, packet); err != nil {
		t.Fatal(err)
	}
	if len(h.backupPeer.inbox) != 1 || len(newPeer.inbox) != 0 {
		t.Fatal("unmeasured connection bypassed fallback qualification")
	}
	if h.route(now).OutboundTransport != TransportWebSocket {
		t.Fatal("unmeasured sibling masked the qualified backup")
	}
	h.backup.mu.Lock()
	h.backup.probeDegraded = true
	h.backup.mu.Unlock()
	if err := h.r.SendPacket(context.Background(), 2, decision.OutboundTransport, packet); err != ErrNoRoute {
		t.Fatalf("degraded backup sent after planning: %v", err)
	}
	if len(h.backupPeer.inbox) != 1 || len(newPeer.inbox) != 0 {
		t.Fatal("failed qualification still sent a packet")
	}
	// Qualification must not prevent transit or other classes from using a
	// newly established connection under their existing routing rules.
	packet.SourceNodeId = 3
	if err := h.r.SendPacket(context.Background(), 2, TransportWebSocket, packet); err != nil {
		t.Fatal(err)
	}
	packet.SourceNodeId, packet.TrafficClass = 1, TrafficControlCritical
	if err := h.r.SendPacket(context.Background(), 2, TransportWebSocket, packet); err != nil {
		t.Fatal(err)
	}
	if len(newPeer.inbox) != 2 {
		t.Fatal("ordinary traffic selection changed")
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
