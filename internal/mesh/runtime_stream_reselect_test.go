package mesh

import (
	"context"
	"testing"
	"time"
)

const testReselectStreamID = "0123456789abcdef"

func testStreamFrame(kind uint32, epoch, offset uint64, payload string) *StreamFrame {
	return &StreamFrame{StreamId: []byte(testReselectStreamID), Kind: kind, Epoch: epoch, Offset: offset, Payload: []byte(payload)}
}

type reselectHarness struct {
	t                 *testing.T
	runtime           *Runtime
	primary           *Adjacency
	alternate         *Adjacency
	primarySends      int
	alternateSends    int
	lastSendAlternate bool
}

// newReselectHarness pins streams to primary first: two parallel WebSocket
// adjacencies to node 2, the same shape as home and kiwi direct plus CDN.
func newReselectHarness(t *testing.T) *reselectHarness {
	h := &reselectHarness{t: t, runtime: newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))}
	primary, _ := newFakeConnPair(TransportWebSocket, "primary", "target-primary")
	alternate, _ := newFakeConnPair(TransportWebSocket, "alternate", "target-alternate")
	h.primary = registerTestAdjacency(h.runtime, primary, 2, TransportWebSocket)
	h.alternate = registerTestAdjacency(h.runtime, alternate, 2, TransportWebSocket)
	primary.sendHook = func([]byte) error { h.primarySends++; h.lastSendAlternate = false; return nil }
	alternate.sendHook = func([]byte) error { h.alternateSends++; h.lastSendAlternate = true; return nil }
	h.scores(5, 50)
	return h
}

func (h *reselectHarness) scores(primary, alternate float64) {
	setTestAdjacencyScore(h.primary, primary, 0)
	setTestAdjacencyScore(h.alternate, alternate, 0)
}

func (h *reselectHarness) send(frame *StreamFrame) {
	h.t.Helper()
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: frame}}
	if err := h.runtime.RouteEnvelope(context.Background(), 2, envelope); err != nil {
		h.t.Fatalf("route kind %d: %v", frame.Kind, err)
	}
}

func (h *reselectHarness) inbound(frame *StreamFrame) {
	h.runtime.observeInboundStreamFrame(2, frame)
}

func (h *reselectHarness) wantLast(alternate bool, context string) {
	h.t.Helper()
	if h.lastSendAlternate != alternate {
		h.t.Fatalf("%s: last frame used alternate=%v, want %v (primary=%d alternate=%d)", context, h.lastSendAlternate, alternate, h.primarySends, h.alternateSends)
	}
}

func TestRuntimeDirectStreamReselectsCheaperPathWhenQuiescent(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 5, ""))
	h.wantLast(false, "initial data")

	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	h.wantLast(true, "data after full acknowledgement")
	h.send(testStreamFrame(streamFrameKindData, 1, 11, "third"))
	h.wantLast(true, "data after reselection")
}

func TestRuntimeDirectStreamKeepsPathWhileDataInFlight(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	// Only the first frame is acknowledged; "second" could still be overtaken.
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 5, ""))

	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindData, 1, 11, "third"))
	h.wantLast(false, "data while earlier data unacknowledged")
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 16, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 16, "fourth"))
	h.wantLast(true, "data once all data acknowledged")
}

func TestRuntimeDirectStreamWaitsForOpenAckBeforeReselect(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	h.wantLast(false, "data before OpenAck")
}

func TestRuntimeDirectStreamReselectHysteresis(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))

	h.scores(40, 20)
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	h.wantLast(false, "candidate within margin")
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 5, ""))
	h.scores(200, 120)
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	h.wantLast(true, "candidate beyond proportional margin")
}

func TestRuntimeDirectStreamAckMovesWithoutOrdering(t *testing.T) {
	h := newReselectHarness(t)
	// This node is the receiver: it only sends cumulative Acks, which the
	// sender accepts out of order, so each Ack may take the cheapest path.
	h.send(testStreamFrame(streamFrameKindAck, 1, 10, ""))
	h.wantLast(false, "initial ack")
	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindAck, 1, 20, ""))
	h.wantLast(true, "ack after score change")
}

func TestRuntimeDirectStreamResumeWaitsForResumeAck(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))

	h.send(testStreamFrame(streamFrameKindResume, 2, 0, ""))
	h.wantLast(false, "resume selects cheapest path")
	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindData, 2, 0, "first"))
	h.wantLast(false, "retransmit before resume ack")
	// A stale-epoch Ack does not satisfy the resumed epoch.
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 5, ""))
	h.send(testStreamFrame(streamFrameKindData, 2, 5, "second"))
	h.wantLast(false, "data after stale ack")
	h.inbound(testStreamFrame(streamFrameKindAck, 2, 11, ""))
	h.send(testStreamFrame(streamFrameKindData, 2, 11, "third"))
	h.wantLast(true, "data after resumed epoch acknowledged")
}

func TestRuntimeDirectStreamObservesForwardedAck(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))

	ack := &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: testStreamFrame(streamFrameKindAck, 1, 5, "")}}
	payload, err := h.runtime.codec.Encode(ack)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.handleLocalForwardedPacket(context.Background(), &ForwardedPacket{SourceNodeId: 2, TargetNodeId: 1, TrafficClass: TrafficPointToPointStream, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	h.wantLast(true, "data after forwarded ack")
}

func TestRuntimeDirectStreamCloseKeepsLastPath(t *testing.T) {
	h := newReselectHarness(t)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.scores(100, 1)
	h.send(testStreamFrame(streamFrameKindClose, 1, 0, ""))
	h.wantLast(false, "close")
}

func setTestMinRTT(adj *Adjacency, minRTT float64) {
	adj.mu.Lock()
	adj.observeMinRTTLocked(minRTT, time.Now())
	adj.mu.Unlock()
}

func TestAdjacencyMinRTTWindow(t *testing.T) {
	adj := &Adjacency{}
	start := time.Now()
	if _, ok := adj.minRTTLocked(); ok {
		t.Fatal("min RTT reported before any sample")
	}
	adj.observeMinRTTLocked(150, start)
	adj.observeMinRTTLocked(400, start.Add(time.Second))
	if got, _ := adj.minRTTLocked(); got != 150 {
		t.Fatalf("min within window = %v, want 150", got)
	}
	// The previous window still bounds the estimate during the next window.
	adj.observeMinRTTLocked(500, start.Add(adjacencyMinRTTWindow))
	if got, _ := adj.minRTTLocked(); got != 150 {
		t.Fatalf("min across adjacent windows = %v, want 150", got)
	}
	// Two windows later the old minimum expires.
	adj.observeMinRTTLocked(450, start.Add(2*adjacencyMinRTTWindow))
	if got, _ := adj.minRTTLocked(); got != 450 {
		t.Fatalf("min after expiry = %v, want 450", got)
	}
	// A gap longer than a window does not resurrect a stale minimum.
	adj.observeMinRTTLocked(600, start.Add(5*adjacencyMinRTTWindow))
	if got, _ := adj.minRTTLocked(); got != 600 {
		t.Fatalf("min after idle gap = %v, want 600", got)
	}
}

func TestRuntimeDirectStreamPrefersBasePathOverLoadedRTT(t *testing.T) {
	h := newReselectHarness(t)
	// primary carries bulk traffic: its pings queue behind data (EWMA 400ms)
	// but its path delay is 140ms; the idle alternate is a slower 200ms path.
	h.scores(400, 210)
	setTestMinRTT(h.primary, 140)
	setTestMinRTT(h.alternate, 200)
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.wantLast(false, "open")
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 5, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	h.wantLast(false, "quiescent data on loaded but shorter path")
}

func TestRuntimeDirectStreamScoreKeepsFailurePenalty(t *testing.T) {
	h := newReselectHarness(t)
	setTestMinRTT(h.primary, 140)
	setTestMinRTT(h.alternate, 200)
	h.primary.mu.Lock()
	h.primary.sendFailures = 4
	h.primary.mu.Unlock()
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.wantLast(true, "open avoids failing path despite lower min RTT")
}

// probeAdjacency runs one real ping/pong through sendPing and
// handleTimeSyncResponse with the given RTT and bytes moved meanwhile.
func probeAdjacency(t *testing.T, r *Runtime, adj *Adjacency, rtt time.Duration, moved uint64) {
	t.Helper()
	r.sendPing(context.Background(), adj)
	adj.mu.Lock()
	var id uint64
	for pending := range adj.inflightPings {
		id = pending
	}
	start := time.UnixMilli(time.Now().Add(-rtt).UnixMilli())
	adj.inflightPings[id] = start
	adj.mu.Unlock()
	if id == 0 {
		t.Fatal("ping was not recorded")
	}
	adj.activity.Add(moved)
	r.handleTimeSyncResponse(adj, &TimeSyncResponse{RequestId: id, ClientSendTimeMs: start.UnixMilli(), ServerReceiveTimeMs: start.UnixMilli() + 1, ServerSendTimeMs: start.UnixMilli() + 1})
}

func spikeRate(adj *Adjacency) float64 {
	adj.mu.Lock()
	defer adj.mu.Unlock()
	return adj.spikeEWMA
}

func TestRuntimeIdleProbeSpikesTrackLossButIgnoreOwnLoad(t *testing.T) {
	h := newReselectHarness(t)
	setTestMinRTT(h.primary, 150)

	probeAdjacency(t, h.runtime, h.primary, 160*time.Millisecond, 0)
	if got := spikeRate(h.primary); got != 0 {
		t.Fatalf("near-minimum idle probe counted as spike: %v", got)
	}
	// A slow probe while the adjacency carried bulk data measures its own
	// queue, not the path, and must not count as loss.
	probeAdjacency(t, h.runtime, h.primary, 600*time.Millisecond, 1<<20)
	if got := spikeRate(h.primary); got != 0 {
		t.Fatalf("busy probe counted as spike: %v", got)
	}
	probeAdjacency(t, h.runtime, h.primary, 600*time.Millisecond, 0)
	if got := spikeRate(h.primary); got < 0.099 || got > 0.101 {
		t.Fatalf("idle slow probe spike rate = %v, want 0.1", got)
	}
}

func TestRuntimeDirectStreamAvoidsLossyLowLatencyPath(t *testing.T) {
	for _, test := range []struct {
		name          string
		spike         float64
		wantAlternate bool
	}{
		{name: "lossy direct", spike: 0.3, wantAlternate: true},
		{name: "clean direct", spike: 0.02, wantAlternate: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newReselectHarness(t)
			// primary: 150ms but losing packets; alternate: slower 200ms CDN.
			setTestMinRTT(h.primary, 150)
			setTestMinRTT(h.alternate, 200)
			h.primary.mu.Lock()
			h.primary.spikeEWMA = test.spike
			h.primary.mu.Unlock()
			h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
			h.wantLast(test.wantAlternate, "open")
		})
	}
}
