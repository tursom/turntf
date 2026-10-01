package mesh

import (
	"context"
	"testing"
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
