package mesh

import (
	"context"
	"testing"
	"time"
)

// pairHarness: two WebSocket adjacencies from localID to node 2, like home and
// kiwi (direct plus CDN). primary scores better unless a test changes it.
type pairHarness struct {
	*reselectHarness
}

func newPairHarness(t *testing.T, localID int64) *pairHarness {
	h := &reselectHarness{t: t, runtime: newTestRuntime(t, localID, newFakeAdapter(TransportWebSocket))}
	primary, _ := newFakeConnPair(TransportWebSocket, "primary", "target-primary")
	alternate, _ := newFakeConnPair(TransportWebSocket, "alternate", "target-alternate")
	h.primary = registerTestAdjacency(h.runtime, primary, 2, TransportWebSocket)
	h.alternate = registerTestAdjacency(h.runtime, alternate, 2, TransportWebSocket)
	primary.sendHook = func([]byte) error { h.primarySends++; h.lastSendAlternate = false; return nil }
	alternate.sendHook = func([]byte) error { h.alternateSends++; h.lastSendAlternate = true; return nil }
	h.scores(5, 20)
	return &pairHarness{h}
}

// arrive simulates the remote's stream frame arriving on adj (nil: forwarded).
func (h *pairHarness) arrive(adj *Adjacency, id string, kind uint32, offset uint64) {
	h.runtime.observeInboundStreamFrame(2, adj, &StreamFrame{StreamId: []byte(id), Kind: kind, Epoch: 1, Offset: offset, Payload: []byte("data")})
}

func (h *pairHarness) sendID(id string, kind uint32, offset uint64, payload string) {
	h.t.Helper()
	frame := &StreamFrame{StreamId: []byte(id), Kind: kind, Epoch: 1, Offset: offset, Payload: []byte(payload)}
	if err := h.runtime.RouteEnvelope(context.Background(), 2, &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: frame}}); err != nil {
		h.t.Fatalf("route kind %d: %v", kind, err)
	}
}

const remoteStream = "rrrrrrrrrrrrrrrr"
const localStream = "llllllllllllllll"

func TestStreamAcksReturnOnIngressAdjacency(t *testing.T) {
	h := newPairHarness(t, 1)
	h.arrive(h.alternate, remoteStream, streamFrameKindOpen, 0)
	h.sendID(remoteStream, streamFrameKindOpenAck, 0, "")
	h.wantLast(true, "OpenAck returns where Open arrived, not on the cheaper path")

	h.arrive(h.primary, remoteStream, streamFrameKindData, 0)
	h.sendID(remoteStream, streamFrameKindAck, 4, "")
	h.wantLast(false, "Ack follows the sender's move")

	// A forwarded frame says nothing about direct adjacencies.
	h.arrive(nil, remoteStream, streamFrameKindData, 4)
	h.sendID(remoteStream, streamFrameKindAck, 8, "")
	h.wantLast(false, "forwarded Data keeps the last direct ingress")

	h.arrive(h.alternate, remoteStream, streamFrameKindClose, 8)
	h.runtime.mu.Lock()
	_, kept := h.runtime.streamIngress[directStreamAffinityKey{targetNodeID: 2, streamID: remoteStream}]
	h.runtime.mu.Unlock()
	if kept {
		t.Fatal("Close did not clear the stream ingress")
	}
}

func TestStreamAckIngressIgnoresLostAdjacency(t *testing.T) {
	h := newPairHarness(t, 1)
	h.arrive(h.alternate, remoteStream, streamFrameKindData, 0)
	h.alternate.mu.Lock()
	h.alternate.established = false
	h.alternate.mu.Unlock()
	h.sendID(remoteStream, streamFrameKindAck, 4, "")
	h.wantLast(false, "Ack after its ingress adjacency closed")
}

func TestFollowerAlignsStreamWithLeaderIngress(t *testing.T) {
	h := newPairHarness(t, 5) // 5 > 2: node 2 leads
	h.arrive(h.alternate, remoteStream, streamFrameKindData, 0)
	h.sendID(localStream, streamFrameKindOpen, 0, "")
	h.wantLast(true, "Open joins the leader's adjacency within the margin")
}

func TestFollowerKeepsClearlyBetterPath(t *testing.T) {
	h := newPairHarness(t, 5)
	h.scores(5, 200)
	h.arrive(h.alternate, remoteStream, streamFrameKindData, 0)
	h.sendID(localStream, streamFrameKindOpen, 0, "")
	h.wantLast(false, "leader's adjacency is worse by more than the margin")
}

func TestLeaderIgnoresPairIngress(t *testing.T) {
	h := newPairHarness(t, 1) // 1 < 2: this node leads
	h.arrive(h.alternate, remoteStream, streamFrameKindData, 0)
	h.sendID(localStream, streamFrameKindOpen, 0, "")
	h.wantLast(false, "leader ranks adjacencies on its own")
}

func TestFollowerMovesToLeaderAdjacencyWhenQuiescent(t *testing.T) {
	h := newPairHarness(t, 5)
	h.sendID(localStream, streamFrameKindOpen, 0, "")
	h.wantLast(false, "no leader traffic yet")
	h.inbound(&StreamFrame{StreamId: []byte(localStream), Kind: streamFrameKindOpenAck, Epoch: 1})
	h.sendID(localStream, streamFrameKindData, 0, "first")
	h.inbound(&StreamFrame{StreamId: []byte(localStream), Kind: streamFrameKindAck, Epoch: 1, Offset: 5})

	// The leader's stream arrives on alternate, slightly worse but in margin.
	h.arrive(h.alternate, remoteStream, streamFrameKindData, 0)
	h.sendID(localStream, streamFrameKindData, 5, "second")
	h.wantLast(true, "quiescent stream joins the leader's adjacency")

	// While Data is in flight the aligned pin stays put even if primary improves.
	h.scores(1, 20)
	h.sendID(localStream, streamFrameKindData, 11, "third")
	h.wantLast(true, "unacknowledged data keeps the aligned pin")
}

func TestPairIngressExpires(t *testing.T) {
	h := newPairHarness(t, 5)
	h.runtime.mu.Lock()
	h.runtime.pairIngress[2] = pairIngressEntry{adj: h.alternate, at: time.Now().Add(-2 * pairIngressFreshFor)}
	h.runtime.mu.Unlock()
	h.sendID(localStream, streamFrameKindOpen, 0, "")
	h.wantLast(false, "stale leader ingress")
}
