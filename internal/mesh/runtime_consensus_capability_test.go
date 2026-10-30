package mesh

import "testing"

func TestConsensusBinaryCapabilityRequiresAllDirectConnections(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, _ := newFakeConnPair(TransportWebSocket, "new", "new-peer")
	b, _ := newFakeConnPair(TransportWebSocket, "old", "old-peer")
	first := r.registerAdjacency(a, TransportWebSocket, &NodeHello{NodeId: 2, ConsensusBinaryV1Supported: true}, false)
	if !r.SupportsConsensusBinaryV1(2) || r.SupportsConsensusBinaryV1(3) {
		t.Fatal("direct/unknown capability incorrect")
	}
	second := r.registerAdjacency(b, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	if r.SupportsConsensusBinaryV1(2) {
		t.Fatal("mixed connections must use JSON")
	}
	r.onAdjacencyLost(second)
	if !r.SupportsConsensusBinaryV1(2) {
		t.Fatal("remaining new connection must support binary")
	}
	r.onAdjacencyLost(first)
	if r.SupportsConsensusBinaryV1(2) {
		t.Fatal("disconnected peer must use JSON")
	}
}
