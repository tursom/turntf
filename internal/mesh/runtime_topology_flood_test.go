package mesh

import (
	"context"
	"testing"
	"time"
)

// This peer accepts no writes until released, simulating a congested outbound
// socket while the ingress socket still has readable control/data packets.
type stalledTopologyConn struct {
	TransportConn
	entered chan struct{}
	release chan struct{}
}

func (c *stalledTopologyConn) Send(ctx context.Context, p []byte) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
		return c.TransportConn.Send(ctx, p)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestTopologyFloodDoesNotBlockIngress(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, _ := newFakeConnPair(TransportWebSocket, "a", "b")
	slow := &stalledTopologyConn{TransportConn: a, entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer close(slow.release)
	r.registerAdjacency(slow, TransportWebSocket, &NodeHello{NodeId: 3}, false)
	fast, fastPeer := newFakeConnPair(TransportWebSocket, "fast", "peer")
	r.registerAdjacency(fast, TransportWebSocket, &NodeHello{NodeId: 4}, false)
	done := make(chan struct{})
	go func() {
		r.handleTopologyUpdate(context.Background(), nil, &TopologyUpdate{OriginNodeId: 2, Generation: 1})
		close(done)
	}()
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("flood was not sent")
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("slow outbound flood blocks ingress dispatch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := fastPeer.Receive(ctx); err != nil {
		t.Fatalf("slow neighbor blocked healthy neighbor: %v", err)
	}
}

func TestTopologyFloodCoalescesPendingVersions(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, b := newFakeConnPair(TransportWebSocket, "a", "b")
	slow := &stalledTopologyConn{TransportConn: a, entered: make(chan struct{}, 1), release: make(chan struct{})}
	adj := r.registerAdjacency(slow, TransportWebSocket, &NodeHello{NodeId: 3}, false)
	r.queueTopologyFlood(slow, &TopologyUpdate{OriginNodeId: 2, Generation: 1})
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("writer not started")
	}
	for gen := uint64(2); gen <= 100; gen++ {
		r.queueTopologyFlood(slow, &TopologyUpdate{OriginNodeId: 2, Generation: gen})
	}
	r.mu.Lock()
	pending := len(adj.pendingTopology)
	latest := adj.pendingTopology[2].Generation
	r.mu.Unlock()
	if pending != 1 || latest != 100 {
		t.Errorf("unbounded/stale queue: %d, %d", pending, latest)
	}
	close(slow.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, want := range []uint64{1, 100} {
		raw, err := b.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		env, err := r.codec.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := env.GetTopologyUpdate().Generation; got != want {
			t.Fatalf("generation %d, want %d", got, want)
		}
	}
}

func TestTopologyFloodStopsWithRuntime(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	r.ctx, r.cancel = context.WithCancel(context.Background())
	a, _ := newFakeConnPair(TransportWebSocket, "a", "b")
	slow := &stalledTopologyConn{TransportConn: a, entered: make(chan struct{}, 1), release: make(chan struct{})}
	r.registerAdjacency(slow, TransportWebSocket, &NodeHello{NodeId: 3}, false)
	r.queueTopologyFlood(slow, &TopologyUpdate{OriginNodeId: 2, Generation: 1})
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("writer not started")
	}
	done := make(chan struct{})
	go func() { _ = r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close did not stop flood worker")
	}
}
