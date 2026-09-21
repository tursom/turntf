package mesh

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPingFailureClosesAllTransports(t *testing.T) {
	for _, kind := range []TransportKind{TransportWebSocket, TransportLibP2P, TransportZeroMQ, TransportTCPMTLS} {
		t.Run(kind.String(), func(t *testing.T) {
			t.Run("timeout", func(t *testing.T) {
				r, err := NewRuntime(RuntimeOptions{LocalNodeID: 1, Adapters: []TransportAdapter{newFakeAdapter(kind)}, PingInterval: 10 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				c, peer := newFakeConnPair(kind, "a", "b")
				adj := r.registerAdjacency(c, kind, &NodeHello{NodeId: 2}, false)
				r.sendPing(context.Background(), adj)
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
				adj.mu.Lock()
				adj.pingStarted = time.Now().Add(-4 * r.pingInterval)
				adj.mu.Unlock()
				r.sendPing(context.Background(), adj)
				select {
				case <-c.closeCh:
				default:
					t.Fatal("silent transport remained open")
				}
				if len(adj.inflightPings) != 0 {
					t.Fatal("expired ping retained")
				}
				r.onAdjacencyLost(adj)
				if adj.established || len(r.Adjacencies()) != 0 {
					t.Fatal("lost adjacency retained state")
				}
			})

			t.Run("send failure", func(t *testing.T) {
				r, err := NewRuntime(RuntimeOptions{LocalNodeID: 1, Adapters: []TransportAdapter{newFakeAdapter(kind)}})
				if err != nil {
					t.Fatal(err)
				}
				c, _ := newFakeConnPair(kind, "a", "b")
				c.sendHook = func([]byte) error { return errors.New("injected ping failure") }
				adj := r.registerAdjacency(c, kind, &NodeHello{NodeId: 2}, false)
				r.sendPing(context.Background(), adj)
				select {
				case <-c.closeCh:
				default:
					t.Fatal("send failure left transport open")
				}
				if len(adj.inflightPings) != 0 {
					t.Fatal("failed ping retained")
				}
				r.onAdjacencyLost(adj)
			})
		})
	}
}

type dropTimeSyncRequestConn struct {
	TransportConn
	codec   EnvelopeCodec
	dropped chan struct{}
	once    sync.Once
}

func (c *dropTimeSyncRequestConn) Send(ctx context.Context, data []byte) error {
	envelope, err := c.codec.Decode(data)
	if err == nil && envelope.GetTimeSyncRequest() != nil {
		c.once.Do(func() { close(c.dropped) })
		return nil
	}
	return c.TransportConn.Send(ctx, data)
}

func TestWebSocketHalfOpenAdjacencyClosesAndReconnects(t *testing.T) {
	adapterA := newFakeAdapter(TransportWebSocket)
	adapterB1 := newFakeAdapter(TransportWebSocket)
	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.PingInterval = 5 * time.Millisecond
	})
	runtimeB1 := newTestRuntime(t, 2, adapterB1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, runtime := range []*Runtime{runtimeA, runtimeB1} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatalf("start runtime: %v", err)
		}
		defer runtime.Close()
	}

	connA1, connB1 := newFakeConnPair(TransportWebSocket, "A", "B")
	halfOpen := &dropTimeSyncRequestConn{
		TransportConn: connA1,
		codec:         runtimeA.codec,
		dropped:       make(chan struct{}),
	}
	adapterA.accept <- halfOpen
	adapterB1.accept <- connB1
	waitForAdjacency(t, runtimeA, 1, time.Second)
	oldAdjacency := runtimeA.bestAdjacency(2, TransportWebSocket)
	if oldAdjacency == nil {
		t.Fatal("old WebSocket adjacency was not registered")
	}

	select {
	case <-halfOpen.dropped:
	case <-time.After(time.Second):
		t.Fatal("TimeSyncRequest was not sent and discarded")
	}
	select {
	case <-connA1.closeCh:
	case <-time.After(time.Second):
		t.Fatal("half-open WebSocket was not closed within the liveness bound")
	}
	waitFor(t, time.Second, func() bool { return len(runtimeA.Adjacencies()) == 0 })

	delivered := make(chan *ClusterEnvelope, 1)
	adapterB2 := newFakeAdapter(TransportWebSocket)
	runtimeB2 := newTestRuntime(t, 2, adapterB2, func(opts *RuntimeOptions) {
		opts.QueryHandler = func(_ context.Context, _ *ForwardedPacket, envelope *ClusterEnvelope) error {
			delivered <- envelope
			return nil
		}
	})
	if err := runtimeB2.Start(ctx); err != nil {
		t.Fatalf("start replacement runtime: %v", err)
	}
	defer runtimeB2.Close()
	connA2, connB2 := newFakeConnPair(TransportWebSocket, "A", "B")
	adapterA.accept <- connA2
	adapterB2.accept <- connB2
	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForNodes(t, runtimeA, []int64{1, 2}, time.Second)

	// A delayed cleanup from the old runConn must only affect its own connection.
	runtimeA.onAdjacencyLost(oldAdjacency)
	if got := runtimeA.bestAdjacency(2, TransportWebSocket); got == nil || got.Conn != connA2 {
		t.Fatal("old adjacency cleanup removed the replacement connection")
	}

	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_QueryRequest{QueryRequest: &QueryRequest{
		RequestId: 42,
		Kind:      "liveness.reconnect",
	}}}
	if err := runtimeA.RouteEnvelope(ctx, 2, envelope); err != nil {
		t.Fatalf("route over replacement connection: %v", err)
	}
	select {
	case got := <-delivered:
		if request := got.GetQueryRequest(); request == nil || request.RequestId != 42 {
			t.Fatalf("unexpected routed envelope: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement connection did not deliver routed envelope")
	}
}
