package mesh

import (
	"context"
	"testing"
	"time"
)

// Each wire serializes writes. Holding DATA models a congested stream without
// introducing wall-clock sleeps or changing the query deadline.
type congestedTestConn struct {
	TransportConn
	codec   EnvelopeCodec
	lock    chan struct{}
	entered chan struct{}
	release chan struct{}
}

func (c *congestedTestConn) Send(ctx context.Context, data []byte) error {
	select {
	case c.lock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.lock }()
	env, err := c.codec.Decode(data)
	if err != nil {
		return err
	}
	if p := env.GetForwardedPacket(); p != nil && p.TrafficClass == TrafficTransientInteractive {
		close(c.entered)
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func TestRuntimeTrafficAdjacencyFallback(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	a, _ := newFakeConnPair(TransportWebSocket, "a", "b")
	b, _ := newFakeConnPair(TransportWebSocket, "a2", "b2")
	first := r.registerAdjacency(a, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	first.rttEWMA = 10
	for _, traffic := range []TrafficClass{TrafficControlCritical, TrafficControlQuery, TrafficTransientInteractive, TrafficReplicationStream, TrafficSnapshotBulk} {
		if got := r.bestAdjacencyForTraffic(2, TransportWebSocket, traffic); got != first {
			t.Fatal("single-connection fallback changed")
		}
	}
	second := r.registerAdjacency(b, TransportWebSocket, &NodeHello{NodeId: 2}, true)
	second.rttEWMA = 20
	for _, traffic := range []TrafficClass{TrafficControlCritical, TrafficControlQuery, TrafficClassUnspecified} {
		if got := r.bestAdjacencyForTraffic(2, TransportWebSocket, traffic); got != first {
			t.Fatal("control lost best connection")
		}
	}
	for _, traffic := range []TrafficClass{TrafficTransientInteractive, TrafficReplicationStream, TrafficSnapshotBulk} {
		if got := r.bestAdjacencyForTraffic(2, TransportWebSocket, traffic); got != second {
			t.Fatal("DATA shared control connection")
		}
	}
	first.established = false
	if r.bestAdjacencyForTraffic(2, TransportWebSocket, TrafficControlQuery) != second || r.bestAdjacencyForTraffic(2, TransportWebSocket, TrafficTransientInteractive) != second {
		t.Fatal("failed connection retained")
	}
	if r.bestAdjacencyForTraffic(99, TransportWebSocket, TrafficTransientInteractive) != nil || r.bestAdjacencyForTraffic(2, TransportLibP2P, TrafficControlQuery) != nil {
		t.Fatal("crossed route boundary")
	}
}

func TestRuntimeQueryAvoidsCongestedDataAdjacency(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	entered, release := make(chan struct{}), make(chan struct{})
	a, _ := newFakeConnPair(TransportWebSocket, "a", "b")
	b, _ := newFakeConnPair(TransportWebSocket, "a2", "b2")
	for i, base := range []*fakeConn{a, b} {
		wire := &congestedTestConn{TransportConn: base, codec: r.codec, lock: make(chan struct{}, 1), entered: entered, release: release}
		adj := r.registerAdjacency(wire, TransportWebSocket, &NodeHello{NodeId: 2}, i == 1)
		adj.rttEWMA = float64(time.Duration(i+1) * time.Millisecond)
	}
	done := make(chan error, 1)
	go func() {
		done <- r.SendPacket(context.Background(), 2, TransportWebSocket, &ForwardedPacket{SourceNodeId: 1, TargetNodeId: 2, PacketId: 1, TrafficClass: TrafficTransientInteractive, Payload: []byte("body")})
	}()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("DATA did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := r.SendPacket(ctx, 2, TransportWebSocket, &ForwardedPacket{SourceNodeId: 1, TargetNodeId: 2, PacketId: 2, TrafficClass: TrafficControlQuery, Payload: []byte("query")}); err != nil {
		t.Fatalf("query queued behind DATA despite an idle adjacency: %v", err)
	}
}
