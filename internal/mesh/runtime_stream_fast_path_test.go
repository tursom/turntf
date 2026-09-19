package mesh

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func testStreamEnvelope() *ClusterEnvelope {
	return &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: &StreamFrame{
		StreamId: []byte("0123456789abcdef"),
		Epoch:    1,
		Payload:  []byte("stream-payload"),
	}}}
}

func registerTestAdjacency(runtime *Runtime, conn TransportConn, remoteNodeID int64, transport TransportKind) *Adjacency {
	return runtime.registerAdjacency(conn, transport, &NodeHello{NodeId: remoteNodeID}, false)
}

func receiveTestEnvelope(t testing.TB, conn TransportConn) *ClusterEnvelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	raw, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive envelope: %v", err)
	}
	envelope, err := (protoCodec{}).Decode(raw)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return envelope
}

func TestRuntimeRoutesDirectStreamAsBareEnvelope(t *testing.T) {
	source := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	target := newTestRuntime(t, 2, newFakeAdapter(TransportLibP2P))
	connSource, connTarget := newFakeConnPair(TransportLibP2P, "source", "target")
	registerTestAdjacency(source, connSource, 2, TransportLibP2P)
	targetAdj := registerTestAdjacency(target, connTarget, 1, TransportLibP2P)

	var gotPacket *ForwardedPacket
	var gotEnvelope *ClusterEnvelope
	target.envelopeHandler = func(_ context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error {
		gotPacket = packet
		gotEnvelope = envelope
		return nil
	}

	want := testStreamEnvelope()
	if err := source.RouteEnvelope(context.Background(), 2, want); err != nil {
		t.Fatalf("route direct stream: %v", err)
	}
	wireEnvelope := receiveTestEnvelope(t, connTarget)
	if wireEnvelope.GetStreamFrame() == nil || wireEnvelope.GetForwardedPacket() != nil {
		t.Fatalf("direct stream used unexpected wire envelope: %T", wireEnvelope.Body)
	}

	target.dispatchEnvelope(context.Background(), targetAdj, wireEnvelope)
	if gotEnvelope != wireEnvelope {
		t.Fatal("direct stream did not reach envelope handler")
	}
	if gotPacket == nil || gotPacket.SourceNodeId != 1 || gotPacket.TargetNodeId != 2 ||
		gotPacket.TrafficClass != TrafficPointToPointStream || gotPacket.LastHopNodeId != 1 ||
		gotPacket.IngressTransport != TransportLibP2P {
		t.Fatalf("unexpected direct stream metadata: %+v", gotPacket)
	}
}

func TestRuntimeFallsBackForStreamWithoutEstablishedDirectAdjacency(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connToTransit, transitConn := newFakeConnPair(TransportLibP2P, "source", "transit")
	registerTestAdjacency(runtime, connToTransit, 2, TransportLibP2P)
	connToTarget, _ := newFakeConnPair(TransportLibP2P, "source", "target")
	stale := registerTestAdjacency(runtime, connToTarget, 3, TransportLibP2P)
	stale.mu.Lock()
	stale.established = false
	stale.mu.Unlock()

	for _, nodeID := range []int64{1, 2, 3} {
		applyRuntimeTestNode(runtime, nodeID, DefaultForwardingPolicy(1), TransportLibP2P)
	}
	applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)
	applyRuntimeTestLink(runtime, 2, 3, 1, TransportLibP2P)

	if err := runtime.RouteEnvelope(context.Background(), 3, testStreamEnvelope()); err != nil {
		t.Fatalf("route fallback stream: %v", err)
	}
	wireEnvelope := receiveTestEnvelope(t, transitConn)
	packet := wireEnvelope.GetForwardedPacket()
	if packet == nil || packet.TrafficClass != TrafficPointToPointStream {
		t.Fatalf("fallback did not use stream forwarded packet: %T", wireEnvelope.Body)
	}
	inner, err := runtime.codec.Decode(packet.Payload)
	if err != nil || inner.GetStreamFrame() == nil {
		t.Fatalf("decode fallback stream payload: envelope=%v err=%v", inner, err)
	}
}

func TestRuntimeRoutesStreamAcrossTransitWithForwardedEnvelope(t *testing.T) {
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	adapterC := newFakeAdapter(TransportLibP2P)

	delivered := make(chan *ForwardedPacket, 1)
	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)
	runtimeC := newTestRuntime(t, 3, adapterC, func(opts *RuntimeOptions) {
		opts.EnvelopeHandler = func(_ context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error {
			if envelope.GetStreamFrame() != nil {
				delivered <- packet
			}
			return nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, runtime := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatalf("start runtime: %v", err)
		}
		defer runtime.Close()
	}

	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportLibP2P, "B", "C")
	adapterA.accept <- connAB
	adapterB.accept <- connBA
	adapterB.accept <- connBC
	adapterC.accept <- connCB
	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)

	forwardedOnFirstHop := make(chan struct{}, 1)
	connAB.mu.Lock()
	connAB.sendHook = func(raw []byte) error {
		envelope, err := runtimeA.codec.Decode(raw)
		if err == nil {
			packet := envelope.GetForwardedPacket()
			if packet != nil && packet.TrafficClass == TrafficPointToPointStream {
				select {
				case forwardedOnFirstHop <- struct{}{}:
				default:
				}
			}
		}
		return nil
	}
	connAB.mu.Unlock()

	if err := runtimeA.RouteEnvelope(ctx, 3, testStreamEnvelope()); err != nil {
		t.Fatalf("route stream across transit: %v", err)
	}
	select {
	case <-forwardedOnFirstHop:
	case <-time.After(time.Second):
		t.Fatal("multi-hop stream did not use forwarded envelope on first hop")
	}
	select {
	case packet := <-delivered:
		if packet.SourceNodeId != 1 || packet.TargetNodeId != 3 ||
			packet.TrafficClass != TrafficPointToPointStream || packet.IngressTransport != TransportLibP2P {
			t.Fatalf("unexpected multi-hop metadata: %+v", packet)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for multi-hop stream delivery")
	}
}

func TestRuntimeKeepsNonStreamTrafficOnForwardingPath(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connSource, connTarget := newFakeConnPair(TransportLibP2P, "source", "target")
	registerTestAdjacency(runtime, connSource, 2, TransportLibP2P)
	for _, nodeID := range []int64{1, 2} {
		applyRuntimeTestNode(runtime, nodeID, DefaultForwardingPolicy(1), TransportLibP2P)
	}
	applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)

	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_QueryRequest{QueryRequest: &QueryRequest{RequestId: 1, Kind: "unchanged"}}}
	if err := runtime.RouteEnvelope(context.Background(), 2, envelope); err != nil {
		t.Fatalf("route query: %v", err)
	}
	wireEnvelope := receiveTestEnvelope(t, connTarget)
	packet := wireEnvelope.GetForwardedPacket()
	if packet == nil || packet.TrafficClass != TrafficControlQuery {
		t.Fatalf("non-stream traffic bypassed forwarding: %T", wireEnvelope.Body)
	}
}

func TestRuntimeDirectStreamSendErrorDoesNotFallback(t *testing.T) {
	wantErr := errors.New("ambiguous direct send failure")
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connSource, _ := newFakeConnPair(TransportLibP2P, "source", "target")
	attempts := 0
	connSource.sendHook = func(raw []byte) error {
		attempts++
		envelope, err := runtime.codec.Decode(raw)
		if err != nil {
			t.Fatalf("decode attempted envelope: %v", err)
		}
		if envelope.GetStreamFrame() == nil || envelope.GetForwardedPacket() != nil {
			t.Fatalf("send error attempted fallback envelope: %T", envelope.Body)
		}
		return wantErr
	}
	registerTestAdjacency(runtime, connSource, 2, TransportLibP2P)
	for _, nodeID := range []int64{1, 2} {
		applyRuntimeTestNode(runtime, nodeID, DefaultForwardingPolicy(1), TransportLibP2P)
	}
	applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)

	err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelope())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected direct send error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected exactly one send attempt, got %d", attempts)
	}
}

type errorEnvelopeSigner struct{ err error }

func (s errorEnvelopeSigner) Sign(*ClusterEnvelope, []byte) ([]byte, error) { return nil, s.err }

func TestRuntimeDirectStreamReturnsSigningErrorBeforeTransport(t *testing.T) {
	wantErr := errors.New("sign direct stream")
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P), func(opts *RuntimeOptions) {
		opts.Signer = errorEnvelopeSigner{err: wantErr}
	})
	connSource, _ := newFakeConnPair(TransportLibP2P, "source", "target")
	attempts := 0
	connSource.sendHook = func([]byte) error {
		attempts++
		return nil
	}
	registerTestAdjacency(runtime, connSource, 2, TransportLibP2P)

	err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelope())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected signing error, got %v", err)
	}
	if attempts != 0 {
		t.Fatalf("transport called after signing error: %d", attempts)
	}
}

type discardBenchmarkConn struct{}

func (discardBenchmarkConn) Send(context.Context, []byte) error      { return nil }
func (discardBenchmarkConn) SendOwned(context.Context, []byte) error { return nil }
func (discardBenchmarkConn) Receive(context.Context) ([]byte, error) { return nil, io.EOF }
func (discardBenchmarkConn) Close() error                            { return nil }
func (discardBenchmarkConn) RemoteNodeHint() string                  { return "benchmark" }
func (discardBenchmarkConn) Transport() TransportKind                { return TransportLibP2P }

func BenchmarkMeshDirectStreamEnvelopeFastPath(b *testing.B) {
	runtime := newTestRuntime(b, 1, newFakeAdapter(TransportLibP2P))
	conn := discardBenchmarkConn{}
	registerTestAdjacency(runtime, conn, 2, TransportLibP2P)
	envelope := testStreamEnvelope()
	ctx := context.Background()
	payloadBytes := int64(len(envelope.GetStreamFrame().Payload))

	b.Run("direct_fast_path", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(payloadBytes)
		for i := 0; i < b.N; i++ {
			if err := runtime.RouteEnvelope(ctx, 2, envelope); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("forwarded_wrapper", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(payloadBytes)
		for i := 0; i < b.N; i++ {
			inner, err := runtime.codec.Encode(envelope)
			if err != nil {
				b.Fatal(err)
			}
			outer := &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: &ForwardedPacket{
				PacketId: uint64(i + 1), SourceNodeId: 1, TargetNodeId: 2,
				TrafficClass: TrafficPointToPointStream, TtlHops: DefaultTTLHops, Payload: inner,
			}}}
			if err := runtime.sendEnvelopeCtx(ctx, conn, outer, runtime.helloTimeout); err != nil {
				b.Fatal(err)
			}
		}
	})
}
