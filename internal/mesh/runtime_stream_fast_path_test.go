package mesh

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func testStreamEnvelope() *ClusterEnvelope {
	return testStreamEnvelopeFor(streamFrameKindOpen, 1)
}

func testStreamEnvelopeFor(kind uint32, epoch uint64) *ClusterEnvelope {
	return &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: &StreamFrame{
		StreamId: []byte("0123456789abcdef"),
		Kind:     kind,
		Epoch:    epoch,
		Payload:  []byte("stream-payload"),
	}}}
}

func testConsensusEnvelope() *ClusterEnvelope {
	return &ClusterEnvelope{Body: &ClusterEnvelope_ConsensusMessage{ConsensusMessage: &ConsensusMessage{
		GroupId:      "test-kv",
		SourceNodeId: 1,
		TargetNodeId: 2,
		MessageId:    7,
		Payload:      []byte("consensus-payload"),
	}}}
}

func setTestAdjacencyScore(adj *Adjacency, rtt, jitter float64) {
	adj.mu.Lock()
	adj.rttEWMA = rtt
	adj.jitterEWMA = jitter
	adj.mu.Unlock()
}

func registerTestAdjacency(runtime *Runtime, conn TransportConn, remoteNodeID int64, transport TransportKind) *Adjacency {
	return runtime.registerAdjacency(conn, transport, &NodeHello{NodeId: remoteNodeID}, false)
}

func newDirectStreamTestRuntime(t testing.TB, adapters ...TransportAdapter) *Runtime {
	t.Helper()
	return newTestRuntime(t, 1, adapters[0], func(opts *RuntimeOptions) {
		opts.Adapters = adapters
	})
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

func TestRuntimeAdvertisesDirectConsensusSupport(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	if !runtime.localHello(TransportLibP2P).GetDirectConsensusSupported() {
		t.Fatal("new runtime did not advertise direct consensus support")
	}
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

func TestRuntimeRoutesConsensusDirectAsBareEnvelope(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	target := newTestRuntime(t, 2, newFakeAdapter(TransportLibP2P))
	connSource, connTarget := newFakeConnPair(TransportLibP2P, "source", "target")
	runtime.registerAdjacency(connSource, TransportLibP2P, &NodeHello{NodeId: 2, DirectConsensusSupported: true}, false)
	targetAdj := registerTestAdjacency(target, connTarget, 1, TransportLibP2P)
	var gotPacket *ForwardedPacket
	var gotEnvelope *ClusterEnvelope
	target.envelopeHandler = func(_ context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error {
		gotPacket, gotEnvelope = packet, envelope
		return nil
	}

	want := testConsensusEnvelope()
	if err := runtime.RouteEnvelope(context.Background(), 2, want); err != nil {
		t.Fatalf("route direct consensus: %v", err)
	}
	wireEnvelope := receiveTestEnvelope(t, connTarget)
	if wireEnvelope.GetConsensusMessage() == nil || wireEnvelope.GetForwardedPacket() != nil {
		t.Fatalf("direct consensus used unexpected wire envelope: %T", wireEnvelope.Body)
	}
	got := wireEnvelope.GetConsensusMessage()
	if got.GetGroupId() != "test-kv" || got.GetSourceNodeId() != 1 || got.GetTargetNodeId() != 2 ||
		got.GetMessageId() != 7 || string(got.GetPayload()) != "consensus-payload" {
		t.Fatalf("direct consensus payload changed: %+v", got)
	}
	target.dispatchEnvelope(context.Background(), targetAdj, wireEnvelope)
	if gotEnvelope != wireEnvelope || gotPacket == nil || gotPacket.SourceNodeId != 1 ||
		gotPacket.TargetNodeId != 2 || gotPacket.TrafficClass != TrafficConsensus ||
		gotPacket.LastHopNodeId != 1 || gotPacket.IngressTransport != TransportLibP2P {
		t.Fatalf("direct consensus did not reach handler: packet=%+v envelope=%p", gotPacket, gotEnvelope)
	}
}

func TestRuntimeRoutesConsensusToLegacyDirectPeerAsForwardedPacket(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connSource, connTarget := newFakeConnPair(TransportLibP2P, "source", "legacy-target")
	registerTestAdjacency(runtime, connSource, 2, TransportLibP2P)
	for _, nodeID := range []int64{1, 2} {
		applyRuntimeTestNode(runtime, nodeID, DefaultForwardingPolicy(1), TransportLibP2P)
	}
	applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)

	if err := runtime.RouteEnvelope(context.Background(), 2, testConsensusEnvelope()); err != nil {
		t.Fatalf("route consensus to legacy peer: %v", err)
	}
	wire := receiveTestEnvelope(t, connTarget)
	packet := wire.GetForwardedPacket()
	if packet == nil || packet.GetTrafficClass() != TrafficConsensus {
		t.Fatalf("legacy peer received unsupported bare consensus: %T", wire.Body)
	}
	inner, err := runtime.codec.Decode(packet.GetPayload())
	if err != nil || inner.GetConsensusMessage() == nil {
		t.Fatalf("decode legacy consensus payload: envelope=%v err=%v", inner, err)
	}
}

func TestRuntimeRoutesConsensusAcrossTransitWithoutDirectAdjacency(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connToTransit, transitConn := newFakeConnPair(TransportLibP2P, "source", "transit")
	registerTestAdjacency(runtime, connToTransit, 2, TransportLibP2P)
	for _, nodeID := range []int64{1, 2, 3} {
		applyRuntimeTestNode(runtime, nodeID, DefaultForwardingPolicy(1), TransportLibP2P)
	}
	applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)
	applyRuntimeTestLink(runtime, 2, 3, 1, TransportLibP2P)

	if err := runtime.RouteEnvelope(context.Background(), 3, testConsensusEnvelope()); err != nil {
		t.Fatalf("route consensus fallback: %v", err)
	}
	wireEnvelope := receiveTestEnvelope(t, transitConn)
	packet := wireEnvelope.GetForwardedPacket()
	if packet == nil || packet.GetTrafficClass() != TrafficConsensus {
		t.Fatalf("consensus fallback did not use consensus forwarded packet: %T", wireEnvelope.Body)
	}
	inner, err := runtime.codec.Decode(packet.GetPayload())
	if err != nil || inner.GetConsensusMessage() == nil {
		t.Fatalf("decode consensus fallback payload: envelope=%v err=%v", inner, err)
	}
}

func TestRuntimeStreamWithoutIDUsesOriginalDirectPath(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connSource, connTarget := newFakeConnPair(TransportLibP2P, "source", "target")
	registerTestAdjacency(runtime, connSource, 2, TransportLibP2P)
	envelope := testStreamEnvelopeFor(streamFrameKindOpen, 1)
	envelope.GetStreamFrame().StreamId = nil

	if err := runtime.RouteEnvelope(context.Background(), 2, envelope); err != nil {
		t.Fatalf("route stream without id: %v", err)
	}
	if wire := receiveTestEnvelope(t, connTarget); wire.GetStreamFrame() == nil || wire.GetForwardedPacket() != nil {
		t.Fatalf("stream without id changed original direct path: %T", wire.Body)
	}
	if got := len(runtime.directStreamAffinity); got != 0 {
		t.Fatalf("stream without id created %d affinities", got)
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

func TestRuntimeKeepsForwardingAffinityWhenDirectAdjacencyAppears(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	connToTransit, transitConn := newFakeConnPair(TransportLibP2P, "source", "transit")
	registerTestAdjacency(runtime, connToTransit, 2, TransportLibP2P)
	for _, nodeID := range []int64{1, 2, 3} {
		applyRuntimeTestNode(runtime, nodeID, DefaultForwardingPolicy(1), TransportLibP2P)
	}
	applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)
	applyRuntimeTestLink(runtime, 2, 3, 1, TransportLibP2P)

	if err := runtime.RouteEnvelope(context.Background(), 3, testStreamEnvelopeFor(streamFrameKindOpen, 1)); err != nil {
		t.Fatalf("route forwarded open: %v", err)
	}
	_ = receiveTestEnvelope(t, transitConn)

	directConn, _ := newFakeConnPair(TransportWebSocket, "source-direct", "target-direct")
	directSends := 0
	directConn.sendHook = func([]byte) error { directSends++; return nil }
	registerTestAdjacency(runtime, directConn, 3, TransportWebSocket)
	if err := runtime.RouteEnvelope(context.Background(), 3, testStreamEnvelopeFor(streamFrameKindData, 1)); err != nil {
		t.Fatalf("route forwarded data: %v", err)
	}
	if directSends != 0 {
		t.Fatalf("same epoch switched from forwarding to direct %d times", directSends)
	}
	if packet := receiveTestEnvelope(t, transitConn).GetForwardedPacket(); packet == nil {
		t.Fatal("same epoch data did not remain on forwarding path")
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

func TestRuntimeDirectStreamAffinityIgnoresDynamicScoreChanges(t *testing.T) {
	runtime := newDirectStreamTestRuntime(t, newFakeAdapter(TransportLibP2P), newFakeAdapter(TransportWebSocket))
	primary, _ := newFakeConnPair(TransportLibP2P, "primary", "target-primary")
	alternate, _ := newFakeConnPair(TransportWebSocket, "alternate", "target-alternate")
	primaryAdj := registerTestAdjacency(runtime, primary, 2, TransportLibP2P)
	alternateAdj := registerTestAdjacency(runtime, alternate, 2, TransportWebSocket)
	setTestAdjacencyScore(primaryAdj, 5, 1)
	setTestAdjacencyScore(alternateAdj, 50, 10)

	primarySends := 0
	alternateSends := 0
	primary.sendHook = func([]byte) error { primarySends++; return nil }
	alternate.sendHook = func([]byte) error { alternateSends++; return nil }

	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindOpen, 1)); err != nil {
		t.Fatalf("route open: %v", err)
	}
	setTestAdjacencyScore(primaryAdj, 100, 20)
	setTestAdjacencyScore(alternateAdj, 1, 0)
	for _, kind := range []uint32{streamFrameKindData, streamFrameKindAck} {
		if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(kind, 1)); err != nil {
			t.Fatalf("route kind %d: %v", kind, err)
		}
	}
	if primarySends != 3 || alternateSends != 0 {
		t.Fatalf("score change moved stream: primary=%d alternate=%d", primarySends, alternateSends)
	}
}

func TestRuntimeDirectStreamResumeReplacesAffinity(t *testing.T) {
	runtime := newDirectStreamTestRuntime(t, newFakeAdapter(TransportLibP2P), newFakeAdapter(TransportWebSocket))
	primary, _ := newFakeConnPair(TransportLibP2P, "primary", "target-primary")
	alternate, _ := newFakeConnPair(TransportWebSocket, "alternate", "target-alternate")
	primaryAdj := registerTestAdjacency(runtime, primary, 2, TransportLibP2P)
	alternateAdj := registerTestAdjacency(runtime, alternate, 2, TransportWebSocket)
	setTestAdjacencyScore(primaryAdj, 5, 0)
	setTestAdjacencyScore(alternateAdj, 50, 0)

	primarySends := 0
	alternateSends := 0
	primary.sendHook = func([]byte) error { primarySends++; return nil }
	alternate.sendHook = func([]byte) error { alternateSends++; return nil }
	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindOpen, 1)); err != nil {
		t.Fatalf("route open: %v", err)
	}

	setTestAdjacencyScore(primaryAdj, 100, 0)
	setTestAdjacencyScore(alternateAdj, 1, 0)
	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindResume, 2)); err != nil {
		t.Fatalf("route resume: %v", err)
	}
	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindData, 2)); err != nil {
		t.Fatalf("route resumed data: %v", err)
	}
	if primarySends != 1 || alternateSends != 2 {
		t.Fatalf("resume did not replace affinity: primary=%d alternate=%d", primarySends, alternateSends)
	}
}

func TestRuntimeDirectStreamResumeAckAdvancesReverseAffinity(t *testing.T) {
	runtimeA := newDirectStreamTestRuntime(t, newFakeAdapter(TransportLibP2P), newFakeAdapter(TransportWebSocket))
	adapterBPrimary := newFakeAdapter(TransportLibP2P)
	adapterBAlternate := newFakeAdapter(TransportWebSocket)
	runtimeB := newTestRuntime(t, 2, adapterBPrimary, func(opts *RuntimeOptions) {
		opts.Adapters = []TransportAdapter{adapterBPrimary, adapterBAlternate}
	})
	primaryA, primaryB := newFakeConnPair(TransportLibP2P, "A-primary", "B-primary")
	alternateA, alternateB := newFakeConnPair(TransportWebSocket, "A-alternate", "B-alternate")
	primaryAdjA := registerTestAdjacency(runtimeA, primaryA, 2, TransportLibP2P)
	primaryAdjB := registerTestAdjacency(runtimeB, primaryB, 1, TransportLibP2P)
	alternateAdjA := registerTestAdjacency(runtimeA, alternateA, 2, TransportWebSocket)
	alternateAdjB := registerTestAdjacency(runtimeB, alternateB, 1, TransportWebSocket)
	for _, adj := range []*Adjacency{primaryAdjA, primaryAdjB} {
		setTestAdjacencyScore(adj, 1, 0)
	}
	for _, adj := range []*Adjacency{alternateAdjA, alternateAdjB} {
		setTestAdjacencyScore(adj, 100, 0)
	}

	ctx := context.Background()
	if err := runtimeA.RouteEnvelope(ctx, 2, testStreamEnvelopeFor(streamFrameKindOpen, 1)); err != nil {
		t.Fatalf("route open A to B: %v", err)
	}
	runtimeB.dispatchEnvelope(ctx, primaryAdjB, receiveTestEnvelope(t, primaryB))
	if err := runtimeB.RouteEnvelope(ctx, 1, testStreamEnvelopeFor(streamFrameKindAck, 1)); err != nil {
		t.Fatalf("route initial ack B to A: %v", err)
	}
	runtimeA.dispatchEnvelope(ctx, primaryAdjA, receiveTestEnvelope(t, primaryA))
	if err := runtimeA.RouteEnvelope(ctx, 2, testStreamEnvelopeFor(streamFrameKindData, 1)); err != nil {
		t.Fatalf("route initial data A to B: %v", err)
	}
	runtimeB.dispatchEnvelope(ctx, primaryAdjB, receiveTestEnvelope(t, primaryB))

	for _, adj := range []*Adjacency{primaryAdjA, primaryAdjB} {
		setTestAdjacencyScore(adj, 100, 0)
	}
	for _, adj := range []*Adjacency{alternateAdjA, alternateAdjB} {
		setTestAdjacencyScore(adj, 1, 0)
	}
	if err := runtimeA.RouteEnvelope(ctx, 2, testStreamEnvelopeFor(streamFrameKindResume, 2)); err != nil {
		t.Fatalf("route resume A to B: %v", err)
	}
	runtimeB.dispatchEnvelope(ctx, alternateAdjB, receiveTestEnvelope(t, alternateB))
	if err := runtimeB.RouteEnvelope(ctx, 1, testStreamEnvelopeFor(streamFrameKindAck, 2)); err != nil {
		t.Fatalf("route resume ack B to A: %v", err)
	}
	runtimeA.dispatchEnvelope(ctx, alternateAdjA, receiveTestEnvelope(t, alternateA))

	key := directStreamAffinityKey{targetNodeID: 1, streamID: "0123456789abcdef"}
	entry := runtimeB.directStreamAffinity[key]
	if entry.epoch != 2 || entry.adj != alternateAdjB {
		t.Fatalf("resume ack affinity = {epoch:%d adj:%p}, want epoch 2 adj %p", entry.epoch, entry.adj, alternateAdjB)
	}

	for _, test := range []struct {
		name  string
		kind  uint32
		epoch uint64
	}{
		{name: "higher open", kind: streamFrameKindOpen, epoch: 3},
		{name: "higher open ack", kind: streamFrameKindOpenAck, epoch: 3},
		{name: "higher data", kind: streamFrameKindData, epoch: 3},
		{name: "higher close", kind: streamFrameKindClose, epoch: 3},
		{name: "stale ack", kind: streamFrameKindAck, epoch: 1},
		{name: "stale data", kind: streamFrameKindData, epoch: 1},
		{name: "stale close", kind: streamFrameKindClose, epoch: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runtimeB.RouteEnvelope(ctx, 1, testStreamEnvelopeFor(test.kind, test.epoch))
			if !errors.Is(err, ErrNoRoute) {
				t.Fatalf("route kind %d epoch %d error = %v, want ErrNoRoute", test.kind, test.epoch, err)
			}
			entry := runtimeB.directStreamAffinity[key]
			if entry.epoch != 2 || entry.adj != alternateAdjB {
				t.Fatalf("affinity changed to {epoch:%d adj:%p}, want epoch 2 adj %p", entry.epoch, entry.adj, alternateAdjB)
			}
		})
	}

	if err := runtimeB.RouteEnvelope(ctx, 1, testStreamEnvelopeFor(streamFrameKindData, 2)); err != nil {
		t.Fatalf("route data on advanced affinity: %v", err)
	}
	if got := receiveTestEnvelope(t, alternateA).GetStreamFrame(); got == nil || got.Kind != streamFrameKindData || got.Epoch != 2 {
		t.Fatalf("advanced affinity delivered unexpected frame: %+v", got)
	}
}

func TestRuntimeDirectStreamCloseAndRuntimeCloseClearAffinity(t *testing.T) {
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	conn, _ := newFakeConnPair(TransportLibP2P, "source", "target")
	registerTestAdjacency(runtime, conn, 2, TransportLibP2P)
	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindOpen, 1)); err != nil {
		t.Fatalf("route open: %v", err)
	}
	if got := len(runtime.directStreamAffinity); got != 1 {
		t.Fatalf("affinity count after open = %d, want 1", got)
	}
	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindClose, 1)); err != nil {
		t.Fatalf("route close: %v", err)
	}
	if got := len(runtime.directStreamAffinity); got != 0 {
		t.Fatalf("affinity count after close = %d, want 0", got)
	}

	if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindOpen, 2)); err != nil {
		t.Fatalf("route second open: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
	if got := len(runtime.directStreamAffinity); got != 0 {
		t.Fatalf("affinity count after runtime close = %d, want 0", got)
	}
}

func TestRuntimeDirectStreamAffinityFailureDoesNotSwitch(t *testing.T) {
	for _, test := range []struct {
		name      string
		breakPath func(*Adjacency, *fakeConn, error)
		wantErr   error
	}{
		{
			name: "send error",
			breakPath: func(_ *Adjacency, conn *fakeConn, wantErr error) {
				conn.sendHook = func([]byte) error { return wantErr }
			},
			wantErr: errors.New("pinned send failed"),
		},
		{
			name: "not established",
			breakPath: func(adj *Adjacency, _ *fakeConn, _ error) {
				adj.mu.Lock()
				adj.established = false
				adj.mu.Unlock()
			},
			wantErr: ErrNoRoute,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := newDirectStreamTestRuntime(t, newFakeAdapter(TransportLibP2P), newFakeAdapter(TransportWebSocket))
			primary, _ := newFakeConnPair(TransportLibP2P, "primary", "target-primary")
			alternate, _ := newFakeConnPair(TransportWebSocket, "alternate", "target-alternate")
			primaryAdj := registerTestAdjacency(runtime, primary, 2, TransportLibP2P)
			alternateAdj := registerTestAdjacency(runtime, alternate, 2, TransportWebSocket)
			setTestAdjacencyScore(primaryAdj, 1, 0)
			setTestAdjacencyScore(alternateAdj, 100, 0)
			alternateSends := 0
			alternate.sendHook = func([]byte) error { alternateSends++; return nil }
			if err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindOpen, 1)); err != nil {
				t.Fatalf("route open: %v", err)
			}

			setTestAdjacencyScore(alternateAdj, 0, 0)
			test.breakPath(primaryAdj, primary, test.wantErr)
			err := runtime.RouteEnvelope(context.Background(), 2, testStreamEnvelopeFor(streamFrameKindData, 1))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("route data error = %v, want %v", err, test.wantErr)
			}
			if alternateSends != 0 {
				t.Fatalf("failed affinity switched to alternate %d times", alternateSends)
			}
		})
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
