package mesh

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// TestRuntimeThreeNodeConvergence covers Phase 4.6 three-node linear
// topology convergence using the real runtime (fake transport).
func TestRuntimeThreeNodeConvergence(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	adapterC := newFakeAdapter(TransportLibP2P)

	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)
	runtimeC := newTestRuntime(t, 3, adapterC)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, r := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := r.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer r.Close()
	}

	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportLibP2P, "B", "C")
	adapterA.accept <- connAB
	adapterB.accept <- connBA
	adapterB.accept <- connBC
	adapterC.accept <- connCB

	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)
	waitForNodes(t, runtimeC, []int64{1, 2, 3}, 3*time.Second)

	planner := NewPlanner(1)
	decision, ok := planner.Compute(runtimeA.store.Snapshot(), 3, TrafficControlCritical, TransportUnspecified)
	if !ok {
		t.Fatalf("planner failed to compute A -> C")
	}
	if decision.NextHopNodeID != 2 {
		t.Fatalf("expected next hop 2, got %d", decision.NextHopNodeID)
	}
}

func TestRuntimeRoutesQueryEnvelopeAcrossTransit(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	adapterC := newFakeAdapter(TransportLibP2P)

	delivered := make(chan *ClusterEnvelope, 1)
	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)
	runtimeC := newTestRuntime(t, 3, adapterC, func(opts *RuntimeOptions) {
		opts.QueryHandler = func(_ context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error {
			if packet.SourceNodeId != 1 || packet.TargetNodeId != 3 {
				t.Fatalf("unexpected delivered packet metadata: %+v", packet)
			}
			delivered <- envelope
			return nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, r := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := r.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer r.Close()
	}

	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportLibP2P, "B", "C")
	adapterA.accept <- connAB
	adapterB.accept <- connBA
	adapterB.accept <- connBC
	adapterC.accept <- connCB

	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_QueryRequest{
		QueryRequest: &QueryRequest{
			RequestId: 77,
			Kind:      "test.query",
			Payload:   []byte("hello"),
		},
	}}
	if err := runtimeA.RouteEnvelope(ctx, 3, envelope); err != nil {
		t.Fatalf("route query envelope: %v", err)
	}

	select {
	case got := <-delivered:
		req := got.GetQueryRequest()
		if req == nil || req.RequestId != 77 || req.Kind != "test.query" || string(req.Payload) != "hello" {
			t.Fatalf("unexpected delivered query envelope: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for query delivery")
	}
}

func TestRuntimeRoutesReplicationEnvelopeAcrossTransit(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	adapterC := newFakeAdapter(TransportLibP2P)

	delivered := make(chan *ClusterEnvelope, 1)
	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)
	runtimeC := newTestRuntime(t, 3, adapterC, func(opts *RuntimeOptions) {
		opts.EnvelopeHandler = func(_ context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error {
			if packet.SourceNodeId != 1 || packet.TargetNodeId != 3 || packet.TrafficClass != TrafficReplicationStream {
				t.Fatalf("unexpected delivered packet metadata: %+v", packet)
			}
			delivered <- envelope
			return nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, r := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := r.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer r.Close()
	}

	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportLibP2P, "B", "C")
	adapterA.accept <- connAB
	adapterB.accept <- connBA
	adapterB.accept <- connBC
	adapterC.accept <- connCB

	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_ReplicationBatch{
		ReplicationBatch: &ReplicationBatch{
			OriginNodeId: 1,
			Sequence:     99,
			SentAtHlc:    "2026-01-01T00:00:00.000000000Z/1/1",
			EventBatch: &internalproto.EventBatch{
				OriginNodeId: 1,
			},
		},
	}}
	if err := runtimeA.RouteEnvelope(ctx, 3, envelope); err != nil {
		t.Fatalf("route replication envelope: %v", err)
	}

	select {
	case got := <-delivered:
		batch := got.GetReplicationBatch()
		if batch == nil || batch.Sequence != 99 || batch.GetEventBatch().GetOriginNodeId() != 1 {
			t.Fatalf("unexpected delivered replication batch: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for replication delivery")
	}
}

// TestRuntimeMixedTransports builds A(libp2p) - B(libp2p+zeromq) - C(zeromq)
// and verifies bridge behavior for control vs replication traffic.
func TestRuntimeMixedTransports(t *testing.T) {
	t.Parallel()
	adapterALibp2p := newFakeAdapter(TransportLibP2P)
	adapterBLibp2p := newFakeAdapter(TransportLibP2P)
	adapterBZmq := newFakeAdapter(TransportZeroMQ)
	adapterCZmq := newFakeAdapter(TransportZeroMQ)

	mkRuntime := func(id int64, adapters []TransportAdapter) *Runtime {
		options := RuntimeOptions{
			LocalNodeID:   id,
			Adapters:      adapters,
			LocalPolicy:   DefaultForwardingPolicy(1),
			TopologyStore: NewMemoryTopologyStore(),
			HelloTimeout:  500 * time.Millisecond,
		}
		runtime, err := NewRuntime(options)
		if err != nil {
			t.Fatalf("NewRuntime(%d): %v", id, err)
		}
		return runtime
	}
	runtimeA := mkRuntime(1, []TransportAdapter{adapterALibp2p})
	runtimeB := mkRuntime(2, []TransportAdapter{adapterBLibp2p, adapterBZmq})
	runtimeC := mkRuntime(3, []TransportAdapter{adapterCZmq})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, r := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := r.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer r.Close()
	}

	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportZeroMQ, "B", "C")
	adapterALibp2p.accept <- connAB
	adapterBLibp2p.accept <- connBA
	adapterBZmq.accept <- connBC
	adapterCZmq.accept <- connCB

	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)

	snapshotA := runtimeA.store.Snapshot()

	// Control-critical traffic is allowed to bridge; A should find a path.
	planner := NewPlanner(1)
	if decision, ok := planner.Compute(snapshotA, 3, TrafficControlCritical, TransportUnspecified); !ok {
		t.Fatalf("control-critical: no path found")
	} else if decision.PathClass != PathClassCrossTransportBridge && decision.PathClass != PathClassSameTransportForward {
		t.Logf("control-critical decision: %+v", decision)
	}

	// Replication stream must not bridge; A should not find a path.
	if decision, ok := planner.Compute(snapshotA, 3, TrafficReplicationStream, TransportUnspecified); ok {
		t.Fatalf("replication stream should not find a path across bridge, got %+v", decision)
	}
	if decision, ok := planner.Compute(snapshotA, 3, TrafficSnapshotBulk, TransportUnspecified); ok {
		t.Fatalf("snapshot bulk should not find a path across bridge, got %+v", decision)
	}
}

func TestRuntimeObserverRecordsBridgeOnlyAtTransitNode(t *testing.T) {
	t.Parallel()
	adapterALibp2p := newFakeAdapter(TransportLibP2P)
	adapterBLibp2p := newFakeAdapter(TransportLibP2P)
	adapterBZmq := newFakeAdapter(TransportZeroMQ)
	adapterCZmq := newFakeAdapter(TransportZeroMQ)

	sourceObserved := make(chan ForwardingObservation, 4)
	transitObserved := make(chan ForwardingObservation, 4)
	delivered := make(chan struct{}, 1)

	mkRuntime := func(id int64, adapters []TransportAdapter, observer ForwardingObserver, handler LocalEnvelopeHandler) *Runtime {
		options := RuntimeOptions{
			LocalNodeID:        id,
			Adapters:           adapters,
			LocalPolicy:        DefaultForwardingPolicy(1),
			TopologyStore:      NewMemoryTopologyStore(),
			HelloTimeout:       500 * time.Millisecond,
			ForwardingObserver: observer,
			EnvelopeHandler:    handler,
			QueryHandler:       handler,
		}
		runtime, err := NewRuntime(options)
		if err != nil {
			t.Fatalf("NewRuntime(%d): %v", id, err)
		}
		return runtime
	}
	runtimeA := mkRuntime(1, []TransportAdapter{adapterALibp2p}, func(observation ForwardingObservation) {
		sourceObserved <- observation
	}, nil)
	runtimeB := mkRuntime(2, []TransportAdapter{adapterBLibp2p, adapterBZmq}, func(observation ForwardingObservation) {
		transitObserved <- observation
	}, nil)
	runtimeC := mkRuntime(3, []TransportAdapter{adapterCZmq}, nil, func(_ context.Context, _ *ForwardedPacket, _ *ClusterEnvelope) error {
		delivered <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, runtime := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer runtime.Close()
	}

	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportZeroMQ, "B", "C")
	adapterALibp2p.accept <- connAB
	adapterBLibp2p.accept <- connBA
	adapterBZmq.accept <- connBC
	adapterCZmq.accept <- connCB

	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)
	if err := runtimeA.RouteEnvelope(ctx, 3, &ClusterEnvelope{
		Body: &ClusterEnvelope_QueryRequest{QueryRequest: &QueryRequest{RequestId: 88, Kind: "bridge.test"}},
	}); err != nil {
		t.Fatalf("route bridge query: %v", err)
	}

	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bridge delivery")
	}

	source := waitForObservation(t, sourceObserved, time.Second)
	if source.PathClass == PathClassCrossTransportBridge {
		t.Fatalf("source should not record the downstream bridge as local bridge: %+v", source)
	}
	transit := waitForObservation(t, transitObserved, time.Second)
	if transit.PathClass != PathClassCrossTransportBridge {
		t.Fatalf("transit node should record local bridge, got %+v", transit)
	}
}

func TestRuntimeForwardingPolicyRejectsInvalidTransitPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*Runtime)
		run   func(*Runtime) error
	}{
		{
			name: "transit disabled",
			setup: func(runtime *Runtime) {
				applyRuntimeTestNode(runtime, 1, DefaultForwardingPolicy(1), TransportLibP2P)
				policy := DefaultForwardingPolicy(1)
				policy.TransitEnabled = false
				applyRuntimeTestNode(runtime, 2, policy, TransportLibP2P)
				applyRuntimeTestNode(runtime, 3, DefaultForwardingPolicy(1), TransportLibP2P)
				applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)
				applyRuntimeTestLink(runtime, 2, 3, 1, TransportLibP2P, policy)
			},
			run: func(runtime *Runtime) error {
				return runtime.ForwardPacket(context.Background(), &ForwardedPacket{
					PacketId:     1,
					SourceNodeId: 1,
					TargetNodeId: 3,
					TrafficClass: TrafficControlQuery,
					TtlHops:      4,
				})
			},
		},
		{
			name: "traffic denied",
			setup: func(runtime *Runtime) {
				applyRuntimeTestNode(runtime, 1, DefaultForwardingPolicy(1), TransportLibP2P)
				policy := DefaultForwardingPolicy(1)
				for _, rule := range policy.TrafficRules {
					if rule.TrafficClass == TrafficTransientInteractive {
						rule.Disposition = DispositionDeny
					}
				}
				applyRuntimeTestNode(runtime, 2, policy, TransportLibP2P)
				applyRuntimeTestNode(runtime, 3, DefaultForwardingPolicy(1), TransportLibP2P)
				applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)
				applyRuntimeTestLink(runtime, 2, 3, 1, TransportLibP2P, policy)
			},
			run: func(runtime *Runtime) error {
				return runtime.ForwardPacket(context.Background(), &ForwardedPacket{
					PacketId:     2,
					SourceNodeId: 1,
					TargetNodeId: 3,
					TrafficClass: TrafficTransientInteractive,
					TtlHops:      4,
				})
			},
		},
		{
			name: "bridge disabled",
			setup: func(runtime *Runtime) {
				applyRuntimeTestNode(runtime, 1, DefaultForwardingPolicy(1), TransportLibP2P)
				policy := DefaultForwardingPolicy(1)
				policy.BridgeEnabled = false
				applyRuntimeTestNode(runtime, 2, policy, TransportLibP2P, TransportZeroMQ)
				applyRuntimeTestNode(runtime, 3, DefaultForwardingPolicy(1), TransportZeroMQ)
				applyRuntimeTestLink(runtime, 1, 2, 1, TransportLibP2P)
				applyRuntimeTestLink(runtime, 2, 3, 1, TransportZeroMQ, policy)
			},
			run: func(runtime *Runtime) error {
				return runtime.RouteEnvelope(context.Background(), 3, &ClusterEnvelope{
					Body: &ClusterEnvelope_QueryRequest{QueryRequest: &QueryRequest{RequestId: 3, Kind: "blocked"}},
				})
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
			tc.setup(runtime)
			if err := tc.run(runtime); err != ErrNoRoute {
				t.Fatalf("expected ErrNoRoute, got %v", err)
			}
		})
	}
}

func waitForObservation(t *testing.T, ch <-chan ForwardingObservation, timeout time.Duration) ForwardingObservation {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case observation := <-ch:
			if !observation.NoPath {
				return observation
			}
		case <-deadline:
			t.Fatal("timed out waiting for forwarding observation")
		}
	}
}

// TestRuntimeDialSeedUnreachableDoesNotBlock ensures a seed that the
// adapter fails to dial does not prevent runtime operation.
func TestRuntimeDialSeedUnreachableDoesNotBlock(t *testing.T) {
	t.Parallel()
	adapter := newFakeAdapter(TransportZeroMQ)
	// Handle dial by immediately returning an error.
	go func() {
		for req := range adapter.dials {
			req.result <- dialResult{err: context.Canceled}
		}
	}()

	runtime := newTestRuntime(t, 1, adapter, func(opts *RuntimeOptions) {
		opts.DialSeeds = []DialSeed{{Transport: TransportZeroMQ, Endpoint: "tcp://nowhere:1"}}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer runtime.Close()

	// Runtime is still responsive: CurrentGeneration returns a value.
	if g := runtime.CurrentGeneration(); g == 0 {
		t.Fatalf("expected generation > 0")
	}
}

func waitForNodes(t *testing.T, runtime *Runtime, expected []int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot := runtime.store.Snapshot()
		all := true
		for _, id := range expected {
			if _, ok := snapshot.Nodes[id]; !ok {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot := runtime.store.Snapshot()
	t.Fatalf("timed out waiting for nodes %v; have %v", expected, nodeIDs(snapshot))
}

func applyRuntimeTestNode(runtime *Runtime, nodeID int64, policy *ForwardingPolicy, transports ...TransportKind) {
	caps := make([]*TransportCapability, 0, len(transports))
	for _, transport := range transports {
		caps = append(caps, &TransportCapability{
			Transport:       transport,
			InboundEnabled:  true,
			OutboundEnabled: true,
		})
	}
	runtime.store.ApplyHello(nodeID, &NodeHello{
		NodeId:           nodeID,
		ProtocolVersion:  ProtocolVersion,
		ForwardingPolicy: policy,
		Transports:       caps,
	})
}

func applyRuntimeTestLink(runtime *Runtime, from, to int64, generation uint64, transport TransportKind, policy ...*ForwardingPolicy) {
	forwardingPolicy := DefaultForwardingPolicy(1)
	if len(policy) > 0 {
		forwardingPolicy = policy[0]
	}
	runtime.store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     from,
		Generation:       generation,
		ForwardingPolicy: forwardingPolicy,
		Transports: []*TransportCapability{
			{Transport: transport, InboundEnabled: true, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{
				FromNodeId:  from,
				ToNodeId:    to,
				Transport:   transport,
				PathClass:   PathClassDirect,
				CostMs:      1,
				Established: true,
			},
		},
	})
}
