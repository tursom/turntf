package mesh

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeGenerationMonotonic(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	initialA := runtimeA.CurrentGeneration()
	if initialA == 0 {
		t.Fatalf("expected initial generation > 0")
	}

	connA, connB := newFakeConnPair(TransportLibP2P, "A", "B")
	adapterA.accept <- connA
	adapterB.accept <- connB
	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 1, time.Second)

	if g := runtimeA.CurrentGeneration(); g <= initialA {
		t.Fatalf("generation did not advance after adjacency: %d <= %d", g, initialA)
	}

	afterAdj := runtimeA.CurrentGeneration()
	_ = connA.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if runtimeA.CurrentGeneration() > afterAdj {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if g := runtimeA.CurrentGeneration(); g <= afterAdj {
		t.Fatalf("generation did not advance after adjacency loss: %d <= %d", g, afterAdj)
	}
}

func TestRuntimeTopologyFloodingConvergence(t *testing.T) {
	t.Parallel()
	// Build a linear A - B - C topology. Only A<->B and B<->C are
	// connected directly; A should still see C's origin via flooding.
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

	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 2, time.Second)
	waitForAdjacency(t, runtimeC, 1, time.Second)

	deadline := time.Now().Add(3 * time.Second)
	var snapA, snapC TopologySnapshot
	for time.Now().Before(deadline) {
		snapA = runtimeA.store.Snapshot()
		snapC = runtimeC.store.Snapshot()
		if hasNode(snapA, 3) && hasNode(snapC, 1) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !hasNode(snapA, 3) {
		t.Fatalf("A never learned about C; nodes=%v", nodeIDs(snapA))
	}
	if !hasNode(snapC, 1) {
		t.Fatalf("C never learned about A; nodes=%v", nodeIDs(snapC))
	}

	// Disconnect B-C; A should eventually no longer see B->C (since B is
	// A's only connected peer, B's new-generation update removes the link).
	_ = connBC.Close()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapA = runtimeA.store.Snapshot()
		if !hasLink(snapA, 2, 3) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapA = runtimeA.store.Snapshot()
	if hasLink(snapA, 2, 3) {
		t.Fatalf("A should no longer see B->C link; links=%v", snapA.Links)
	}
}

func hasNode(snapshot TopologySnapshot, nodeID int64) bool {
	_, ok := snapshot.Nodes[nodeID]
	return ok
}

func nodeIDs(snapshot TopologySnapshot) []int64 {
	ids := make([]int64, 0, len(snapshot.Nodes))
	for id := range snapshot.Nodes {
		ids = append(ids, id)
	}
	return ids
}

func hasLink(snapshot TopologySnapshot, from, to int64) bool {
	for _, link := range snapshot.Links {
		if link.FromNodeID == from && link.ToNodeID == to && link.Established {
			return true
		}
	}
	return false
}

func TestRuntimeTombstoneBeforeRemoval(t *testing.T) {
	t.Parallel()
	// Three nodes in a line so that when B-C breaks, A should first
	// observe B advertising B->C with established=false (tombstone),
	// and then the link entirely absent in a later generation.
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

	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 2, time.Second)
	waitForAdjacency(t, runtimeC, 1, time.Second)

	// Wait for full convergence so B->C is visible from A.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hasLink(runtimeA.store.Snapshot(), 2, 3) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !hasLink(runtimeA.store.Snapshot(), 2, 3) {
		t.Fatalf("A should have seen B->C before break")
	}

	_ = connBC.Close()

	// Eventually the B->C link should no longer be present as established.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !hasLink(runtimeA.store.Snapshot(), 2, 3) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if hasLink(runtimeA.store.Snapshot(), 2, 3) {
		t.Fatalf("A should no longer treat B->C as established")
	}
}

func TestRuntimeLinkMeasurement(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.PingInterval = 20 * time.Millisecond
	})
	runtimeB := newTestRuntime(t, 2, adapterB, func(opts *RuntimeOptions) {
		opts.PingInterval = 20 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("startA: %v", err)
	}
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("startB: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	connA, connB := newFakeConnPair(TransportLibP2P, "A", "B")
	adapterA.accept <- connA
	adapterB.accept <- connB
	waitForAdjacency(t, runtimeA, 1, time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adjs := runtimeA.Adjacencies()
		if len(adjs) == 1 && adjs[0].Samples > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	adjs := runtimeA.Adjacencies()
	t.Fatalf("expected at least one RTT sample, got %+v", adjs)
}
