package mesh

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRuntimeLinearFloodingConvergesAt50Nodes(t *testing.T) {
	runLinearFloodingConvergence(t, 50, 10*time.Second)
}

func TestRuntimeLinearFloodingConvergesAt100Nodes(t *testing.T) {
	runLinearFloodingConvergence(t, 100, 20*time.Second)
}

func TestRuntimeDiamondReRoutesAfterNextHopFailure(t *testing.T) {
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	adapterC := newFakeAdapter(TransportLibP2P)
	adapterD := newFakeAdapter(TransportLibP2P)

	runtimeA := newScaleTestRuntime(t, 1, adapterA)
	runtimeB := newScaleTestRuntime(t, 2, adapterB)
	runtimeC := newScaleTestRuntime(t, 3, adapterC)
	runtimeD := newScaleTestRuntime(t, 4, adapterD)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, runtime := range []*Runtime{runtimeA, runtimeB, runtimeC, runtimeD} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatalf("start runtime: %v", err)
		}
		defer runtime.Close()
	}

	connectFakeAdjacency(adapterA, adapterB, TransportLibP2P, "A", "B")
	connectFakeAdjacency(adapterA, adapterC, TransportLibP2P, "A", "C")
	connBD, connDB := newFakeConnPair(TransportLibP2P, "B", "D")
	adapterB.accept <- connBD
	adapterD.accept <- connDB
	connCD, connDC := newFakeConnPair(TransportLibP2P, "C", "D")
	adapterC.accept <- connCD
	adapterD.accept <- connDC

	waitForNodes(t, runtimeA, []int64{1, 2, 3, 4}, 5*time.Second)
	waitForRouteNextHop(t, runtimeA, 4, 2, 5*time.Second)

	if err := connBD.Close(); err != nil {
		t.Fatalf("close B-D conn: %v", err)
	}
	waitForRouteNextHop(t, runtimeA, 4, 3, 5*time.Second)
}

func runLinearFloodingConvergence(t *testing.T, nodes int, timeout time.Duration) {
	t.Helper()

	adapters := make([]*fakeAdapter, 0, nodes)
	runtimes := make([]*Runtime, 0, nodes)
	expectedNodes := make([]int64, 0, nodes)
	for i := 0; i < nodes; i++ {
		adapter := newFakeAdapter(TransportLibP2P)
		adapters = append(adapters, adapter)
		runtimes = append(runtimes, newScaleTestRuntime(t, int64(i+1), adapter))
		expectedNodes = append(expectedNodes, int64(i+1))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, runtime := range runtimes {
		if err := runtime.Start(ctx); err != nil {
			t.Fatalf("start runtime: %v", err)
		}
		defer runtime.Close()
	}

	for i := 0; i < nodes-1; i++ {
		connectFakeAdjacency(adapters[i], adapters[i+1], TransportLibP2P, fmt.Sprintf("n%d", i+1), fmt.Sprintf("n%d", i+2))
	}

	waitForNodes(t, runtimes[0], expectedNodes, timeout)
	waitForNodes(t, runtimes[nodes-1], expectedNodes, timeout)
}

func connectFakeAdjacency(left, right *fakeAdapter, kind TransportKind, leftHint, rightHint string) {
	connLeft, connRight := newFakeConnPair(kind, leftHint, rightHint)
	left.accept <- connLeft
	right.accept <- connRight
}

func newScaleTestRuntime(t *testing.T, localID int64, adapter TransportAdapter) *Runtime {
	t.Helper()
	return newTestRuntime(t, localID, adapter, func(opts *RuntimeOptions) {
		opts.TopologyPublishPeriod = 50 * time.Millisecond
		opts.PingInterval = time.Hour
	})
}

func waitForRouteNextHop(t *testing.T, runtime *Runtime, destinationNodeID, nextHopNodeID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if decision, ok := runtime.DescribeRoute(destinationNodeID, TrafficControlCritical); ok && decision.NextHopNodeID == nextHopNodeID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	decision, ok := runtime.DescribeRoute(destinationNodeID, TrafficControlCritical)
	t.Fatalf("timed out waiting for next hop %d to %d; ok=%v decision=%+v", nextHopNodeID, destinationNodeID, ok, decision)
}
