package mesh

import (
	"context"
	"sync"
	"testing"
)

func TestRuntimeConcurrentPublicationVersions(t *testing.T) {
	p := &memoryGenerationPersistence{}
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket), func(o *RuntimeOptions) { o.GenerationPersistence = p })
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.bumpGenerationAndPublish(context.Background())
			r.publishLocalTopology(context.Background())
		}()
	}
	wg.Wait()
	persisted, err := p.Load()
	if err != nil || persisted != r.CurrentGeneration() || r.lastUpdate[1].Generation != persisted || r.knownGeneration[1] != persisted {
		t.Fatalf("publication version regressed: persisted=%d current=%d err=%v", persisted, r.CurrentGeneration(), err)
	}
}

func TestRuntimePeriodicMetricsHaveFreshGeneration(t *testing.T) {
	source := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	target := newTestRuntime(t, 2, newFakeAdapter(TransportWebSocket))
	a, _ := newFakeConnPair(TransportWebSocket, "a", "b")
	adj := source.registerAdjacency(a, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	adj.rttEWMA = 20
	source.publishLocalTopology(context.Background())
	first := source.lastUpdate[1]
	target.handleTopologyUpdate(context.Background(), nil, first)
	// Jitter can change without triggering a meaningful RTT change. The periodic
	// publication must not reuse the version already accepted by the peer.
	adj.jitterEWMA = 7
	source.publishLocalTopology(context.Background())
	second := source.lastUpdate[1]
	if second.Generation <= first.Generation {
		t.Fatalf("changed metrics reused generation %d", first.Generation)
	}
	target.handleTopologyUpdate(context.Background(), nil, second)
	if !TopologyUpdatesEqual(source.lastUpdate[1], target.lastUpdate[1]) {
		t.Fatal("local and remote topology diverged")
	}
	source.publishLocalTopology(context.Background())
	if source.lastUpdate[1].Generation != second.Generation {
		t.Fatal("unchanged heartbeat needlessly changed generation")
	}
}

func TestRuntimeAdvertisesBestLiveParallelLink(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	for i := 0; i < 2; i++ {
		a, _ := newFakeConnPair(TransportWebSocket, "a", "b")
		adj := r.registerAdjacency(a, TransportWebSocket, &NodeHello{NodeId: 2}, i == 1)
		adj.rttEWMA = float64(10 + 90*i)
	}
	for i := 0; i < 20; i++ {
		r.pendingTombstones = append(r.pendingTombstones, &LinkAdvertisement{FromNodeId: 1, ToNodeId: 2, Transport: TransportWebSocket, Established: false})
		update := r.buildLocalTopologyUpdate()
		if len(update.Links) != 1 || !update.Links[0].Established || update.Links[0].CostMs != 10 {
			t.Fatalf("unstable or dead parallel link advertised: %+v", update.Links)
		}
	}
}
