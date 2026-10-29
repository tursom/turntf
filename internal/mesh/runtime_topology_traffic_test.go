package mesh

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestQualityPublicationSkipsUnchangedAggregate(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	defer r.Close()
	conn, _ := newFakeConnPair(TransportTCPMTLS, "source", "target")
	adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(adj, 100, 0)
	r.publishLocalTopology(context.Background())
	before := r.lastUpdate[1]
	// A measurement on a worse parallel connection can mark quality dirty
	// without changing the single best link included in the advertisement.
	r.scheduleQualityPublish()
	r.publishQualityIfChanged(context.Background())
	if r.lastUpdate[1] != before {
		t.Fatal("unchanged aggregate was broadcast again")
	}
	setTestAdjacencyScore(adj, 104, 0)
	r.scheduleQualityPublish()
	r.publishQualityIfChanged(context.Background())
	if r.lastUpdate[1] != before {
		t.Fatal("sub-threshold quality change was broadcast")
	}
	// Compare against the last advertisement, so a series of small changes
	// eventually crosses the publication threshold rather than disappearing.
	setTestAdjacencyScore(adj, 111, 0)
	r.scheduleQualityPublish()
	r.publishQualityIfChanged(context.Background())
	if got := r.lastUpdate[1].Links[0].CostMs; got != 111 {
		t.Fatalf("accumulated quality change not published: cost=%d", got)
	}
}

func TestRTTChangesWaitForQualityWindow(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	defer r.Close()
	conn, _ := newFakeConnPair(TransportTCPMTLS, "source", "target")
	adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(adj, 100, 0)
	r.publishLocalTopology(context.Background())
	// Feed real probe responses, not the publication helper, to verify the
	// production measurement path cannot bypass the batching window.
	for id := uint64(1); id <= 100; id++ {
		start := time.Now().Add(-300 * time.Millisecond).Truncate(time.Millisecond)
		adj.mu.Lock()
		adj.inflightPings[id] = start
		adj.mu.Unlock()
		r.handleTimeSyncResponse(adj, &TimeSyncResponse{RequestId: id,
			ClientSendTimeMs: start.UnixMilli(), ServerReceiveTimeMs: start.UnixMilli(), ServerSendTimeMs: start.UnixMilli()})
	}
	if !r.qualityDirty.Load() {
		t.Fatal("probe changes did not mark pending quality")
	}
	select {
	case <-r.qualitySignal:
		t.Fatal("ordinary RTT changes requested immediate flooding")
	default:
	}
	r.publishQualityIfChanged(context.Background())
	if got := r.lastUpdate[1].Links[0].CostMs; got < 290 {
		t.Fatalf("batch did not retain latest measurement: cost=%d", got)
	}
}

func TestQualityPublisherCoalescesProbeBurst(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	r.qualityPublishPeriod = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	r.ctx, r.cancel = ctx, cancel
	defer r.Close()
	conn, peer := newFakeConnPair(TransportTCPMTLS, "source", "target")
	adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(adj, 100, 0)
	r.publishLocalTopology(ctx)
	receiveTestEnvelope(t, peer)
	r.wg.Add(1)
	go r.topologyPublishLoop(ctx)
	for id := uint64(1); id <= 100; id++ {
		start := time.Now().Add(-300 * time.Millisecond).Truncate(time.Millisecond)
		adj.mu.Lock()
		adj.inflightPings[id] = start
		adj.mu.Unlock()
		r.handleTimeSyncResponse(adj, &TimeSyncResponse{RequestId: id,
			ClientSendTimeMs: start.UnixMilli(), ServerReceiveTimeMs: start.UnixMilli(), ServerSendTimeMs: start.UnixMilli()})
	}
	select {
	case <-peer.inbox:
		t.Fatal("probe burst flooded before the quality window elapsed")
	case <-time.After(40 * time.Millisecond):
	}
	envelope := receiveTestEnvelope(t, peer)
	update := envelope.GetTopologyUpdate()
	if update == nil || len(update.Links) != 1 || update.Links[0].CostMs < 290 {
		t.Fatalf("quality window did not publish the newest aggregate: %+v", update)
	}
	// Repeated samples with an unchanged aggregate must not be sent again
	// on the next window. Periodic anti-entropy is tested separately.
	r.qualityDirty.Store(true)
	select {
	case <-peer.inbox:
		t.Fatal("unchanged aggregate was resent on the next quality window")
	case <-time.After(2 * r.qualityPublishPeriod):
	}
	t.Log("100 valid probe responses coalesced into one topology announcement; unchanged next window sent zero")
}

func TestUrgentQualityPublicationBypassesLongWindow(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	r.qualityPublishPeriod = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	r.ctx, r.cancel = ctx, cancel
	defer r.Close()
	conn, peer := newFakeConnPair(TransportTCPMTLS, "source", "target")
	adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(adj, 100, 0)
	r.publishLocalTopology(ctx)
	receiveTestEnvelope(t, peer)
	r.wg.Add(1)
	go r.topologyPublishLoop(ctx)
	r.recordAdjacencySend(conn, errors.New("failed"))
	r.recordAdjacencySend(conn, errors.New("failed again"))
	if cost := receiveTestEnvelope(t, peer).GetTopologyUpdate().Links[0].CostMs; cost != 200 {
		t.Fatalf("urgent failure penalty not published: %d", cost)
	}
	r.recordAdjacencySend(conn, nil)
	if cost := receiveTestEnvelope(t, peer).GetTopologyUpdate().Links[0].CostMs; cost != 100 {
		t.Fatalf("urgent recovery not published: %d", cost)
	}
}

func TestPeriodicTopologyRefreshBypassesQualityFilter(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	r.topologyPublishPeriod = 20 * time.Millisecond
	r.qualityPublishPeriod = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	r.ctx, r.cancel = ctx, cancel
	defer r.Close()
	conn, peer := newFakeConnPair(TransportWebSocket, "source", "target")
	r.registerAdjacency(conn, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	r.publishLocalTopology(ctx)
	first := receiveTestEnvelope(t, peer).GetTopologyUpdate()
	r.wg.Add(1)
	go r.topologyPublishLoop(ctx)
	if next := receiveTestEnvelope(t, peer).GetTopologyUpdate(); !TopologyUpdatesEqual(first, next) {
		t.Fatal("periodic refresh did not retain the unchanged immutable version")
	}
}

func TestTopologyFloodDoesNotEchoToParallelIngressOrOrigin(t *testing.T) {
	ws := newFakeAdapter(TransportWebSocket)
	tcp := newFakeAdapter(TransportTCPMTLS)
	r := newTestRuntime(t, 1, ws, func(o *RuntimeOptions) { o.Adapters = []TransportAdapter{ws, tcp} })
	defer r.Close()
	var echoes atomic.Int64
	var ingress *Adjacency
	for i, peer := range []int64{2, 2, 3} {
		kind := TransportWebSocket
		if i == 1 {
			kind = TransportTCPMTLS
		}
		conn, _ := newFakeConnPair(kind, "source", "target")
		conn.sendHook = func([]byte) error { echoes.Add(1); return nil }
		adj := r.registerAdjacency(conn, kind, &NodeHello{NodeId: peer}, false)
		if i == 0 {
			ingress = adj
		}
	}
	other, otherPeer := newFakeConnPair(TransportWebSocket, "other", "peer")
	r.registerAdjacency(other, TransportWebSocket, &NodeHello{NodeId: 4}, false)
	r.handleTopologyUpdate(context.Background(), ingress, &TopologyUpdate{OriginNodeId: 3, Generation: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := otherPeer.Receive(ctx); err != nil {
		t.Fatalf("other neighbor did not receive update: %v", err)
	}
	// This runtime has no background lifecycle loops. Wait for its flood
	// writers to drain so Close cannot discard a queued erroneous echo.
	r.wg.Wait()
	if got := echoes.Load(); got != 0 {
		t.Fatalf("sent %d needless echoes to ingress peer or origin", got)
	}
}
