package mesh

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockingQualityPersistence struct {
	entered chan struct{}
	release chan struct{}
}

func (p *blockingQualityPersistence) Load() (uint64, error) { return 0, nil }
func (p *blockingQualityPersistence) Store(uint64) error {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-p.release
	return nil
}

func TestParallelAdjacencyCostPenalizesFailureAndRecentJitter(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	primaryConn, _ := newFakeConnPair(TransportTCPMTLS, "primary", "peer")
	alternateConn, _ := newFakeConnPair(TransportTCPMTLS, "alternate", "peer")
	primary := r.registerAdjacency(primaryConn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	alternate := r.registerAdjacency(alternateConn, TransportTCPMTLS, &NodeHello{NodeId: 2}, true)
	setTestAdjacencyScore(primary, 100, 5)
	setTestAdjacencyScore(alternate, 140, 0)
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != primary {
		t.Fatal("initially healthy primary was not selected")
	}

	r.recordAdjacencySend(primaryConn, errors.New("write failed"))
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != primary {
		t.Fatal("one transient failure switched adjacency")
	}
	r.recordAdjacencySend(primaryConn, errors.New("write failed again"))
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != alternate {
		t.Fatal("repeated failures did not select healthy parallel adjacency")
	}
	update := r.buildLocalTopologyUpdate()
	if len(update.Links) != 1 || update.Links[0].CostMs != 140 || update.Links[0].JitterMs != 0 {
		t.Fatalf("advertised cost did not select healthy connection: %+v", update.Links)
	}
	r.recordAdjacencySend(primaryConn, nil)
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != primary {
		t.Fatal("successful send did not restore original adjacency")
	}
	update = r.buildLocalTopologyUpdate()
	if len(update.Links) != 1 || update.Links[0].CostMs != 105 || update.Links[0].JitterMs != 5 {
		t.Fatalf("advertised RTT/jitter cost = %+v", update.Links)
	}
	setTestAdjacencyScore(primary, 100, 25)
	if cost := r.buildLinkAdvertisementLocked(primary, true); cost.CostMs != 125 || cost.JitterMs != 25 {
		t.Fatalf("recent jitter not represented in existing cost labels: %+v", cost)
	}
	setTestAdjacencyScore(alternate, 148, 0)
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != alternate {
		t.Fatal("recent jitter did not affect physical adjacency selection")
	}
}

func TestPendingProbePenalizesBeforeLivenessTimeoutAndRecovers(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS), func(o *RuntimeOptions) {
		o.PingInterval = 10 * time.Millisecond
		o.LivenessTimeout = time.Second
	})
	primaryConn, _ := newFakeConnPair(TransportTCPMTLS, "primary", "peer")
	alternateConn, _ := newFakeConnPair(TransportTCPMTLS, "alternate", "peer")
	primary := r.registerAdjacency(primaryConn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	alternate := r.registerAdjacency(alternateConn, TransportTCPMTLS, &NodeHello{NodeId: 2}, true)
	setTestAdjacencyScore(primary, 100, 0)
	setTestAdjacencyScore(alternate, 130, 0)
	r.sendPing(context.Background(), primary)
	primary.mu.Lock()
	var id uint64
	var started time.Time
	for id, started = range primary.inflightPings {
		break
	}
	primary.pingStarted = time.Now().Add(-4 * r.pingInterval)
	primary.mu.Unlock()
	r.sendPing(context.Background(), primary)
	if !primary.probeDegraded || len(primary.inflightPings) != 1 {
		t.Fatal("stale probe was discarded before liveness timeout")
	}
	select {
	case <-primaryConn.closeCh:
		t.Fatal("quality penalty closed a still-live connection")
	default:
	}
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != alternate {
		t.Fatal("unanswered probe did not favor healthy parallel connection")
	}
	update := r.buildLocalTopologyUpdate()
	if len(update.Links) != 1 || update.Links[0].CostMs != 130 {
		t.Fatalf("topology did not choose responsive connection: %+v", update.Links)
	}
	// A valid response removes the penalty and restores the original RTT path.
	r.handleTimeSyncResponse(primary, &TimeSyncResponse{
		RequestId: id, ClientSendTimeMs: started.UnixMilli(),
		ServerReceiveTimeMs: started.UnixMilli(), ServerSendTimeMs: started.UnixMilli(),
	})
	if primary.probeDegraded || len(primary.inflightPings) != 0 {
		t.Fatal("matching response did not clear probe penalty")
	}
	if got := r.bestAdjacency(2, TransportTCPMTLS); got != primary {
		t.Fatal("recovered probe did not restore healthy primary")
	}
}

func TestBusinessSendQualityIgnoresSuccessfulControlFrames(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	conn, _ := newFakeConnPair(TransportTCPMTLS, "source", "target")
	adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(adj, 100, 0)
	r.publishLocalTopology(context.Background())
	conn.sendHook = func(raw []byte) error {
		envelope, err := r.codec.Decode(raw)
		if err != nil {
			return err
		}
		if envelope.GetForwardedPacket() != nil {
			return errors.New("business send failed")
		}
		return nil
	}
	business := &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: &ForwardedPacket{}}}
	for i := 0; i < 2; i++ {
		if err := r.sendEnvelopeCtx(context.Background(), conn, business, time.Second); err == nil {
			t.Fatal("injected business send unexpectedly succeeded")
		}
	}
	if !r.qualityDirty.Load() {
		t.Fatal("business failure did not schedule quality publication")
	}
	r.publishQualityIfChanged(context.Background())
	if adj.sendFailures != 2 || r.lastUpdate[1].Links[0].CostMs != 200 {
		t.Fatalf("business failures not published: failures=%d links=%+v", adj.sendFailures, r.lastUpdate[1].Links)
	}
	control := &ClusterEnvelope{Body: &ClusterEnvelope_TimeSyncRequest{TimeSyncRequest: &TimeSyncRequest{RequestId: 1}}}
	if err := r.sendEnvelopeCtx(context.Background(), conn, control, time.Second); err != nil {
		t.Fatal(err)
	}
	if adj.sendFailures != 2 || r.lastUpdate[1].Links[0].CostMs != 200 {
		t.Fatal("successful control send cleared business failure penalty")
	}
	if err := r.sendEnvelopeCtx(context.Background(), conn, &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: &TopologyUpdate{}}}, time.Second); err != nil {
		t.Fatal(err)
	}
	if adj.sendFailures != 2 {
		t.Fatal("successful topology send cleared business failure penalty")
	}
	if err := r.sendEnvelopeCtx(context.Background(), conn, business, time.Second); err == nil {
		t.Fatal("third business failure unexpectedly succeeded")
	}
	r.publishQualityIfChanged(context.Background())
	if r.lastUpdate[1].Links[0].CostMs != 300 {
		t.Fatalf("third failure cost not published: %+v", r.lastUpdate[1].Links)
	}
	conn.mu.Lock()
	conn.sendHook = nil
	conn.mu.Unlock()
	if err := r.sendEnvelopeCtx(context.Background(), conn, business, time.Second); err != nil {
		t.Fatal(err)
	}
	r.publishQualityIfChanged(context.Background())
	if adj.sendFailures != 0 || r.lastUpdate[1].Links[0].CostMs != 100 {
		t.Fatalf("business recovery cost not published: failures=%d links=%+v", adj.sendFailures, r.lastUpdate[1].Links)
	}
}

func TestBusinessSendDoesNotWaitForQualityPersistence(t *testing.T) {
	persistence := &blockingQualityPersistence{entered: make(chan struct{}, 1), release: make(chan struct{})}
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS), func(o *RuntimeOptions) {
		o.GenerationPersistence = persistence
	})
	conn, _ := newFakeConnPair(TransportTCPMTLS, "source", "target")
	adj := r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	business := &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: &ForwardedPacket{}}}
	conn.sendHook = func([]byte) error { return errors.New("business send failed") }
	for i := 0; i < 2; i++ {
		if err := r.sendEnvelopeCtx(context.Background(), conn, business, time.Second); err == nil {
			t.Fatal("injected business send unexpectedly succeeded")
		}
	}
	flushDone := make(chan struct{})
	go func() {
		r.publishQualityIfChanged(context.Background())
		close(flushDone)
	}()
	select {
	case <-persistence.entered:
	case <-time.After(time.Second):
		close(persistence.release)
		t.Fatal("quality publication did not reach persistence")
	}
	released := false
	defer func() {
		if !released {
			close(persistence.release)
		}
	}()
	conn.mu.Lock()
	conn.sendHook = nil
	conn.mu.Unlock()
	sendDone := make(chan error, 1)
	go func() { sendDone <- r.sendEnvelopeCtx(context.Background(), conn, business, time.Second) }()
	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("business send waited for topology persistence")
	}
	if !r.qualityDirty.Load() {
		t.Fatal("recovery during publication was not queued for the next tick")
	}
	adj.mu.Lock()
	adj.pingStarted = time.Now().Add(-2 * r.livenessTimeout)
	adj.inflightPings[77] = time.Now().Add(-2 * r.livenessTimeout)
	adj.mu.Unlock()
	probeDone := make(chan struct{})
	go func() {
		r.sendPing(context.Background(), adj)
		close(probeDone)
	}()
	select {
	case <-probeDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("liveness probe waited for topology persistence")
	}
	select {
	case <-conn.closeCh:
	default:
		t.Fatal("expired probe did not close half-open connection")
	}
	close(persistence.release)
	released = true
	<-flushDone
	r.publishQualityIfChanged(context.Background())
}

func TestDirectStreamStartupKeepsTCPPreferenceDespiteCost(t *testing.T) {
	tcp := newFakeAdapter(TransportTCPMTLS)
	ws := newFakeAdapter(TransportWebSocket)
	r := newTestRuntime(t, 1, tcp, func(o *RuntimeOptions) { o.Adapters = []TransportAdapter{tcp, ws} })
	tcpConn, tcpPeer := newFakeConnPair(TransportTCPMTLS, "source", "tcp")
	wsConn, wsPeer := newFakeConnPair(TransportWebSocket, "source", "ws")
	tcpAdj := r.registerAdjacency(tcpConn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	wsAdj := r.registerAdjacency(wsConn, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(tcpAdj, 900, 0)
	setTestAdjacencyScore(wsAdj, 1, 0)
	for _, frame := range []*ClusterEnvelope{testStreamEnvelopeFor(streamFrameKindOpen, 1),
		{Body: &ClusterEnvelope_StreamFrame{StreamFrame: &StreamFrame{Kind: streamFrameKindOpen}}}} {
		if err := r.RouteEnvelope(context.Background(), 2, frame); err != nil {
			t.Fatal(err)
		}
		receiveTestEnvelope(t, tcpPeer)
		select {
		case <-wsPeer.inbox:
			t.Fatal("startup or legacy stream bypassed established TCP for cheaper WSS")
		default:
		}
	}
}

func TestQualityChangesReachTopologyPublisher(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportTCPMTLS))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	conn, _ := newFakeConnPair(TransportTCPMTLS, "source", "target")
	r.registerAdjacency(conn, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	setTestAdjacencyScore(r.bestAdjacency(2, TransportTCPMTLS), 100, 0)
	r.recordAdjacencySend(conn, errors.New("failed"))
	r.recordAdjacencySend(conn, errors.New("failed again"))
	waitCost := func(want uint32) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			r.mu.Lock()
			update := r.lastUpdate[1]
			found := update != nil && len(update.Links) == 1 && update.Links[0].CostMs == want
			r.mu.Unlock()
			if found {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("topology publisher did not advertise cost %d", want)
	}
	waitCost(200)
	r.recordAdjacencySend(conn, nil)
	waitCost(100)
}
