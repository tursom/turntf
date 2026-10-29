package mesh

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestTopologyFloodOneCopyPerNeighborAndFallback(t *testing.T) {
	for _, failPrimary := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "fallback"}[failPrimary], func(t *testing.T) {
			r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
			defer r.Close()
			tcp, tcpPeer := newFakeConnPair(TransportTCPMTLS, "tcp", "tcp-peer")
			ws, wsPeer := newFakeConnPair(TransportWebSocket, "ws", "ws-peer")
			a := r.registerAdjacency(tcp, TransportTCPMTLS, &NodeHello{NodeId: 3}, false)
			b := r.registerAdjacency(ws, TransportWebSocket, &NodeHello{NodeId: 3}, false)
			a.rttEWMA, b.rttEWMA = 1, 10
			if failPrimary {
				tcp.sendHook = func([]byte) error { return io.ErrClosedPipe }
			}
			other, otherPeer := newFakeConnPair(TransportWebSocket, "other", "other-peer")
			r.registerAdjacency(other, TransportWebSocket, &NodeHello{NodeId: 4}, false)
			r.handleTopologyUpdate(context.Background(), nil, &TopologyUpdate{OriginNodeId: 2, Generation: 1})
			waitFor(t, time.Second, func() bool {
				r.mu.Lock()
				defer r.mu.Unlock()
				return len(r.topologyFloods) == 0
			})
			if got := len(tcpPeer.inbox) + len(wsPeer.inbox); got != 1 || len(otherPeer.inbox) != 1 {
				t.Fatalf("copies: parallel peer=%d other=%d", got, len(otherPeer.inbox))
			}
			if failPrimary && len(wsPeer.inbox) != 1 {
				t.Fatal("failed primary did not fall back")
			}
			// 本地发布也必须去重，不能仅优化转发收到的公告。
			r.publishLocalTopology(context.Background())
			waitFor(t, time.Second, func() bool {
				r.mu.Lock()
				defer r.mu.Unlock()
				return len(r.topologyFloods) == 0
			})
			if got := len(tcpPeer.inbox) + len(wsPeer.inbox); got != 2 {
				t.Fatalf("local publish duplicated: %d", got)
			}
		})
	}
}

func TestPresenceDigestCapabilityRequiresAllDirectConnections(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, _ := newFakeConnPair(TransportWebSocket, "new", "new-peer")
	b, _ := newFakeConnPair(TransportWebSocket, "old", "old-peer")
	r.registerAdjacency(a, TransportWebSocket, &NodeHello{NodeId: 2, PresenceDigestSupported: true}, false)
	if !r.SupportsPresenceDigest(2) || r.SupportsPresenceDigest(3) {
		t.Fatal("direct/unknown capability incorrect")
	}
	r.registerAdjacency(b, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	if r.SupportsPresenceDigest(2) {
		t.Fatal("mixed old/new connections must use legacy repair")
	}
}

func TestTopologyFloodPendingSurvivesSelectedConnectionLoss(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, _ := newFakeConnPair(TransportWebSocket, "a", "a-peer")
	slow := &stalledTopologyConn{TransportConn: a, entered: make(chan struct{}, 1), release: make(chan struct{})}
	first := r.registerAdjacency(slow, TransportWebSocket, &NodeHello{NodeId: 3}, false)
	backup, bp := newFakeConnPair(TransportWebSocket, "b", "b-peer")
	second := r.registerAdjacency(backup, TransportWebSocket, &NodeHello{NodeId: 3}, false)
	second.rttEWMA = 10
	r.queueTopologyFloodNode(3, &TopologyUpdate{OriginNodeId: 2, Generation: 1})
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("selected writer did not start")
	}
	r.queueTopologyFloodNode(3, &TopologyUpdate{OriginNodeId: 2, Generation: 2})
	_ = slow.Close()
	r.onAdjacencyLost(first)
	close(slow.release)
	waitFor(t, time.Second, func() bool { return len(bp.inbox) == 2 })
}

func TestStandbyProbeIdleCadenceActivityAndDeadline(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, _ := newFakeConnPair(TransportTCPMTLS, "primary", "primary-peer")
	b, bp := newFakeConnPair(TransportWebSocket, "backup", "backup-peer")
	primary := r.registerAdjacency(a, TransportTCPMTLS, &NodeHello{NodeId: 2}, false)
	backup := r.registerAdjacency(b, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	primary.samples, backup.samples = 8, 8
	primary.rttEWMA, backup.rttEWMA = 10, 20
	now := time.Now()
	if got := r.standbyProbeInterval(backup, now); got != 4*r.pingInterval {
		t.Fatalf("idle standby interval %v", got)
	}
	if got := r.standbyProbeInterval(primary, now); got != r.pingInterval {
		t.Fatalf("primary interval %v", got)
	}
	backup.lastProbeSent = now
	r.sendPing(context.Background(), backup)
	if len(bp.inbox) != 0 {
		t.Fatal("idle backup was probed before its interval")
	}
	backup.lastBusinessActivity.Store(now.UnixNano())
	r.sendPing(context.Background(), backup)
	if len(bp.inbox) != 1 {
		t.Fatal("business activity did not restore normal probing")
	}
	backup.mu.Lock()
	clear(backup.inflightPings)
	backup.lastProbeSent = now.Add(-9 * time.Second)
	backup.lastProbeSuccess = now.Add(-29 * time.Second)
	backup.lastBusinessActivity.Store(0)
	backup.mu.Unlock()
	r.sendPing(context.Background(), backup)
	backup.mu.Lock()
	deadline := backup.probeDeadline
	backup.probeDeadline = time.Now().Add(-time.Millisecond)
	backup.mu.Unlock()
	if deadline.After(now.Add(2 * time.Second)) {
		t.Fatalf("standby stretched liveness deadline: %v", deadline)
	}
	r.sendPing(context.Background(), backup)
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if !closed {
		t.Fatal("standby stayed alive past its bounded response deadline")
	}
}
