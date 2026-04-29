package cluster

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

type transientPointToPointBenchmarkScenario struct {
	name  string
	build func(testing.TB, benchroot.Mode) *transientPointToPointBenchmarkTopology
}

const transientPointToPointBenchmarkPacketIDBase = uint64(1) << 62

type transientPointToPointBenchmarkTopology struct {
	source          *Manager
	sourceNodeID    int64
	targetNodeID    int64
	hopCount        int32
	deliveryTimeout time.Duration
	waiter          *transientPointToPointDeliveryWaiter
	closeFns        []func()
}

type transientPointToPointExpectedDelivery struct {
	packet      store.TransientPacket
	payload     []byte
	expectedTTL int32
	done        chan error
}

type transientPointToPointDeliveryWaiter struct {
	mu      sync.Mutex
	pending map[uint64]transientPointToPointExpectedDelivery
	errs    chan error
}

func BenchmarkMeshTransientPointToPointThroughput(b *testing.B) {
	scenarios := append([]transientPointToPointBenchmarkScenario{
		{name: "single-node/local", build: buildTransientPointToPointSingleNodeTopology},
		{name: "websocket/direct-2nodes", build: buildTransientPointToPointWebSocketDirectTopology},
		{name: "libp2p/direct-2nodes", build: buildTransientPointToPointLibP2PDirectTopology},
		{name: "bridge/websocket-libp2p/3nodes", build: buildTransientPointToPointWebSocketLibP2PBridgeTopology},
	}, transientPointToPointBenchmarkExtraScenarios()...)

	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range scenarios {
				scenario := scenario
				for _, tc := range []struct {
					name        string
					payloadSize int
				}{
					{name: "256B", payloadSize: 256},
					{name: "4KiB", payloadSize: 4 << 10},
				} {
					b.Run(fmt.Sprintf("%s/%s", scenario.name, tc.name), func(b *testing.B) {
						benchmarkMeshTransientPointToPointThroughput(b, mode, scenario, tc.payloadSize)
					})
				}
			}
		})
	}
}

func benchmarkMeshTransientPointToPointThroughput(b *testing.B, mode benchroot.Mode, scenario transientPointToPointBenchmarkScenario, payloadSize int) {
	ctx := context.Background()
	b.StopTimer()
	silenceClusterLogs(b)

	topology := scenario.build(b, mode)
	b.Cleanup(topology.Close)

	payload := bytes.Repeat([]byte("t"), payloadSize)
	var packetID atomic.Uint64

	runClusterBenchmarkWarmup(b, func() {
		runTransientPointToPointBenchmarkDelivery(b, ctx, topology, payload, packetID.Add(1))
	})

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()
	b.SetParallelism(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			runTransientPointToPointBenchmarkDelivery(b, ctx, topology, payload, packetID.Add(1))
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(payloadSize), "bytes/op")
}

func (t *transientPointToPointBenchmarkTopology) Close() {
	if t == nil {
		return
	}
	for idx := len(t.closeFns) - 1; idx >= 0; idx-- {
		if t.closeFns[idx] != nil {
			t.closeFns[idx]()
		}
	}
}

func newTransientPointToPointDeliveryWaiter() *transientPointToPointDeliveryWaiter {
	return &transientPointToPointDeliveryWaiter{
		pending: make(map[uint64]transientPointToPointExpectedDelivery),
		errs:    make(chan error, 1),
	}
}

func (w *transientPointToPointDeliveryWaiter) handle(packet store.TransientPacket) bool {
	if w == nil {
		return true
	}

	w.mu.Lock()
	expected, ok := w.pending[packet.PacketID]
	if ok {
		delete(w.pending, packet.PacketID)
	}
	w.mu.Unlock()

	if !ok {
		w.reportError(fmt.Errorf("unexpected transient delivery packet_id=%d", packet.PacketID))
		return true
	}
	expected.done <- validateTransientPointToPointDelivery(packet, expected.packet, expected.payload, expected.expectedTTL)
	return true
}

func (w *transientPointToPointDeliveryWaiter) expect(packet store.TransientPacket, expectedTTL int32, payload []byte) <-chan error {
	done := make(chan error, 1)
	if w == nil {
		done <- fmt.Errorf("delivery waiter is not configured")
		return done
	}

	w.mu.Lock()
	if _, ok := w.pending[packet.PacketID]; ok {
		w.mu.Unlock()
		done <- fmt.Errorf("duplicate transient benchmark packet_id=%d", packet.PacketID)
		return done
	}
	w.pending[packet.PacketID] = transientPointToPointExpectedDelivery{
		packet:      packet,
		payload:     append([]byte(nil), payload...),
		expectedTTL: expectedTTL,
		done:        done,
	}
	w.mu.Unlock()
	return done
}

func (w *transientPointToPointDeliveryWaiter) wait(packetID uint64, done <-chan error, timeout time.Duration) error {
	if w == nil {
		return fmt.Errorf("delivery waiter is not configured")
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case err := <-w.errs:
		w.cancel(packetID)
		return err
	case <-timer.C:
		w.cancel(packetID)
		return fmt.Errorf("timed out waiting for transient delivery packet_id=%d within %s", packetID, timeout)
	}
}

func (w *transientPointToPointDeliveryWaiter) cancel(packetID uint64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	delete(w.pending, packetID)
	w.mu.Unlock()
}

func (w *transientPointToPointDeliveryWaiter) reportError(err error) {
	if w == nil || err == nil {
		return
	}
	select {
	case w.errs <- err:
	default:
	}
}

func validateTransientPointToPointDelivery(got, expected store.TransientPacket, payload []byte, expectedTTL int32) error {
	if got.PacketID != expected.PacketID {
		return fmt.Errorf("unexpected packet_id: got=%d want=%d", got.PacketID, expected.PacketID)
	}
	if got.SourceNodeID != expected.SourceNodeID || got.TargetNodeID != expected.TargetNodeID {
		return fmt.Errorf("unexpected packet route: got source=%d target=%d want source=%d target=%d", got.SourceNodeID, got.TargetNodeID, expected.SourceNodeID, expected.TargetNodeID)
	}
	if got.Recipient != expected.Recipient || got.Sender != expected.Sender {
		return fmt.Errorf("unexpected packet refs: got recipient=%+v sender=%+v want recipient=%+v sender=%+v", got.Recipient, got.Sender, expected.Recipient, expected.Sender)
	}
	if got.TTLHops != expectedTTL {
		return fmt.Errorf("unexpected packet ttl: got=%d want=%d", got.TTLHops, expectedTTL)
	}
	if !bytes.Equal(got.Body, payload) {
		return fmt.Errorf("unexpected packet body size=%d want=%d", len(got.Body), len(payload))
	}
	return nil
}

func runTransientPointToPointBenchmarkDelivery(tb testing.TB, ctx context.Context, topology *transientPointToPointBenchmarkTopology, payload []byte, packetID uint64) {
	tb.Helper()

	packet := store.TransientPacket{
		// Keep benchmark packet ids away from mesh runtime control/data packet ids
		// so transport-specific startup traffic cannot trigger deduplication.
		PacketID:     transientPointToPointBenchmarkPacketIDBase + packetID,
		SourceNodeID: topology.sourceNodeID,
		TargetNodeID: topology.targetNodeID,
		Recipient:    store.UserKey{NodeID: topology.targetNodeID, UserID: 99},
		Sender:       store.UserKey{NodeID: topology.sourceNodeID, UserID: 100},
		Body:         append([]byte(nil), payload...),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      defaultPacketTTLHops,
	}

	done := topology.waiter.expect(packet, packet.TTLHops-topology.hopCount, payload)
	if err := topology.source.RouteTransientPacket(ctx, packet); err != nil {
		tb.Fatalf("route transient packet for throughput benchmark: %v", err)
	}
	if err := topology.waiter.wait(packet.PacketID, done, topology.deliveryTimeout); err != nil {
		tb.Fatalf("wait for throughput transient delivery: %v", err)
	}
}

func buildTransientPointToPointSingleNodeTopology(tb testing.TB, mode benchroot.Mode) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)
	st, closeStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-single-node", 1)
	mgr, _, closeManager := openBenchmarkMixedTransportManager(tb, mixedTransportBaseConfig(1), st)
	waiter := newTransientPointToPointDeliveryWaiter()
	mgr.SetTransientHandler(waiter.handle)

	return &transientPointToPointBenchmarkTopology{
		source:          mgr,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(1),
		hopCount:        0,
		deliveryTimeout: deliveryTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeStore, closeManager},
	}
}

func buildTransientPointToPointWebSocketDirectTopology(tb testing.TB, mode benchroot.Mode) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-websocket-target", 2)
	targetMgr, targetURL, closeTargetMgr := openBenchmarkMixedTransportManager(tb, mixedTransportBaseConfig(2), targetStore)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-websocket-source", 1)
	sourceCfg := mixedTransportBaseConfig(1)
	sourceCfg.Peers = []Peer{{URL: websocketURL(targetURL) + websocketPath}}
	sourceMgr, _, closeSourceMgr := openBenchmarkMixedTransportManager(tb, sourceCfg, sourceStore)

	waitForMeshRouteDecision(tb, sourceMgr, testNodeID(2), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)

	waiter := newTransientPointToPointDeliveryWaiter()
	targetMgr.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          sourceMgr,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(2),
		hopCount:        1,
		deliveryTimeout: deliveryTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr},
	}
}

func buildTransientPointToPointLibP2PDirectTopology(tb testing.TB, mode benchroot.Mode) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-libp2p-target", 2)
	targetCfg := mixedTransportBaseConfig(2)
	targetCfg.LibP2P = mixedTransportLibP2PConfig(tb, "bench-p2p-libp2p-target")
	targetMgr, _, closeTargetMgr := openBenchmarkMixedTransportManager(tb, targetCfg, targetStore)
	targetPeerURL := firstLibP2PListenAddr(tb, targetMgr)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-libp2p-source", 1)
	sourceCfg := mixedTransportBaseConfig(1)
	sourceCfg.LibP2P = mixedTransportLibP2PConfig(tb, "bench-p2p-libp2p-source")
	sourceCfg.Peers = []Peer{{URL: targetPeerURL}}
	sourceMgr, _, closeSourceMgr := openBenchmarkMixedTransportManager(tb, sourceCfg, sourceStore)

	waitForMeshRouteDecision(tb, sourceMgr, testNodeID(2), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportLibP2P)

	waiter := newTransientPointToPointDeliveryWaiter()
	targetMgr.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          sourceMgr,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(2),
		hopCount:        1,
		deliveryTimeout: deliveryTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr},
	}
}

func buildTransientPointToPointWebSocketLibP2PBridgeTopology(tb testing.TB, mode benchroot.Mode) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-bridge-libp2p-target", 3)
	targetCfg := mixedTransportBaseConfig(3)
	targetCfg.LibP2P = mixedTransportLibP2PConfig(tb, "bench-p2p-bridge-libp2p-target")
	targetMgr, _, closeTargetMgr := openBenchmarkMixedTransportManager(tb, targetCfg, targetStore)
	targetPeerURL := firstLibP2PListenAddr(tb, targetMgr)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-bridge-libp2p-source", 1)
	sourceMgr, sourceURL, closeSourceMgr := openBenchmarkMixedTransportManager(tb, mixedTransportBaseConfig(1), sourceStore)

	bridgeStore, closeBridgeStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-bridge-libp2p-bridge", 2)
	bridgeCfg := mixedTransportBaseConfig(2)
	bridgeCfg.LibP2P = mixedTransportLibP2PConfig(tb, "bench-p2p-bridge-libp2p-bridge")
	bridgeCfg.Peers = []Peer{
		{URL: websocketURL(sourceURL) + websocketPath},
		{URL: targetPeerURL},
	}
	_, _, closeBridgeMgr := openBenchmarkMixedTransportManager(tb, bridgeCfg, bridgeStore)

	waitForMeshRouteDecision(tb, sourceMgr, testNodeID(3), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(tb, targetMgr, testNodeID(1), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportLibP2P)

	waiter := newTransientPointToPointDeliveryWaiter()
	targetMgr.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          sourceMgr,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(3),
		hopCount:        2,
		deliveryTimeout: deliveryTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr, closeBridgeStore, closeBridgeMgr},
	}
}

func openBenchmarkSQLiteClusterStore(tb testing.TB, mode benchroot.Mode, nodeID string, slot uint16) (*store.Store, func()) {
	tb.Helper()

	dir, cleanupDir := mode.MkdirTemp(tb, "turntf-cluster-p2p-bench-*")
	st, err := store.Open(filepath.Join(dir, nodeID+".db"), store.Options{
		NodeID:                     testNodeID(slot),
		MessageWindowSize:          store.DefaultMessageWindowSize,
		EventLogMaxEventsPerOrigin: store.DefaultEventLogMaxEventsPerOrigin,
	})
	if err != nil {
		cleanupDir()
		tb.Fatalf("open throughput benchmark sqlite store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("init throughput benchmark sqlite store: %v", err)
	}
	return st, func() {
		_ = st.Close()
		cleanupDir()
	}
}
