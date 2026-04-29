//go:build zeromq

package cluster

import (
	"fmt"
	"testing"

	"github.com/tursom/turntf/internal/mesh"
)

func transientPointToPointBenchmarkExtraScenarios() []transientPointToPointBenchmarkScenario {
	return []transientPointToPointBenchmarkScenario{
		{name: "zeromq/direct-2nodes", build: buildTransientPointToPointZeroMQDirectTopology},
		{name: "zeromq/linear-7nodes", build: buildTransientPointToPointZeroMQLinear7NodesTopology},
		{name: "bridge/websocket-zeromq/3nodes", build: buildTransientPointToPointWebSocketZeroMQBridgeTopology},
		{name: "bridge/websocket-zeromq/5nodes", build: buildTransientPointToPointWebSocketZeroMQBridge5NodesTopology},
	}
}

func buildTransientPointToPointZeroMQDirectTopology(tb testing.TB) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-zeromq-target", 2)
	targetCfg := mixedTransportBaseConfig(2)
	targetCfg.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	targetMgr, _, closeTargetMgr := openBenchmarkMixedTransportManager(tb, targetCfg, targetStore)
	startZeroMQClusterListener(tb, targetMgr)
	targetPeerURL := zeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-zeromq-source", 1)
	sourceCfg := mixedTransportBaseConfig(1)
	sourceCfg.ZeroMQ = ZeroMQConfig{
		Enabled: true,
	}
	sourceCfg.Peers = []Peer{{URL: targetPeerURL}}
	sourceMgr, _, closeSourceMgr := openBenchmarkMixedTransportManager(tb, sourceCfg, sourceStore)

	waitForMeshRouteDecision(tb, sourceMgr, testNodeID(2), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportZeroMQ)

	waiter := newTransientPointToPointDeliveryWaiter()
	targetMgr.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          sourceMgr,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(2),
		hopCount:        1,
		deliveryTimeout: transientPointToPointBenchmarkTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr},
	}
}

func buildTransientPointToPointZeroMQLinear7NodesTopology(tb testing.TB) *transientPointToPointBenchmarkTopology {
	tb.Helper()
	return buildTransientPointToPointLinearZeroMQTopology(tb, 7)
}

func buildTransientPointToPointLinearZeroMQTopology(tb testing.TB, nodeCount int) *transientPointToPointBenchmarkTopology {
	tb.Helper()
	if nodeCount < 2 {
		tb.Fatalf("zeromq linear throughput topology requires at least 2 nodes, got %d", nodeCount)
	}

	managers := make([]*Manager, 0, nodeCount)
	closeFns := make([]func(), 0, nodeCount*2)
	peerURLs := make([]string, 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeSlot := uint16(idx + 1)
		st, closeStore := openBenchmarkSQLiteClusterStore(tb, fmt.Sprintf("bench-p2p-zeromq-linear-%d", idx+1), nodeSlot)
		closeFns = append(closeFns, closeStore)

		cfg := mixedTransportBaseConfig(nodeSlot)
		cfg.ZeroMQ = ZeroMQConfig{
			Enabled: true,
			BindURL: nextZeroMQTCPAddress(tb),
		}
		if idx > 0 {
			cfg.Peers = []Peer{{URL: peerURLs[idx-1]}}
		}
		mgr, _, closeMgr := openBenchmarkMixedTransportManager(tb, cfg, st)
		closeFns = append(closeFns, closeMgr)
		startZeroMQClusterListener(tb, mgr)
		managers = append(managers, mgr)
		peerURLs = append(peerURLs, zeroMQPeerURLForBindURL(cfg.ZeroMQ.BindURL))
	}

	source := managers[0]
	target := managers[len(managers)-1]
	targetNodeID := testNodeID(uint16(nodeCount))
	waitForMeshRouteDecision(tb, source, targetNodeID, mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportZeroMQ)

	waiter := newTransientPointToPointDeliveryWaiter()
	target.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          source,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    targetNodeID,
		hopCount:        int32(nodeCount - 1),
		deliveryTimeout: transientPointToPointBenchmarkTimeout,
		waiter:          waiter,
		closeFns:        closeFns,
	}
}

func buildTransientPointToPointWebSocketZeroMQBridgeTopology(tb testing.TB) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-target", 3)
	targetCfg := mixedTransportBaseConfig(3)
	targetCfg.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	targetMgr, _, closeTargetMgr := openBenchmarkMixedTransportManager(tb, targetCfg, targetStore)
	startZeroMQClusterListener(tb, targetMgr)
	targetPeerURL := zeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-source", 1)
	sourceMgr, sourceURL, closeSourceMgr := openBenchmarkMixedTransportManager(tb, mixedTransportBaseConfig(1), sourceStore)

	bridgeStore, closeBridgeStore := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-bridge", 2)
	bridgeCfg := mixedTransportBaseConfig(2)
	bridgeCfg.ZeroMQ = ZeroMQConfig{
		Enabled: true,
	}
	bridgeCfg.Peers = []Peer{
		{URL: websocketURL(sourceURL) + websocketPath},
		{URL: targetPeerURL},
	}
	_, _, closeBridgeMgr := openBenchmarkMixedTransportManager(tb, bridgeCfg, bridgeStore)

	waitForMeshRouteDecision(tb, sourceMgr, testNodeID(3), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(tb, targetMgr, testNodeID(1), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportZeroMQ)

	waiter := newTransientPointToPointDeliveryWaiter()
	targetMgr.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          sourceMgr,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(3),
		hopCount:        2,
		deliveryTimeout: transientPointToPointBenchmarkTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr, closeBridgeStore, closeBridgeMgr},
	}
}

func buildTransientPointToPointWebSocketZeroMQBridge5NodesTopology(tb testing.TB) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	st5, closeStore5 := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-large-5", 5)
	cfg5 := mixedTransportBaseConfig(5)
	cfg5.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	mgr5, _, closeMgr5 := openBenchmarkMixedTransportManager(tb, cfg5, st5)
	startZeroMQClusterListener(tb, mgr5)
	node5ZeroMQURL := zeroMQPeerURLForBindURL(cfg5.ZeroMQ.BindURL)

	st3, closeStore3 := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-large-3", 3)
	cfg3 := mixedTransportBaseConfig(3)
	cfg3.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	mgr3, serverURL3, closeMgr3 := openBenchmarkMixedTransportManager(tb, cfg3, st3)
	startZeroMQClusterListener(tb, mgr3)

	st1, closeStore1 := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-large-1", 1)
	mgr1, serverURL1, closeMgr1 := openBenchmarkMixedTransportManager(tb, mixedTransportBaseConfig(1), st1)

	st2, closeStore2 := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-large-2", 2)
	cfg2 := mixedTransportBaseConfig(2)
	cfg2.ZeroMQ = ZeroMQConfig{
		Enabled: true,
	}
	cfg2.Peers = []Peer{
		{URL: websocketURL(serverURL1) + websocketPath},
		{URL: zeroMQPeerURLForBindURL(cfg3.ZeroMQ.BindURL)},
	}
	_, _, closeMgr2 := openBenchmarkMixedTransportManager(tb, cfg2, st2)

	st4, closeStore4 := openBenchmarkSQLiteClusterStore(tb, "bench-p2p-bridge-zeromq-large-4", 4)
	cfg4 := mixedTransportBaseConfig(4)
	cfg4.ZeroMQ = ZeroMQConfig{
		Enabled: true,
	}
	cfg4.Peers = []Peer{
		{URL: websocketURL(serverURL3) + websocketPath},
		{URL: node5ZeroMQURL},
	}
	_, _, closeMgr4 := openBenchmarkMixedTransportManager(tb, cfg4, st4)

	waitForMeshRouteDecision(tb, mgr1, testNodeID(5), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)

	waiter := newTransientPointToPointDeliveryWaiter()
	mgr5.SetTransientHandler(waiter.handle)
	return &transientPointToPointBenchmarkTopology{
		source:          mgr1,
		sourceNodeID:    testNodeID(1),
		targetNodeID:    testNodeID(5),
		hopCount:        4,
		deliveryTimeout: transientPointToPointBenchmarkTimeout,
		waiter:          waiter,
		closeFns: []func(){
			closeStore5, closeMgr5,
			closeStore3, closeMgr3,
			closeStore1, closeMgr1,
			closeStore2, closeMgr2,
			closeStore4, closeMgr4,
		},
	}
}
