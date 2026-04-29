//go:build zeromq

package cluster

import (
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

func transientPointToPointBenchmarkExtraScenarios() []transientPointToPointBenchmarkScenario {
	return []transientPointToPointBenchmarkScenario{
		{name: "zeromq/direct-2nodes", build: buildTransientPointToPointZeroMQDirectTopology},
		{name: "bridge/websocket-zeromq/3nodes", build: buildTransientPointToPointWebSocketZeroMQBridgeTopology},
	}
}

func buildTransientPointToPointZeroMQDirectTopology(tb testing.TB, mode benchroot.Mode) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-zeromq-target", 2)
	targetCfg := mixedTransportBaseConfig(2)
	targetCfg.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	targetMgr, _, closeTargetMgr := openBenchmarkMixedTransportManager(tb, targetCfg, targetStore)
	startZeroMQClusterListener(tb, targetMgr)
	targetPeerURL := zeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-zeromq-source", 1)
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
		deliveryTimeout: deliveryTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr},
	}
}

func buildTransientPointToPointWebSocketZeroMQBridgeTopology(tb testing.TB, mode benchroot.Mode) *transientPointToPointBenchmarkTopology {
	tb.Helper()

	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	targetStore, closeTargetStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-bridge-zeromq-target", 3)
	targetCfg := mixedTransportBaseConfig(3)
	targetCfg.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	targetMgr, _, closeTargetMgr := openBenchmarkMixedTransportManager(tb, targetCfg, targetStore)
	startZeroMQClusterListener(tb, targetMgr)
	targetPeerURL := zeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceStore, closeSourceStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-bridge-zeromq-source", 1)
	sourceMgr, sourceURL, closeSourceMgr := openBenchmarkMixedTransportManager(tb, mixedTransportBaseConfig(1), sourceStore)

	bridgeStore, closeBridgeStore := openBenchmarkSQLiteClusterStore(tb, mode, "bench-p2p-bridge-zeromq-bridge", 2)
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
		deliveryTimeout: deliveryTimeout,
		waiter:          waiter,
		closeFns:        []func(){closeTargetStore, closeTargetMgr, closeSourceStore, closeSourceMgr, closeBridgeStore, closeBridgeMgr},
	}
}
