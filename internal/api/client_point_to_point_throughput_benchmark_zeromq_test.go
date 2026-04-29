//go:build zeromq

package api

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

func clientTransientPointToPointThroughputExtraScenarios() []clientTransientPointToPointThroughputScenario {
	return []clientTransientPointToPointThroughputScenario{
		{name: "zeromq/direct-2nodes", build: buildClientTransientPointToPointZeroMQDirectTopology},
	}
}

func buildClientTransientPointToPointZeroMQDirectTopology(tb testing.TB, mode benchroot.Mode) *clientTransientPointToPointThroughputTopology {
	tb.Helper()

	targetCfg := benchmarkAPIClusterBaseConfig(2)
	targetCfg.ZeroMQ = cluster.ZeroMQConfig{
		Enabled: true,
		BindURL: benchmarkPointToPointAPINextZeroMQTCPAddress(tb),
	}
	targetNode, closeTarget := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-zeromq-target", 2, &targetCfg)
	closeTargetListener := benchmarkPointToPointAPIStartZeroMQClusterListener(tb, targetNode.manager, targetCfg.ZeroMQ)
	targetPeerURL := benchmarkPointToPointAPIZeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceCfg := benchmarkAPIClusterBaseConfig(1)
	sourceCfg.ZeroMQ = cluster.ZeroMQConfig{
		Enabled: true,
	}
	sourceCfg.Peers = []cluster.Peer{{URL: targetPeerURL}}
	sourceNode, closeSource := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-zeromq-source", 1, &sourceCfg)

	return &clientTransientPointToPointThroughputTopology{
		source:            sourceNode,
		target:            targetNode,
		expectedTransport: mesh.TransportZeroMQ,
		deliveryTimeout:   benchmarkAPIClientTimeout(mode, 5*time.Second, 30*time.Second),
		closeFns:          []func(){closeTarget, closeSource, closeTargetListener},
	}
}

func benchmarkPointToPointAPIStartZeroMQClusterListener(tb testing.TB, manager *cluster.Manager, cfg cluster.ZeroMQConfig) func() {
	tb.Helper()
	if manager == nil {
		tb.Fatalf("expected zeromq benchmark manager")
	}

	ctx, cancel := context.WithCancel(context.Background())
	listener := cluster.NewZeroMQMuxListenerWithConfig(cfg.BindURL, cfg)
	listener.SetClusterAccept(manager.AcceptZeroMQConn)
	if err := listener.Start(ctx); err != nil {
		cancel()
		tb.Fatalf("start zeromq benchmark cluster listener: %v", err)
	}
	manager.SetZeroMQListenerRunning(true)
	return func() {
		manager.SetZeroMQListenerRunning(false)
		cancel()
		_ = listener.Close()
	}
}

func benchmarkPointToPointAPINextZeroMQTCPAddress(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen for zeromq benchmark address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		tb.Fatalf("close zeromq benchmark address listener: %v", err)
	}
	return "tcp://" + addr
}

func benchmarkPointToPointAPIZeroMQPeerURLForBindURL(bindURL string) string {
	trimmed := strings.TrimSpace(bindURL)
	if strings.HasPrefix(strings.ToLower(trimmed), "zmq+tcp://") {
		return trimmed
	}
	return "zmq+tcp://" + strings.TrimPrefix(trimmed, "tcp://")
}
