//go:build zeromq

package api

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/mesh"
)

func clientTransientPointToPointThroughputExtraScenarios() []clientTransientPointToPointThroughputScenario {
	return []clientTransientPointToPointThroughputScenario{
		{name: "zeromq/direct-2nodes", build: buildClientTransientPointToPointZeroMQDirectTopology},
		{name: "zeromq/linear-7nodes", build: buildClientTransientPointToPointZeroMQLinear7NodesTopology},
	}
}

func buildClientTransientPointToPointZeroMQDirectTopology(tb testing.TB) *clientTransientPointToPointThroughputTopology {
	tb.Helper()

	targetCfg := benchmarkAPIClusterBaseConfig(2)
	targetCfg.ZeroMQ = cluster.ZeroMQConfig{
		Enabled: true,
		BindURL: benchmarkPointToPointAPINextZeroMQTCPAddress(tb),
	}
	targetNode, closeTarget := openBenchmarkAuthenticatedPointToPointAPINode(tb, "bench-api-p2p-zeromq-target", 2, &targetCfg)
	closeTargetListener := benchmarkPointToPointAPIStartZeroMQClusterListener(tb, targetNode.manager, targetCfg.ZeroMQ)
	targetPeerURL := benchmarkPointToPointAPIZeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceCfg := benchmarkAPIClusterBaseConfig(1)
	sourceCfg.ZeroMQ = cluster.ZeroMQConfig{
		Enabled: true,
	}
	sourceCfg.Peers = []cluster.Peer{{URL: targetPeerURL}}
	sourceNode, closeSource := openBenchmarkAuthenticatedPointToPointAPINode(tb, "bench-api-p2p-zeromq-source", 1, &sourceCfg)

	return &clientTransientPointToPointThroughputTopology{
		source:               sourceNode,
		target:               targetNode,
		expectedTransport:    mesh.TransportZeroMQ,
		sourceNextHopNodeID:  targetNode.nodeID,
		reverseNextHopNodeID: sourceNode.nodeID,
		deliveryTimeout:      clientTransientPointToPointThroughputTimeout,
		closeFns:             []func(){closeTarget, closeSource, closeTargetListener},
	}
}

func buildClientTransientPointToPointZeroMQLinear7NodesTopology(tb testing.TB) *clientTransientPointToPointThroughputTopology {
	tb.Helper()
	return buildClientTransientPointToPointLinearZeroMQTopology(tb, 7)
}

func buildClientTransientPointToPointLinearZeroMQTopology(tb testing.TB, nodeCount int) *clientTransientPointToPointThroughputTopology {
	tb.Helper()
	if nodeCount < 2 {
		tb.Fatalf("zeromq linear point-to-point throughput topology requires at least 2 nodes, got %d", nodeCount)
	}

	nodes := make([]benchmarkLinearMeshAPINode, 0, nodeCount)
	closeFns := make([]func(), 0, nodeCount*2)
	peerURLs := make([]string, 0, nodeCount)
	listenerClosers := make([]func(), 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeSlot := uint16(idx + 1)
		cfg := benchmarkAPIClusterBaseConfig(nodeSlot)
		cfg.ZeroMQ = cluster.ZeroMQConfig{
			Enabled: true,
			BindURL: benchmarkPointToPointAPINextZeroMQTCPAddress(tb),
		}
		if idx > 0 {
			cfg.Peers = []cluster.Peer{{URL: peerURLs[idx-1]}}
		}
		node, closeNode := openBenchmarkAuthenticatedPointToPointAPINode(tb, fmt.Sprintf("bench-api-p2p-zeromq-linear-%d", idx+1), nodeSlot, &cfg)
		nodes = append(nodes, node)
		closeFns = append(closeFns, closeNode)
		listenerClosers = append(listenerClosers, benchmarkPointToPointAPIStartZeroMQClusterListener(tb, node.manager, cfg.ZeroMQ))
		peerURLs = append(peerURLs, benchmarkPointToPointAPIZeroMQPeerURLForBindURL(cfg.ZeroMQ.BindURL))
	}
	closeFns = append(closeFns, listenerClosers...)

	return &clientTransientPointToPointThroughputTopology{
		source:               nodes[0],
		target:               nodes[len(nodes)-1],
		expectedTransport:    mesh.TransportZeroMQ,
		sourceNextHopNodeID:  nodes[1].nodeID,
		reverseNextHopNodeID: nodes[len(nodes)-2].nodeID,
		deliveryTimeout:      clientTransientPointToPointThroughputTimeout,
		closeFns:             closeFns,
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
