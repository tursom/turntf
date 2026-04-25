package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

type linearWebSocketStoreFactory func(testing.TB, string, uint16) *store.Store

func startLinearMeshManagers(tb testing.TB, configFns ...func(nodeSlot int, cfg *Config)) (*Manager, *Manager, *Manager) {
	tb.Helper()

	managers := startLinearWebSocketManagersWithStoreFactory(tb, 3, nil, configFns...)
	return managers[0], managers[1], managers[2]
}

func startLinearWebSocketManagers(tb testing.TB, nodeCount int) []*Manager {
	tb.Helper()
	return startLinearWebSocketManagersWithStoreFactory(tb, nodeCount, nil)
}

func startLinearWebSocketManagersWithStoreFactory(tb testing.TB, nodeCount int, storeFactory linearWebSocketStoreFactory, configFns ...func(nodeSlot int, cfg *Config)) []*Manager {
	tb.Helper()
	if nodeCount < 2 {
		tb.Fatalf("linear websocket cluster requires at least 2 nodes, got %d", nodeCount)
	}
	if storeFactory == nil {
		storeFactory = func(tb testing.TB, nodeID string, slot uint16) *store.Store {
			return newReplicationTestStore(tb, nodeID, slot)
		}
	}

	applyConfig := func(nodeSlot int, cfg *Config) {
		for _, fn := range configFns {
			if fn != nil {
				fn(nodeSlot, cfg)
			}
		}
	}

	managers := make([]*Manager, 0, nodeCount)
	serverURLs := make([]string, 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeSlot := uint16(idx + 1)
		st := storeFactory(tb, fmt.Sprintf("linear-websocket-node-%d", idx+1), nodeSlot)
		cfg := mixedTransportBaseConfig(nodeSlot)
		if idx > 0 {
			cfg.Peers = []Peer{{URL: websocketURL(serverURLs[idx-1]) + websocketPath}}
		}
		applyConfig(int(nodeSlot), &cfg)
		mgr, serverURL := startMixedTransportManager(tb, cfg, st)
		managers = append(managers, mgr)
		serverURLs = append(serverURLs, serverURL)
	}
	return managers
}

func waitForMeshRoute(tb testing.TB, mgr *Manager, destinationNodeID int64, trafficClass mesh.TrafficClass) {
	tb.Helper()
	waitFor(tb, 5*time.Second, func() bool {
		binding := mgr.MeshRuntime()
		if binding == nil {
			return false
		}
		planner := mesh.NewPlanner(mgr.cfg.NodeID)
		_, ok := planner.Compute(binding.TopologyStore().Snapshot(), destinationNodeID, trafficClass, mesh.TransportUnspecified)
		return ok
	})
}
