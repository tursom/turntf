package api

import (
	"testing"

	"notifier/internal/app"
)

func TestMergePeerStatusKeepsConfiguredPeersWithoutDiscoveredNodeID(t *testing.T) {
	t.Parallel()

	peers := mergePeerStatus(nil, []app.ClusterPeerStatus{
		{ConfiguredURL: "ws://127.0.0.1:9082/internal/cluster/ws"},
		{ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws"},
	})

	if len(peers) != 2 {
		t.Fatalf("unexpected peer count: got=%d want=2", len(peers))
	}
	if peers[0].ConfiguredURL != "ws://127.0.0.1:9081/internal/cluster/ws" || peers[0].NodeID != 0 {
		t.Fatalf("unexpected first peer: %+v", peers[0])
	}
	if peers[1].ConfiguredURL != "ws://127.0.0.1:9082/internal/cluster/ws" || peers[1].NodeID != 0 {
		t.Fatalf("unexpected second peer: %+v", peers[1])
	}
}
