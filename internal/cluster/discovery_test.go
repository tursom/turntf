package cluster

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func TestDiscoveryBroadcastsVerifiedPeerURLsAndDialsDynamicPeer(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, DiscoveryEnabled: true},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}, DiscoveryEnabled: true},
		clusterTestNodeSpec{Name: "node-c", Slot: 3, DiscoveryEnabled: true},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")
	nodeC := scenario.Node("node-c")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeB, nodeA.id),
		expectClusterPeer(nodeB, nodeC.id),
	)
	scenario.WaitForPeersActive(15*time.Second,
		expectClusterPeer(nodeA, nodeC.id),
		expectClusterPeer(nodeC, nodeA.id),
	)

	status, err := nodeA.manager.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	found := false
	for _, peer := range status.Peers {
		if peer.NodeID != nodeC.id {
			continue
		}
		found = true
		if peer.Source != peerSourceDiscovered {
			t.Fatalf("expected discovered source for node-c, got %+v", peer)
		}
		if peer.DiscoveredURL == "" || peer.DiscoveryState != discoveryStateConnected {
			t.Fatalf("expected connected discovered peer details, got %+v", peer)
		}
	}
	if !found {
		t.Fatalf("node-a status did not include discovered node-c peer: %+v", status.Peers)
	}
}

func TestMembershipUpdateRejectsInvalidAdvertisements(t *testing.T) {
	node := newClusterTestNodeFixture(t, clusterTestNodeConfig{
		Name:             "node-a",
		Slot:             1,
		DiscoveryEnabled: true,
	})
	mgr := node.manager
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}

	err := mgr.handleMembershipUpdate(sess, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_MembershipUpdate{
			MembershipUpdate: &internalproto.MembershipUpdate{
				OriginNodeId: testNodeID(2),
				Peers: []*internalproto.PeerAdvertisement{
					{NodeId: testNodeID(3), Url: "http://127.0.0.1/internal/cluster/ws"},
					{NodeId: testNodeID(3), Url: ""},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("handle membership update: %v", err)
	}
	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Discovery.RejectedTotal != 2 {
		t.Fatalf("expected two rejected advertisements, got %+v", status.Discovery)
	}
	if status.Discovery.DiscoveredPeers != 0 {
		t.Fatalf("expected no discovered peers, got %+v", status.Discovery)
	}
}
