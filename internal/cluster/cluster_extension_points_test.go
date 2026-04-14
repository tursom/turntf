package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestExtensionPointHandshakeObservation(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	observed := scenario.WaitForHelloOnLink("node-a", "node-b", "node-b", clusterTestHelloAll(
		clusterTestHelloNodeID(nodeA.id),
		clusterTestHelloProtocolVersion(internalproto.ProtocolVersion),
		clusterTestHelloSnapshotVersion(internalproto.SnapshotVersion),
		clusterTestHelloSupportsRouting(true),
		clusterTestHelloConnectionIDNonZero(),
	), 10*time.Second)
	if observed.LinkAlias != "node-b" {
		t.Fatalf("unexpected hello link alias: got=%q want=%q", observed.LinkAlias, "node-b")
	}
	scenario.WaitForHandshakeAccepted("node-a", "node-b", 10*time.Second)
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)
}

func TestExtensionPointMembershipLinkAliases(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{
			Name: "node-a",
			Slot: 1,
			PeerLinks: []clusterTestPeerLinkSpec{
				{Alias: "primary", Target: "node-b"},
				{Alias: "discovery-duplicate", Target: "node-b", InitiallyBlocked: true},
			},
		},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForHelloOnLink("node-a", "node-b", "primary", clusterTestHelloNodeID(nodeA.id), 10*time.Second)
	scenario.WaitForHandshakeAccepted("node-a", "node-b", 10*time.Second)

	scenario.DisableLink("node-a", "node-b", "primary")
	scenario.EnableLink("node-a", "node-b", "discovery-duplicate")
	scenario.HealLink("node-a", "node-b", "discovery-duplicate")
	observed := scenario.WaitForHelloOnLink("node-a", "node-b", "discovery-duplicate", clusterTestHelloAll(
		clusterTestHelloNodeID(nodeA.id),
		clusterTestHelloProtocolVersion(internalproto.ProtocolVersion),
	), 20*time.Second)
	if observed.LinkAlias != "discovery-duplicate" {
		t.Fatalf("unexpected duplicate discovery link alias: got=%q", observed.LinkAlias)
	}

	scenario.WaitForPeersActive(10*time.Second, expectClusterPeer(nodeB, nodeA.id))
}

func TestExtensionPointCustomSemanticAssertion(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)
	user, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "semantic-extension-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create semantic extension user: %v", err)
	}

	scenario.WaitForAllNodesSatisfy(10*time.Second, func(ctx context.Context, node *clusterTestNode) error {
		loaded, err := node.store.GetUser(ctx, user.Key())
		if err != nil {
			return err
		}
		if loaded.Username != "semantic-extension-user" {
			return errors.New("unexpected replicated username")
		}
		return nil
	})
}

func TestExtensionPointMembershipLateJoinRemoveAndRecover(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{
			Name:      "node-b",
			Slot:      2,
			PeerNames: []string{"node-a"},
			PeerLinks: []clusterTestPeerLinkSpec{
				{Alias: "discovery", Target: "node-c", InitiallyBlocked: true},
			},
		},
		clusterTestNodeSpec{Name: "node-c", Slot: 3},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")
	nodeC := scenario.Node("node-c")

	scenario.Start("node-a", "node-b")
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)
	scenario.WaitForRouteUnreachable(10*time.Second, "node-a", nodeC.id)

	scenario.Start("node-c")
	scenario.HealLink("node-b", "node-c", "discovery")
	scenario.WaitForPeersActive(20*time.Second,
		expectClusterPeer(nodeB, nodeC.id),
		expectClusterPeer(nodeC, nodeB.id),
	)
	nodeC.manager.broadcastRoutingUpdate()
	nodeB.manager.broadcastRoutingUpdate()
	scenario.WaitForRouteReachable(20*time.Second, "node-a", nodeC.id)

	scenario.DisableLink("node-b", "node-c", "discovery")
	scenario.WaitForPeersInactive(20*time.Second,
		expectClusterPeer(nodeB, nodeC.id),
		expectClusterPeer(nodeC, nodeB.id),
	)
	nodeB.manager.broadcastRoutingUpdate()
	scenario.WaitForRouteUnreachable(20*time.Second, "node-a", nodeC.id)

	scenario.EnableLink("node-b", "node-c", "discovery")
	scenario.HealLink("node-b", "node-c", "discovery")
	scenario.WaitForPeersActive(20*time.Second,
		expectClusterPeer(nodeB, nodeC.id),
		expectClusterPeer(nodeC, nodeB.id),
	)
	nodeC.manager.broadcastRoutingUpdate()
	nodeB.manager.broadcastRoutingUpdate()
	scenario.WaitForRouteReachable(20*time.Second, "node-a", nodeC.id)
}

func TestExtensionPointDiscoveryDuplicateLinkJitterSwitchesAliases(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{
			Name: "node-a",
			Slot: 1,
			PeerLinks: []clusterTestPeerLinkSpec{
				{Alias: "primary", Target: "node-b", InitiallyBlocked: true},
				{Alias: "discovery-duplicate", Target: "node-b"},
			},
		},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForHelloOnLink("node-a", "node-b", "discovery-duplicate", clusterTestHelloNodeID(nodeA.id), 10*time.Second)
	scenario.WaitForHandshakeAccepted("node-a", "node-b", 10*time.Second)

	scenario.DisableLink("node-a", "node-b", "discovery-duplicate")
	scenario.EnableLink("node-a", "node-b", "primary")
	scenario.HealLink("node-a", "node-b", "primary")
	scenario.WaitForHelloOnLink("node-a", "node-b", "primary", clusterTestHelloNodeID(nodeA.id), 20*time.Second)
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)
}

func TestExtensionPointRoutingCanBeDisabledForRelay(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}, DisableRouting: true},
		clusterTestNodeSpec{Name: "node-c", Slot: 3, PeerNames: []string{"node-b"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")
	nodeC := scenario.Node("node-c")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
		expectClusterPeer(nodeB, nodeC.id),
		expectClusterPeer(nodeC, nodeB.id),
	)

	nodeC.manager.broadcastRoutingUpdate()
	nodeB.manager.broadcastRoutingUpdate()
	scenario.WaitForRouteUnreachable(10*time.Second, "node-a", nodeC.id)

	nodeB.manager.setSupportsRouting(true)
	nodeB.manager.broadcastRoutingUpdate()
	scenario.WaitForRouteReachable(10*time.Second, "node-a", nodeC.id)
}
