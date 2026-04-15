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

func TestBuildMembershipEnvelopeIncludesZeroMQURLs(t *testing.T) {
	node := newClusterTestNodeFixture(t, clusterTestNodeConfig{
		Name:             "node-a",
		Slot:             1,
		DiscoveryEnabled: true,
	})
	mgr := node.manager
	mgr.cfg.ZeroMQ = ZeroMQConfig{
		Enabled:  true,
		Security: ZeroMQSecurityCurve,
		Curve: ZeroMQCurveConfig{
			ServerPublicKey: testZeroMQCurveKey("S"),
		},
	}
	mgr.configuredPeers = append(mgr.configuredPeers, &configuredPeer{
		URL:                        "zmq+tcp://127.0.0.1:9091",
		zeroMQCurveServerPublicKey: testZeroMQCurveKey("P"),
		nodeID:                     testNodeID(2),
		source:                     peerSourceStatic,
	})
	mgr.discoveredPeers["zmq+tcp://127.0.0.1:9092"] = &discoveredPeerState{
		nodeID:                     testNodeID(3),
		url:                        "zmq+tcp://127.0.0.1:9092",
		zeroMQCurveServerPublicKey: testZeroMQCurveKey("D"),
		state:                      discoveryStateConnected,
	}
	mgr.selfKnownURLs["zmq+tcp://127.0.0.1:9093"] = 7

	envelope := mgr.buildMembershipEnvelope()
	if envelope == nil {
		t.Fatal("expected membership envelope")
	}
	update := envelope.GetMembershipUpdate()
	if update == nil {
		t.Fatal("expected membership update body")
	}
	seen := make(map[string]string)
	for _, item := range update.Peers {
		if item == nil {
			continue
		}
		seen[item.Url] = item.ZeromqCurveServerPublicKey
	}
	for want, wantKey := range map[string]string{
		"zmq+tcp://127.0.0.1:9091": testZeroMQCurveKey("P"),
		"zmq+tcp://127.0.0.1:9092": testZeroMQCurveKey("D"),
		"zmq+tcp://127.0.0.1:9093": testZeroMQCurveKey("S"),
	} {
		if seen[want] != wantKey {
			t.Fatalf("expected membership advertisement %q to carry key %q, got %+v", want, wantKey, update.Peers)
		}
	}
}

func TestMembershipUpdateStoresZeroMQCurveServerPublicKey(t *testing.T) {
	t.Parallel()

	node := newClusterTestNodeFixture(t, clusterTestNodeConfig{
		Name:             "node-a",
		Slot:             1,
		DiscoveryEnabled: true,
	})
	mgr := node.manager
	mgr.cfg.ZeroMQ.Enabled = true
	mgr.cfg.ZeroMQ.Security = ZeroMQSecurityCurve
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}

	err := mgr.handleMembershipUpdate(sess, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_MembershipUpdate{
			MembershipUpdate: &internalproto.MembershipUpdate{
				OriginNodeId: testNodeID(2),
				Peers: []*internalproto.PeerAdvertisement{
					{
						NodeId:                     testNodeID(3),
						Url:                        "zmq+tcp://127.0.0.1:9091",
						ZeromqCurveServerPublicKey: testZeroMQCurveKey("Z"),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("handle membership update: %v", err)
	}
	mgr.mu.Lock()
	discovered := mgr.discoveredPeers["zmq+tcp://127.0.0.1:9091"]
	mgr.mu.Unlock()
	if discovered == nil || discovered.zeroMQCurveServerPublicKey != testZeroMQCurveKey("Z") {
		t.Fatalf("expected discovered peer to retain curve server key, got %+v", discovered)
	}
}

func TestStatusReportsZeroMQModeAndTransport(t *testing.T) {
	t.Parallel()

	node := newClusterTestNodeFixture(t, clusterTestNodeConfig{
		Name:             "node-a",
		Slot:             1,
		DiscoveryEnabled: true,
	})
	mgr := node.manager
	mgr.cfg.ZeroMQ.Enabled = true
	mgr.configuredPeers = append(mgr.configuredPeers, &configuredPeer{
		URL:    "zmq+tcp://127.0.0.1:9091",
		nodeID: testNodeID(2),
		source: peerSourceStatic,
	})
	mgr.discoveredPeers["zmq+tcp://127.0.0.1:9092"] = &discoveredPeerState{
		nodeID: testNodeID(3),
		url:    "zmq+tcp://127.0.0.1:9092",
		state:  discoveryStateConnected,
	}

	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Discovery.ZeroMQMode != "outbound_only" {
		t.Fatalf("unexpected zeromq mode: %+v", status.Discovery)
	}
	if status.Discovery.ZeroMQListenerRunning {
		t.Fatalf("expected zeromq listener to stay stopped in outbound-only mode: %+v", status.Discovery)
	}
	if status.Discovery.PeersByScheme["zmq+tcp"] != 1 {
		t.Fatalf("expected discovered zeromq peer to be counted by scheme, got %+v", status.Discovery.PeersByScheme)
	}

	seenTransport := make(map[int64]string)
	for _, peer := range status.Peers {
		seenTransport[peer.NodeID] = peer.Transport
	}
	if seenTransport[testNodeID(2)] != transportZeroMQ {
		t.Fatalf("expected static zeromq peer transport, got %+v", status.Peers)
	}
	if seenTransport[testNodeID(3)] != transportZeroMQ {
		t.Fatalf("expected discovered zeromq peer transport, got %+v", status.Peers)
	}
}
