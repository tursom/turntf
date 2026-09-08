package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/clock"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

const recoveryURL = "ws://peer.example/internal/cluster/ws"

func recoveryManager(t *testing.T, st *store.Store, slot uint16) *Manager {
	t.Helper()
	m, err := NewManager(Config{NodeID: testNodeID(slot), AdvertisePath: websocketPath, ClusterSecret: "secret", MessageWindowSize: store.DefaultMessageWindowSize, MaxClockSkewMs: DefaultMaxClockSkewMs}, st)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func recoveryObservation(slot uint16, hint string) mesh.AdjacencyObservation {
	return mesh.AdjacencyObservation{RemoteNodeID: testNodeID(slot), Transport: mesh.TransportWebSocket, RemoteHint: hint, Established: true, Hello: &mesh.NodeHello{NodeId: testNodeID(slot), Transports: []*mesh.TransportCapability{{Transport: mesh.TransportWebSocket, AdvertisedEndpoints: []string{websocketPath}}}}}
}

func recoveryAdvertises(m *Manager, id int64, url string) bool {
	for _, p := range m.buildMembershipEnvelope().GetMembershipUpdate().Peers {
		if p.NodeId == id && p.Url == url {
			return true
		}
	}
	return false
}

func TestDiscoveryRecoverySelfURLsAreNotRebroadcast(t *testing.T) {
	for _, source := range []string{"gossip", "history"} {
		t.Run(source, func(t *testing.T) {
			st := newReplicationTestStore(t, "recovery", 1)
			if source == "history" {
				if err := st.UpsertDiscoveredPeer(context.Background(), store.DiscoveredPeer{NodeID: testNodeID(1), URL: recoveryURL, State: discoveryStateConnected}); err != nil {
					t.Fatal(err)
				}
			}
			m := recoveryManager(t, st, 1)
			old := recoveryManager(t, nil, 4)
			old.discoveredPeers[recoveryURL] = &discoveredPeerState{nodeID: testNodeID(1), url: recoveryURL, state: discoveryStateConnected}
			if source == "gossip" {
				if err := m.handleMembershipUpdateBody(testNodeID(4), old.buildMembershipEnvelope().GetMembershipUpdate()); err != nil {
					t.Fatal(err)
				}
			}
			if recoveryAdvertises(m, testNodeID(1), recoveryURL) {
				t.Error("unverified self URL must not be rebroadcast")
			}
			if len(m.collectDialSeeds()) != 0 {
				t.Error("self URL must not become a dial seed")
			}
			observer := recoveryManager(t, nil, 3)
			if err := observer.handleMembershipUpdateBody(testNodeID(4), old.buildMembershipEnvelope().GetMembershipUpdate()); err != nil {
				t.Fatal(err)
			}
			if recoveryAdvertises(observer, testNodeID(1), recoveryURL) {
				t.Error("another peer must verify the candidate before advertising it")
			}
			observer.observeMeshAdjacency(recoveryObservation(1, recoveryURL))
			if !recoveryAdvertises(observer, testNodeID(1), recoveryURL) {
				t.Error("peer that verified this URL must still advertise it")
			}
		})
	}
}

func TestDiscoveryRecoveryHistoryRequiresRuntimeVerification(t *testing.T) {
	st := newReplicationTestStore(t, "recovery", 1)
	connectedAt := clock.Timestamp{WallTimeMs: time.Now().Add(-time.Minute).UnixMilli(), NodeID: testNodeID(1)}
	// 旧的重复身份行仍可存在，本次不迁移存储或自动清理绑定。
	for _, slot := range []uint16{2, 3, 4} {
		if err := st.UpsertDiscoveredPeer(context.Background(), store.DiscoveredPeer{
			NodeID: testNodeID(slot), URL: recoveryURL, State: discoveryStateConnected, LastConnectedAt: &connectedAt,
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertCandidate := func(m *Manager) {
		t.Helper()
		peer := m.discoveredPeers[recoveryURL]
		if peer == nil || peer.state != discoveryStateCandidate || peer.lastConnectedAt.IsZero() {
			t.Errorf("history must retain connection time but restore candidate: %+v", peer)
		}
		if len(m.buildMembershipEnvelope().GetMembershipUpdate().Peers) != 0 {
			t.Error("history was advertised without runtime URL verification")
		}
		found := false
		for _, seed := range m.collectDialSeeds() {
			found = found || seed.Endpoint == recoveryURL
		}
		if !found {
			t.Error("historical candidate must remain a bootstrap dial seed")
		}
	}
	m := recoveryManager(t, st, 1)
	assertCandidate(m)
	m.observeMeshAdjacency(recoveryObservation(2, recoveryURL))
	if !recoveryAdvertises(m, testNodeID(2), recoveryURL) || recoveryAdvertises(m, testNodeID(4), recoveryURL) {
		t.Error("exact URL verification must advertise the corrected runtime binding")
	}
	assertCandidate(recoveryManager(t, st, 1))
	rows, err := st.ListDiscoveredPeers(context.Background())
	if err != nil || len(rows) != 3 {
		t.Fatalf("recovery must not migrate or delete historical rows: rows=%+v err=%v", rows, err)
	}
}

func TestDiscoveryRecoveryKnownNodeDoesNotVerifyURL(t *testing.T) {
	for _, kind := range []string{"configured", "dynamic", "discovered"} {
		t.Run(kind, func(t *testing.T) {
			st := newReplicationTestStore(t, "recovery", 1)
			m := recoveryManager(t, st, 1)
			// 模拟旧节点从污染的 connected 状态生成真实 membership 广告。
			old := recoveryManager(t, nil, 4)
			old.discoveredPeers[recoveryURL] = &discoveredPeerState{nodeID: testNodeID(2), url: recoveryURL, state: discoveryStateConnected}
			if err := m.handleMembershipUpdateBody(testNodeID(4), old.buildMembershipEnvelope().GetMembershipUpdate()); err != nil {
				t.Fatal(err)
			}
			peer := &configuredPeer{URL: recoveryURL, nodeID: testNodeID(2), dynamic: kind == "dynamic"}
			switch kind {
			case "configured":
				m.configuredPeers = []*configuredPeer{peer}
			case "dynamic":
				m.dynamicPeers[recoveryURL] = peer
			}
			unrelated := recoveryObservation(2, "ws://other.example/internal/cluster/ws")
			if configuredPeerMatchesMeshObservation(peer, unrelated) || discoveredPeerMatchesMeshObservation(m.discoveredPeers[recoveryURL], unrelated) {
				t.Error("known node ID and shared relative path must not verify another URL")
			}
			crossTransport := recoveryObservation(2, recoveryURL)
			crossTransport.Transport = mesh.TransportZeroMQ
			if configuredPeerMatchesMeshObservation(peer, crossTransport) || discoveredPeerMatchesMeshObservation(m.discoveredPeers[recoveryURL], crossTransport) {
				t.Error("known node ID must not bypass transport matching")
			}
			m.observeMeshAdjacency(unrelated)
			if got := m.discoveredPeers[recoveryURL].state; got != discoveryStateCandidate {
				t.Errorf("unverified URL state = %s, want candidate", got)
			}
			if kind != "configured" && recoveryAdvertises(m, testNodeID(2), recoveryURL) {
				t.Error("unverified gossip URL was rebroadcast")
			}
			rows, err := st.ListDiscoveredPeers(context.Background())
			if err != nil || len(rows) != 1 || rows[0].State != discoveryStateCandidate || rows[0].LastConnectedAt != nil {
				t.Errorf("unrelated adjacency persisted verification: rows=%+v err=%v", rows, err)
			}
			m.observeMeshAdjacency(recoveryObservation(2, recoveryURL))
			if m.discoveredPeers[recoveryURL].state != discoveryStateConnected || !recoveryAdvertises(m, testNodeID(2), recoveryURL) {
				t.Error("exact URL adjacency must remain eligible for advertisement")
			}
		})
	}
}
