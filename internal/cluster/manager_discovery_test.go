package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerMembershipUpdateAcceptsValidPeersAndCountsRejectedAdvertisements(t *testing.T) {
	t.Parallel()

	mgr := newDiscoveryTestManager(t, newReplicationTestStore(t, "membership-update", 1))
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}
	peerURL := "ws://127.0.0.1:9093/internal/cluster/ws"

	err := mgr.handleMembershipUpdate(sess, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_MembershipUpdate{MembershipUpdate: &internalproto.MembershipUpdate{
			OriginNodeId: testNodeID(2),
			Generation:   9,
			Peers: []*internalproto.PeerAdvertisement{
				{NodeId: testNodeID(3), Url: peerURL, Generation: 7},
				{NodeId: 0, Url: "ws://127.0.0.1:9094/internal/cluster/ws"},
				nil,
			},
		}},
	})
	if err != nil {
		t.Fatalf("handle membership update: %v", err)
	}

	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("manager status: %v", err)
	}
	if status.Discovery.MembershipUpdatesRecv != 1 || status.Discovery.RejectedTotal != 1 || status.Discovery.DiscoveredPeers != 1 {
		t.Fatalf("unexpected discovery counters: %+v", status.Discovery)
	}
	peer := requireDiscoveryPeerStatus(t, status.Peers, testNodeID(3))
	if peer.DiscoveredURL != peerURL || peer.DiscoveryState != discoveryStateCandidate || peer.Source != peerSourceDiscovered {
		t.Fatalf("unexpected discovered peer status: %+v", peer)
	}
}

func TestManagerMembershipUpdateRejectsInvalidEnvelopeAndOrigin(t *testing.T) {
	t.Parallel()

	mgr := newDiscoveryTestManager(t, nil)
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}
	tests := []struct {
		name     string
		envelope *internalproto.Envelope
	}{
		{name: "missing body", envelope: &internalproto.Envelope{NodeId: testNodeID(2)}},
		{name: "wrong envelope node", envelope: &internalproto.Envelope{
			NodeId: testNodeID(3),
			Body: &internalproto.Envelope_MembershipUpdate{MembershipUpdate: &internalproto.MembershipUpdate{
				OriginNodeId: testNodeID(2),
			}},
		}},
		{name: "wrong origin", envelope: &internalproto.Envelope{
			NodeId: testNodeID(2),
			Body: &internalproto.Envelope_MembershipUpdate{MembershipUpdate: &internalproto.MembershipUpdate{
				OriginNodeId: testNodeID(3),
			}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := mgr.handleMembershipUpdate(sess, tt.envelope); err == nil {
				t.Fatal("expected membership update to be rejected")
			}
		})
	}
}

func TestManagerDiscoveredPeerLifecyclePersistsFailureConnectionAndExpiration(t *testing.T) {
	t.Parallel()

	st := newReplicationTestStore(t, "discovery-lifecycle", 1)
	mgr := newDiscoveryTestManager(t, st)
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	t.Cleanup(mgr.cancel)
	peerURL := "ws://127.0.0.1:9095/internal/cluster/ws"
	peerID := testNodeID(3)
	if err := mgr.recordDiscoveredCandidate(peerID, peerURL, "", testNodeID(2), 11); err != nil {
		t.Fatalf("record discovered candidate: %v", err)
	}
	dynamic := &configuredPeer{URL: peerURL, nodeID: peerID, dynamic: true, source: peerSourceDiscovered}

	mgr.recordConfiguredPeerDialing(dynamic)
	assertDiscoveryState(t, mgr, peerID, discoveryStateDialing, "")
	mgr.recordConfiguredPeerDialFailure(dynamic, errors.New("dial refused"))
	assertDiscoveryState(t, mgr, peerID, discoveryStateFailed, "dial refused")

	sess := &session{manager: mgr, peerID: peerID, configuredPeer: dynamic, send: make(chan *internalproto.Envelope, 1)}
	mgr.recordSessionDiscoveryConnected(sess)
	connected := assertDiscoveryState(t, mgr, peerID, discoveryStateConnected, "")
	if connected.LastConnectedAt == nil {
		t.Fatal("expected connected discovery state to record connection time")
	}
	mgr.recordConfiguredPeerSessionClosed(dynamic, sess)
	assertDiscoveryState(t, mgr, peerID, discoveryStateFailed, "session closed")

	reloaded := newDiscoveryTestManager(t, st)
	assertDiscoveryState(t, reloaded, peerID, discoveryStateFailed, "session closed")

	reloaded.mu.Lock()
	reloaded.discoveredPeers[peerURL].lastSeenAt = time.Now().UTC().Add(-discoveryCandidateTTL - time.Second)
	reloaded.mu.Unlock()
	reloaded.expireDiscoveredCandidates()
	assertDiscoveryState(t, reloaded, peerID, discoveryStateExpired, "candidate expired")

	finalReload := newDiscoveryTestManager(t, st)
	assertDiscoveryState(t, finalReload, peerID, discoveryStateExpired, "candidate expired")
}

func newDiscoveryTestManager(t *testing.T, st *store.Store) *Manager {
	t.Helper()
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
	}, st)
	if err != nil {
		t.Fatalf("new discovery manager: %v", err)
	}
	return mgr
}

func assertDiscoveryState(t *testing.T, mgr *Manager, peerID int64, wantState, wantError string) store.DiscoveredPeer {
	t.Helper()
	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("manager status: %v", err)
	}
	peer := requireDiscoveryPeerStatus(t, status.Peers, peerID)
	if peer.DiscoveryState != wantState || peer.LastDiscoveryError != wantError {
		t.Fatalf("unexpected discovery state: got state=%q error=%q want state=%q error=%q", peer.DiscoveryState, peer.LastDiscoveryError, wantState, wantError)
	}
	items, err := mgr.store.ListDiscoveredPeers(context.Background())
	if err != nil {
		t.Fatalf("list persisted discovered peers: %v", err)
	}
	for _, item := range items {
		if item.NodeID == peerID {
			if item.State != wantState || item.LastError != wantError {
				t.Fatalf("unexpected persisted discovery state: %+v", item)
			}
			return item
		}
	}
	t.Fatalf("peer %d not found in persisted discovery state: %+v", peerID, items)
	return store.DiscoveredPeer{}
}

func requireDiscoveryPeerStatus(t *testing.T, peers []app.ClusterPeerStatus, peerID int64) app.ClusterPeerStatus {
	t.Helper()
	for _, peer := range peers {
		if peer.NodeID == peerID {
			return peer
		}
	}
	t.Fatalf("peer %d not found in status: %+v", peerID, peers)
	return app.ClusterPeerStatus{}
}
