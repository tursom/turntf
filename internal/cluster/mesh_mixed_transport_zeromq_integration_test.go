//go:build zeromq

package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerQueryLoggedInUsersUsesWebSocketZeroMQBridge(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startWebSocketZeroMQBridgeManagers(t)
	mgrC.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(3),
			UserID:   8192,
			Username: "mixed-zeromq-user",
		}}, nil
	})

	waitForMeshRouteDecision(t, mgrA, testNodeID(3), mesh.TrafficControlQuery, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(t, mgrC, testNodeID(1), mesh.TrafficControlQuery, testNodeID(2), mesh.TransportZeroMQ)

	users, err := mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
	if err != nil {
		t.Fatalf("query logged-in users over websocket/zeromq bridge: %v", err)
	}
	if len(users) != 1 || users[0].NodeID != testNodeID(3) || users[0].UserID != 8192 || users[0].Username != "mixed-zeromq-user" {
		t.Fatalf("unexpected users: %+v", users)
	}
}

func TestManagerRoutesTransientPacketUsesWebSocketZeroMQBridge(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startWebSocketZeroMQBridgeManagers(t)
	delivered := make(chan store.TransientPacket, 1)
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRouteDecision(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)

	packet := store.TransientPacket{
		PacketID:     902,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    store.UserKey{NodeID: testNodeID(3), UserID: 99},
		Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
		Body:         []byte("mixed-zeromq-transient"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      defaultPacketTTLHops,
	}
	if err := mgrA.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route transient packet over websocket/zeromq bridge: %v", err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.SourceNodeID != packet.SourceNodeID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "mixed-zeromq-transient" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for mixed-zeromq transient delivery")
	}
}

func startWebSocketZeroMQBridgeManagers(t *testing.T) (*Manager, *Manager, *Manager) {
	t.Helper()

	stC := newReplicationTestStore(t, "node-c", 3)
	cfgC := mixedTransportBaseConfig(3)
	cfgC.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(t),
	}
	mgrC, _ := startMixedTransportManager(t, cfgC, stC)
	startZeroMQClusterListener(t, mgrC)
	cZeroMQURL := zeroMQPeerURLForBindURL(mgrC.cfg.ZeroMQ.BindURL)

	stA := newReplicationTestStore(t, "node-a", 1)
	cfgA := mixedTransportBaseConfig(1)
	mgrA, serverAURL := startMixedTransportManager(t, cfgA, stA)

	stB := newReplicationTestStore(t, "node-b", 2)
	cfgB := mixedTransportBaseConfig(2)
	cfgB.ZeroMQ = ZeroMQConfig{
		Enabled: true,
	}
	cfgB.Peers = []Peer{
		{URL: websocketURL(serverAURL) + websocketPath},
		{URL: cZeroMQURL},
	}
	mgrB, _ := startMixedTransportManager(t, cfgB, stB)

	return mgrA, mgrB, mgrC
}

func startZeroMQClusterListener(t *testing.T, mgr *Manager) {
	t.Helper()
	if mgr == nil || !mgr.cfg.zeroMQListenerEnabled() {
		t.Fatalf("expected listening zeromq manager")
	}
	ctx, cancel := context.WithCancel(context.Background())
	listener := NewZeroMQMuxListenerWithConfig(mgr.cfg.ZeroMQ.BindURL, mgr.cfg.ZeroMQ)
	listener.SetClusterAccept(mgr.AcceptZeroMQConn)
	if err := listener.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start zeromq cluster listener: %v", err)
	}
	mgr.SetZeroMQListenerRunning(true)
	t.Cleanup(func() {
		mgr.SetZeroMQListenerRunning(false)
		cancel()
		_ = listener.Close()
	})
}

func nextZeroMQTCPAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for zeromq test address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close zeromq test address listener: %v", err)
	}
	return "tcp://" + addr
}
