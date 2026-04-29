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

	var users []app.LoggedInUserSummary
	waitFor(t, 5*time.Second, func() bool {
		var err error
		users, err = mgrA.QueryLoggedInUsers(context.Background(), testNodeID(3))
		return err == nil && len(users) == 1
	})
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

func startWebSocketZeroMQBridgeManagers(tb testing.TB) (*Manager, *Manager, *Manager) {
	tb.Helper()

	stC := newReplicationTestStore(tb, "node-c", 3)
	cfgC := mixedTransportBaseConfig(3)
	cfgC.ZeroMQ = ZeroMQConfig{
		Enabled: true,
		BindURL: nextZeroMQTCPAddress(tb),
	}
	mgrC, _ := startMixedTransportManager(tb, cfgC, stC)
	startZeroMQClusterListener(tb, mgrC)
	cZeroMQURL := zeroMQPeerURLForBindURL(mgrC.cfg.ZeroMQ.BindURL)

	stA := newReplicationTestStore(tb, "node-a", 1)
	cfgA := mixedTransportBaseConfig(1)
	mgrA, serverAURL := startMixedTransportManager(tb, cfgA, stA)

	stB := newReplicationTestStore(tb, "node-b", 2)
	cfgB := mixedTransportBaseConfig(2)
	cfgB.ZeroMQ = ZeroMQConfig{
		Enabled: true,
	}
	cfgB.Peers = []Peer{
		{URL: websocketURL(serverAURL) + websocketPath},
		{URL: cZeroMQURL},
	}
	mgrB, _ := startMixedTransportManager(tb, cfgB, stB)

	return mgrA, mgrB, mgrC
}

func startZeroMQClusterListener(tb testing.TB, mgr *Manager) {
	tb.Helper()
	if mgr == nil || !mgr.cfg.zeroMQListenerEnabled() {
		tb.Fatalf("expected listening zeromq manager")
	}
	ctx, cancel := context.WithCancel(context.Background())
	listener := NewZeroMQMuxListenerWithConfig(mgr.cfg.ZeroMQ.BindURL, mgr.cfg.ZeroMQ)
	listener.SetClusterAccept(mgr.AcceptZeroMQConn)
	if err := listener.Start(ctx); err != nil {
		cancel()
		tb.Fatalf("start zeromq cluster listener: %v", err)
	}
	mgr.SetZeroMQListenerRunning(true)
	tb.Cleanup(func() {
		mgr.SetZeroMQListenerRunning(false)
		cancel()
		_ = listener.Close()
	})
}

func nextZeroMQTCPAddress(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen for zeromq test address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		tb.Fatalf("close zeromq test address listener: %v", err)
	}
	return "tcp://" + addr
}
