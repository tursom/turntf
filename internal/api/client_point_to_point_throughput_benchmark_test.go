package api

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

type clientTransientPointToPointThroughputScenario struct {
	name  string
	build func(testing.TB, benchroot.Mode) *clientTransientPointToPointThroughputTopology
}

type clientTransientPointToPointThroughputTopology struct {
	source            benchmarkLinearMeshAPINode
	target            benchmarkLinearMeshAPINode
	local             bool
	expectedTransport mesh.TransportKind
	deliveryTimeout   time.Duration
	closeFns          []func()
}

type clientTransientPointToPointThroughputWorker struct {
	sender       *benchmarkClientWebSocket
	recipient    *benchmarkClientWebSocket
	recipientKey store.UserKey
}

func BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput(b *testing.B) {
	scenarios := append([]clientTransientPointToPointThroughputScenario{
		{name: "single-node/local", build: buildClientTransientPointToPointSingleNodeTopology},
		{name: "websocket/direct-2nodes", build: buildClientTransientPointToPointWebSocketDirectTopology},
		{name: "libp2p/direct-2nodes", build: buildClientTransientPointToPointLibP2PDirectTopology},
	}, clientTransientPointToPointThroughputExtraScenarios()...)

	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range scenarios {
				scenario := scenario
				for _, tc := range []struct {
					name        string
					payloadSize int
				}{
					{name: "256B", payloadSize: 256},
					{name: "4KiB", payloadSize: 4 << 10},
				} {
					b.Run(fmt.Sprintf("%s/%s", scenario.name, tc.name), func(b *testing.B) {
						benchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput(b, mode, scenario, tc.payloadSize)
					})
				}
			}
		})
	}
}

func benchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput(b *testing.B, mode benchroot.Mode, scenario clientTransientPointToPointThroughputScenario, payloadSize int) {
	b.StopTimer()
	silenceAPIBenchmarkLogs(b)

	ctx := context.Background()
	timeout := benchmarkAPIClientTimeout(mode, 5*time.Second, 30*time.Second)
	topology := scenario.build(b, mode)
	b.Cleanup(topology.Close)

	if !topology.local {
		waitForAPIBenchmarkMeshRouteDecision(b, topology.source.manager, topology.target.nodeID, mesh.TrafficTransientInteractive, topology.target.nodeID, topology.expectedTransport, timeout)
		waitForAPIBenchmarkMeshRouteDecision(b, topology.source.manager, topology.target.nodeID, mesh.TrafficControlCritical, topology.target.nodeID, topology.expectedTransport, timeout)
		waitForAPIBenchmarkMeshRouteDecision(b, topology.source.manager, topology.target.nodeID, mesh.TrafficControlQuery, topology.target.nodeID, topology.expectedTransport, timeout)
		waitForAPIBenchmarkMeshRouteDecision(b, topology.target.manager, topology.source.nodeID, mesh.TrafficControlCritical, topology.source.nodeID, topology.expectedTransport, timeout)
		waitForAPIBenchmarkMeshRouteDecision(b, topology.target.manager, topology.source.nodeID, mesh.TrafficReplicationStream, topology.source.nodeID, topology.expectedTransport, timeout)
	}

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}

	passwordHash := benchmarkMustHashPassword(b, "bench-password")
	recipients := make([]store.UserKey, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		user, _, err := topology.target.service.CreateUser(ctx, store.CreateUserParams{
			Username:     fmt.Sprintf("bench-p2p-throughput-recipient-%d-%d", topology.target.nodeID, i),
			PasswordHash: passwordHash,
			Role:         store.RoleUser,
		})
		if err != nil {
			b.Fatalf("create point-to-point throughput recipient %d: %v", i, err)
		}
		recipients = append(recipients, user.Key())
	}

	if !topology.local {
		for _, recipientKey := range recipients {
			recipientKey := recipientKey
			waitForAPIBenchmarkCondition(b, timeout, func() bool {
				_, err := topology.source.service.GetUser(ctx, recipientKey)
				return err == nil
			})
		}
	}

	adminKey := store.UserKey{NodeID: topology.source.nodeID, UserID: store.BootstrapAdminUserID}
	workers := make([]clientTransientPointToPointThroughputWorker, 0, workerCount)
	for idx, recipientKey := range recipients {
		sender := dialBenchmarkClientWebSocketWithOptions(b, topology.source.serverURL, true)
		recipient := dialBenchmarkClientWebSocketWithOptions(b, topology.target.serverURL, true)
		b.Cleanup(func() { _ = sender.Close() })
		b.Cleanup(func() { _ = recipient.Close() })

		benchmarkLoginClientWebSocket(b, sender, adminKey, "root-password")
		benchmarkLoginClientWebSocket(b, recipient, recipientKey, "bench-password")

		if !topology.local {
			waitForAPIBenchmarkLoggedInUser(b, topology.target.http, recipientKey, timeout)
			mustBenchmarkManagerTransientPushToClientOnce(b, ctx, topology.source.manager, recipient, store.TransientPacket{
				PacketID:     (1 << 62) + uint64(idx) + 1,
				SourceNodeID: topology.source.nodeID,
				TargetNodeID: recipientKey.NodeID,
				Recipient:    recipientKey,
				Sender:       adminKey,
				Body:         []byte("p2p-probe"),
				DeliveryMode: store.DeliveryModeBestEffort,
				TTLHops:      8,
			}, []byte("p2p-probe"), timeout)
		}

		workers = append(workers, clientTransientPointToPointThroughputWorker{
			sender:       sender,
			recipient:    recipient,
			recipientKey: recipientKey,
		})
	}

	payload := bytes.Repeat([]byte("t"), payloadSize)
	var requestID atomic.Uint64
	runAPIBenchmarkWarmup(b, func() {
		for _, worker := range workers {
			mustBenchmarkClientWebSocketTransientSendMessageThroughputOnce(b, worker.sender, worker.recipient, adminKey, worker.recipientKey, payload, requestID.Add(1), timeout)
		}
	})
	assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.source.service, recipients)
	if topology.target.service != topology.source.service {
		assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.target.service, recipients)
	}

	var workerSlot atomic.Uint64
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()
	b.SetParallelism(1)
	b.RunParallel(func(pb *testing.PB) {
		slot := int(workerSlot.Add(1) - 1)
		if slot < 0 || slot >= len(workers) {
			b.Fatalf("unexpected throughput worker slot %d for %d workers", slot, len(workers))
		}
		worker := workers[slot]
		for pb.Next() {
			mustBenchmarkClientWebSocketTransientSendMessageThroughputOnce(b, worker.sender, worker.recipient, adminKey, worker.recipientKey, payload, requestID.Add(1), timeout)
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(payloadSize), "bytes/op")

	assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.source.service, recipients)
	if topology.target.service != topology.source.service {
		assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.target.service, recipients)
	}
}

func (t *clientTransientPointToPointThroughputTopology) Close() {
	if t == nil {
		return
	}
	for idx := len(t.closeFns) - 1; idx >= 0; idx-- {
		if t.closeFns[idx] != nil {
			t.closeFns[idx]()
		}
	}
}

func buildClientTransientPointToPointSingleNodeTopology(tb testing.TB, mode benchroot.Mode) *clientTransientPointToPointThroughputTopology {
	tb.Helper()

	node, closeNode := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-local", 1, nil)
	return &clientTransientPointToPointThroughputTopology{
		source:          node,
		target:          node,
		local:           true,
		deliveryTimeout: benchmarkAPIClientTimeout(mode, 5*time.Second, 30*time.Second),
		closeFns:        []func(){closeNode},
	}
}

func buildClientTransientPointToPointWebSocketDirectTopology(tb testing.TB, mode benchroot.Mode) *clientTransientPointToPointThroughputTopology {
	tb.Helper()

	targetCfg := benchmarkAPIClusterBaseConfig(2)
	targetNode, closeTarget := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-websocket-target", 2, &targetCfg)

	sourceCfg := benchmarkAPIClusterBaseConfig(1)
	sourceCfg.Peers = []cluster.Peer{{URL: benchmarkWebSocketURL(targetNode.serverURL) + cluster.WebSocketPath}}
	sourceNode, closeSource := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-websocket-source", 1, &sourceCfg)

	return &clientTransientPointToPointThroughputTopology{
		source:            sourceNode,
		target:            targetNode,
		expectedTransport: mesh.TransportWebSocket,
		deliveryTimeout:   benchmarkAPIClientTimeout(mode, 5*time.Second, 30*time.Second),
		closeFns:          []func(){closeTarget, closeSource},
	}
}

func buildClientTransientPointToPointLibP2PDirectTopology(tb testing.TB, mode benchroot.Mode) *clientTransientPointToPointThroughputTopology {
	tb.Helper()

	targetCfg := benchmarkAPIClusterBaseConfig(2)
	targetCfg.LibP2P = benchmarkPointToPointAPILibP2PConfig(tb, "bench-api-p2p-libp2p-target")
	targetNode, closeTarget := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-libp2p-target", 2, &targetCfg)
	targetPeerURL := benchmarkPointToPointAPIFirstLibP2PListenAddr(tb, targetNode.manager)

	sourceCfg := benchmarkAPIClusterBaseConfig(1)
	sourceCfg.LibP2P = benchmarkPointToPointAPILibP2PConfig(tb, "bench-api-p2p-libp2p-source")
	sourceCfg.Peers = []cluster.Peer{{URL: targetPeerURL}}
	sourceNode, closeSource := openBenchmarkAuthenticatedPointToPointAPINode(tb, mode, "bench-api-p2p-libp2p-source", 1, &sourceCfg)

	return &clientTransientPointToPointThroughputTopology{
		source:            sourceNode,
		target:            targetNode,
		expectedTransport: mesh.TransportLibP2P,
		deliveryTimeout:   benchmarkAPIClientTimeout(mode, 5*time.Second, 30*time.Second),
		closeFns:          []func(){closeTarget, closeSource},
	}
}

func mustBenchmarkClientWebSocketTransientSendMessageThroughputOnce(tb testing.TB, sender, recipient *benchmarkClientWebSocket, senderKey, recipientKey store.UserKey, payload []byte, requestID uint64, timeout time.Duration) {
	tb.Helper()

	benchmarkWriteClientEnvelope(tb, sender.conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:    requestID,
				Target:       &internalproto.UserRef{NodeId: recipientKey.NodeID, UserId: recipientKey.UserID},
				Body:         payload,
				DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
				DeliveryMode: internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT,
			},
		},
	})

	acceptedEnvelope := sender.nextEnvelopeWithLabel(tb, timeout, "throughput-sender")
	pushedEnvelope := recipient.nextEnvelopeWithLabel(tb, timeout, "throughput-recipient")

	sendResp := acceptedEnvelope.envelope.GetSendMessageResponse()
	if sendResp == nil {
		tb.Fatalf("expected throughput transient send response envelope, got %+v", acceptedEnvelope.envelope)
	}
	if sendResp.RequestId != requestID {
		tb.Fatalf("unexpected throughput transient request id: got=%d want=%d", sendResp.RequestId, requestID)
	}
	accepted := sendResp.GetTransientAccepted()
	if accepted == nil {
		tb.Fatalf("expected throughput transient accepted response body, got %+v", sendResp)
	}
	if accepted.PacketId == 0 {
		tb.Fatalf("expected throughput transient accepted packet id")
	}
	if accepted.SourceNodeId != senderKey.NodeID || accepted.TargetNodeId != recipientKey.NodeID || !senderMatchesRef(accepted.Recipient, recipientKey) {
		tb.Fatalf("unexpected throughput transient accepted response: %+v", accepted)
	}
	if accepted.DeliveryMode != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT {
		tb.Fatalf("unexpected throughput transient accepted delivery mode: %s", accepted.DeliveryMode.String())
	}

	packetPushed := pushedEnvelope.envelope.GetPacketPushed()
	if packetPushed == nil || packetPushed.Packet == nil {
		tb.Fatalf("expected throughput packet push envelope, got %+v", pushedEnvelope.envelope)
	}
	packet := packetPushed.Packet
	if packet.GetPacketId() != accepted.PacketId {
		tb.Fatalf("unexpected throughput transient packet id: got=%d want=%d", packet.GetPacketId(), accepted.PacketId)
	}
	if packet.GetSourceNodeId() != senderKey.NodeID || packet.GetTargetNodeId() != recipientKey.NodeID {
		tb.Fatalf("unexpected throughput transient packet route: %+v", packet)
	}
	if !senderMatchesRef(packet.GetRecipient(), recipientKey) || !senderMatchesRef(packet.GetSender(), senderKey) {
		tb.Fatalf("unexpected throughput transient packet refs: %+v", packet)
	}
	if packet.GetDeliveryMode() != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT {
		tb.Fatalf("unexpected throughput transient packet delivery mode: %s", packet.GetDeliveryMode().String())
	}
	if !bytes.Equal(packet.GetBody(), payload) {
		tb.Fatalf("unexpected throughput transient packet body: got=%q want=%q", packet.GetBody(), payload)
	}
}

func assertBenchmarkTransientMessagesNotPersistedForKeys(tb testing.TB, svc *Service, keys []store.UserKey) {
	tb.Helper()

	for _, key := range keys {
		assertBenchmarkTransientMessagesNotPersisted(tb, svc, key)
	}
}

func waitForAPIBenchmarkLoggedInUser(tb testing.TB, httpAPI *HTTP, recipientKey store.UserKey, timeout time.Duration) {
	tb.Helper()

	waitForAPIBenchmarkCondition(tb, timeout, func() bool {
		if httpAPI == nil {
			return false
		}
		users, err := httpAPI.ListLoggedInUsers(context.Background())
		if err != nil {
			return false
		}
		for _, user := range users {
			if user.NodeID == recipientKey.NodeID && user.UserID == recipientKey.UserID {
				return true
			}
		}
		return false
	})
}

func waitForAPIBenchmarkMeshRouteDecision(tb testing.TB, manager *cluster.Manager, destinationNodeID int64, trafficClass mesh.TrafficClass, nextHopNodeID int64, outboundTransport mesh.TransportKind, timeout time.Duration) {
	tb.Helper()

	waitForAPIBenchmarkCondition(tb, timeout, func() bool {
		if manager == nil || manager.MeshRuntime() == nil {
			return false
		}
		decision, ok := manager.MeshRuntime().DescribeRoute(destinationNodeID, trafficClass)
		return ok && decision.NextHopNodeID == nextHopNodeID && decision.OutboundTransport == outboundTransport
	})

	if manager == nil || manager.MeshRuntime() == nil {
		tb.Fatalf("mesh runtime is not attached")
	}
	decision, ok := manager.MeshRuntime().DescribeRoute(destinationNodeID, trafficClass)
	if !ok {
		tb.Fatalf("expected mesh route to node %d for traffic %v", destinationNodeID, trafficClass)
	}
	if decision.NextHopNodeID != nextHopNodeID || decision.OutboundTransport != outboundTransport {
		tb.Fatalf("unexpected mesh route decision: got=%+v want next=%d transport=%v", decision, nextHopNodeID, outboundTransport)
	}
}

func openBenchmarkAuthenticatedPointToPointAPINode(tb testing.TB, mode benchroot.Mode, name string, nodeSlot uint16, cfg *cluster.Config) (benchmarkLinearMeshAPINode, func()) {
	tb.Helper()

	scenario := apiBenchmarkEngineScenario{name: store.EngineSQLite, engine: store.EngineSQLite}
	st, closeStore := openBenchmarkAPIClusterStore(tb, mode, scenario, name, nodeSlot)

	signer, err := auth.NewSigner("token-secret")
	if err != nil {
		closeStore()
		tb.Fatalf("new throughput benchmark signer: %v", err)
	}

	var (
		manager   *cluster.Manager
		eventSink EventSink
	)
	if cfg != nil {
		manager, err = cluster.NewManager(*cfg, st)
		if err != nil {
			closeStore()
			tb.Fatalf("new throughput benchmark cluster manager: %v", err)
		}
		eventSink = benchmarkClusterSink{manager: manager}
	}

	service := New(st, eventSink)
	httpAPI := NewHTTP(service, HTTPOptions{
		NodeID:   st.NodeID(),
		Signer:   signer,
		TokenTTL: time.Hour,
	})
	if manager != nil {
		manager.SetTransientHandler(httpAPI.ReceiveTransientPacket)
		manager.SetLoggedInUsersProvider(httpAPI.ListLoggedInUsers)
	}

	handler := benchmarkClusterAPIHandler(httpAPI.Handler(), manager)
	server, closeServer := openBenchmarkClientWebSocketServer(tb, handler)

	if manager != nil {
		if err := manager.Start(context.Background()); err != nil {
			closeServer()
			_ = httpAPI.Close()
			closeStore()
			tb.Fatalf("start throughput benchmark cluster manager: %v", err)
		}
	}

	closeNode := func() {
		if manager != nil {
			_ = manager.Close()
		}
		closeServer()
		_ = httpAPI.Close()
		closeStore()
	}
	return benchmarkLinearMeshAPINode{
		nodeID:    st.NodeID(),
		manager:   manager,
		service:   service,
		http:      httpAPI,
		handler:   handler,
		serverURL: server.URL,
	}, closeNode
}

func benchmarkPointToPointAPILibP2PConfig(tb testing.TB, name string) cluster.LibP2PConfig {
	tb.Helper()
	return cluster.LibP2PConfig{
		Enabled:            true,
		PrivateKeyPath:     filepath.Join(tb.TempDir(), name+".key"),
		ListenAddrs:        []string{"/ip4/127.0.0.1/tcp/0"},
		EnableHolePunching: true,
	}
}

func benchmarkPointToPointAPIFirstLibP2PListenAddr(tb testing.TB, manager *cluster.Manager) string {
	tb.Helper()
	if manager == nil {
		tb.Fatalf("expected libp2p benchmark manager")
	}

	status, err := manager.Status(context.Background())
	if err != nil {
		tb.Fatalf("status for libp2p benchmark manager: %v", err)
	}
	if len(status.Discovery.LibP2PListenAddrs) == 0 {
		tb.Fatalf("expected libp2p benchmark listen address")
	}
	for _, addr := range status.Discovery.LibP2PListenAddrs {
		if strings.Contains(addr, "/ip4/127.0.0.1/") {
			return addr
		}
	}
	return status.Discovery.LibP2PListenAddrs[0]
}
