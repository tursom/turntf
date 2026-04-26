package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

const clientWebSocketBenchmarkEnvelopeTimeout = 5 * time.Second

type clientWebSocketBenchmarkEnvelope struct {
	envelope   *internalproto.ServerEnvelope
	receivedAt time.Time
}

type benchmarkClientWebSocket struct {
	conn      *websocket.Conn
	envelopes chan clientWebSocketBenchmarkEnvelope
	errs      chan error
}

type benchmarkClusterSink struct {
	manager *cluster.Manager
}

type benchmarkLinearMeshAPICluster struct {
	nodes    []benchmarkLinearMeshAPINode
	closeFns []func()
}

type benchmarkLinearMeshAPINode struct {
	nodeID    int64
	manager   *cluster.Manager
	service   *Service
	http      *HTTP
	handler   http.Handler
	serverURL string
}

func apiOnlineCapacityBenchmarkEngineScenarios() []apiBenchmarkEngineScenario {
	return []apiBenchmarkEngineScenario{
		{name: store.EngineSQLite, engine: store.EngineSQLite},
	}
}

func apiTransientBenchmarkEngineScenarios() []apiBenchmarkEngineScenario {
	return []apiBenchmarkEngineScenario{
		{name: store.EngineSQLite, engine: store.EngineSQLite},
		{name: store.EnginePebble + "/" + string(store.PebbleProfileBalanced), engine: store.EnginePebble, pebbleProfile: store.PebbleProfileBalanced},
		{name: store.EnginePebble + "/" + string(store.PebbleProfileThroughput), engine: store.EnginePebble, pebbleProfile: store.PebbleProfileThroughput},
	}
}

func BenchmarkClientWebSocketTransientSendMessageAuthenticated(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range apiTransientBenchmarkEngineScenarios() {
				for _, tc := range []struct {
					name        string
					payloadSize int
				}{
					{name: "256B", payloadSize: 256},
					{name: "4KiB", payloadSize: 4 << 10},
				} {
					b.Run(fmt.Sprintf("%s/%s", scenario.name, tc.name), func(b *testing.B) {
						benchmarkClientWebSocketTransientSendMessageAuthenticated(b, mode, scenario, tc.payloadSize)
					})
				}
			}
		})
	}
}

func BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range apiTransientBenchmarkEngineScenarios() {
				for _, nodeCount := range []int{3, 7} {
					for _, tc := range []struct {
						name        string
						payloadSize int
					}{
						{name: "256B", payloadSize: 256},
						{name: "4KiB", payloadSize: 4 << 10},
					} {
						b.Run(fmt.Sprintf("%s/%d-nodes/%s", scenario.name, nodeCount, tc.name), func(b *testing.B) {
							benchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh(b, mode, scenario, nodeCount, tc.payloadSize)
						})
					}
				}
			}
		})
	}
}

func BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range apiOnlineCapacityBenchmarkEngineScenarios() {
				for _, nodeCount := range []int{3, 7} {
					for _, onlineUsersTotal := range []int{1000, 5000, 10000} {
						b.Run(fmt.Sprintf("%s/%d-nodes/%d-online/256B", scenario.name, nodeCount, onlineUsersTotal), func(b *testing.B) {
							benchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers(b, mode, scenario, nodeCount, onlineUsersTotal, 256)
						})
					}
				}
			}
		})
	}
}

func benchmarkClientWebSocketTransientSendMessageAuthenticated(b *testing.B, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, payloadSize int) {
	b.StopTimer()
	silenceAPIBenchmarkLogs(b)

	testAPI, closeAPI := openBenchmarkAuthenticatedTestAPI(b, mode, scenario)
	b.Cleanup(closeAPI)

	server, closeServer := openBenchmarkClientWebSocketServer(b, testAPI.handler)
	b.Cleanup(closeServer)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := benchmarkLoginToken(b, testAPI.handler, adminKey, "root-password")
	recipientKey := benchmarkCreateUserAs(b, testAPI.handler, adminToken, "bench-transient-recipient", "bench-password", store.RoleUser)

	sender := dialBenchmarkClientWebSocket(b, server.URL)
	b.Cleanup(func() {
		_ = sender.Close()
	})
	recipient := dialBenchmarkClientWebSocket(b, server.URL)
	b.Cleanup(func() {
		_ = recipient.Close()
	})

	benchmarkLoginClientWebSocket(b, sender, adminKey, "root-password")
	benchmarkLoginClientWebSocket(b, recipient, recipientKey, "bench-password")

	payload := bytes.Repeat([]byte("t"), payloadSize)
	nextRequestID := uint64(1)

	runAPIBenchmarkWarmup(b, func() {
		_, _ = mustBenchmarkClientWebSocketTransientSendMessageOnce(b, sender, recipient, adminKey, recipientKey, payload, nextRequestID, clientWebSocketBenchmarkEnvelopeTimeout)
		nextRequestID++
	})
	assertBenchmarkTransientMessagesNotPersisted(b, testAPI.http.service, recipientKey)

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()

	var totalAccept time.Duration
	var totalPush time.Duration
	for i := 0; i < b.N; i++ {
		acceptLatency, pushLatency := mustBenchmarkClientWebSocketTransientSendMessageOnce(b, sender, recipient, adminKey, recipientKey, payload, nextRequestID, clientWebSocketBenchmarkEnvelopeTimeout)
		totalAccept += acceptLatency
		totalPush += pushLatency
		nextRequestID++
	}

	b.StopTimer()
	reportAPIBenchmarkAverageLatencyMetric(b, totalAccept, "accept_ms/op")
	reportAPIBenchmarkAverageLatencyMetric(b, totalPush, "push_ms/op")
	assertBenchmarkTransientMessagesNotPersisted(b, testAPI.http.service, recipientKey)
}

func benchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh(b *testing.B, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, nodeCount, payloadSize int) {
	b.StopTimer()
	silenceAPIBenchmarkLogs(b)

	ctx := context.Background()
	timeout := benchmarkAPIClientTimeout(mode, 5*time.Second, 30*time.Second)
	meshCluster := openBenchmarkAuthenticatedLinearMeshAPICluster(b, mode, scenario, nodeCount)
	b.Cleanup(meshCluster.Close)

	sourceNode := meshCluster.nodes[0]
	targetNode := meshCluster.nodes[len(meshCluster.nodes)-1]
	targetNodeID := targetNode.nodeID

	waitForAPIBenchmarkMeshRoute(b, sourceNode.nodeID, sourceNode.manager, targetNodeID, mesh.TrafficTransientInteractive, timeout)
	waitForAPIBenchmarkMeshRoute(b, sourceNode.nodeID, sourceNode.manager, targetNodeID, mesh.TrafficControlCritical, timeout)
	waitForAPIBenchmarkMeshRoute(b, targetNode.nodeID, targetNode.manager, sourceNode.nodeID, mesh.TrafficReplicationStream, timeout)

	recipient, _, err := targetNode.service.CreateUser(ctx, store.CreateUserParams{
		Username:     fmt.Sprintf("bench-mesh-recipient-%d", nodeCount),
		PasswordHash: benchmarkMustHashPassword(b, "bench-password"),
		Role:         store.RoleUser,
	})
	if err != nil {
		b.Fatalf("create benchmark mesh recipient: %v", err)
	}

	waitForAPIBenchmarkCondition(b, timeout, func() bool {
		replicated, err := sourceNode.service.GetUser(ctx, recipient.Key())
		return err == nil && replicated.Username == recipient.Username
	})

	adminKey := store.UserKey{NodeID: sourceNode.nodeID, UserID: store.BootstrapAdminUserID}
	sender := dialBenchmarkClientWebSocket(b, sourceNode.serverURL)
	b.Cleanup(func() {
		_ = sender.Close()
	})
	recipientClient := dialBenchmarkClientWebSocket(b, targetNode.serverURL)
	b.Cleanup(func() {
		_ = recipientClient.Close()
	})

	benchmarkLoginClientWebSocket(b, sender, adminKey, "root-password")
	benchmarkLoginClientWebSocket(b, recipientClient, recipient.Key(), "bench-password")
	waitForAPIBenchmarkCondition(b, timeout, func() bool {
		users, err := targetNode.http.ListLoggedInUsers(ctx)
		return err == nil && len(users) == 1 && users[0].NodeID == recipient.Key().NodeID && users[0].UserID == recipient.Key().UserID
	})

	meshProbePayload := []byte("mesh-probe")
	mustBenchmarkManagerTransientPushToClientOnce(b, ctx, sourceNode.manager, recipientClient, store.TransientPacket{
		PacketID:     1 << 62,
		SourceNodeID: sourceNode.nodeID,
		TargetNodeID: recipient.Key().NodeID,
		Recipient:    recipient.Key(),
		Sender:       adminKey,
		Body:         meshProbePayload,
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      8,
	}, meshProbePayload, timeout)

	payload := bytes.Repeat([]byte("t"), payloadSize)
	nextRequestID := uint64(1)

	runAPIBenchmarkWarmup(b, func() {
		_, _ = mustBenchmarkClientWebSocketTransientSendMessageOnce(b, sender, recipientClient, adminKey, recipient.Key(), payload, nextRequestID, timeout)
		nextRequestID++
	})
	assertBenchmarkTransientMessagesNotPersisted(b, sourceNode.service, recipient.Key())
	assertBenchmarkTransientMessagesNotPersisted(b, targetNode.service, recipient.Key())

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()

	var totalAccept time.Duration
	var totalPush time.Duration
	for i := 0; i < b.N; i++ {
		acceptLatency, pushLatency := mustBenchmarkClientWebSocketTransientSendMessageOnce(b, sender, recipientClient, adminKey, recipient.Key(), payload, nextRequestID, timeout)
		totalAccept += acceptLatency
		totalPush += pushLatency
		nextRequestID++
	}

	b.StopTimer()
	reportAPIBenchmarkAverageLatencyMetric(b, totalAccept, "accept_ms/op")
	reportAPIBenchmarkAverageLatencyMetric(b, totalPush, "push_ms/op")
	assertBenchmarkTransientMessagesNotPersisted(b, sourceNode.service, recipient.Key())
	assertBenchmarkTransientMessagesNotPersisted(b, targetNode.service, recipient.Key())
}

func benchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers(b *testing.B, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, nodeCount, onlineUsersTotal, payloadSize int) {
	b.StopTimer()
	silenceAPIBenchmarkLogs(b)

	if onlineUsersTotal < 1 {
		b.Fatalf("online users total must be positive, got %d", onlineUsersTotal)
	}

	ctx := context.Background()
	timeout := benchmarkAPIClientTimeout(mode, 30*time.Second, 120*time.Second)
	meshCluster := openBenchmarkAuthenticatedLinearMeshAPIClusterWithEventLogLimit(b, mode, scenario, nodeCount, 32)
	b.Cleanup(meshCluster.Close)

	sourceNode := meshCluster.nodes[0]
	targetNode := meshCluster.nodes[len(meshCluster.nodes)-1]
	targetNodeID := targetNode.nodeID

	waitForAPIBenchmarkMeshRoute(b, sourceNode.nodeID, sourceNode.manager, targetNodeID, mesh.TrafficTransientInteractive, timeout)
	waitForAPIBenchmarkMeshRoute(b, sourceNode.nodeID, sourceNode.manager, targetNodeID, mesh.TrafficControlCritical, timeout)
	waitForAPIBenchmarkMeshRoute(b, targetNode.nodeID, targetNode.manager, sourceNode.nodeID, mesh.TrafficReplicationStream, timeout)

	passwordHash := benchmarkMustHashPassword(b, "bench-password")
	recipient, _, err := targetNode.service.CreateUser(ctx, store.CreateUserParams{
		Username:     fmt.Sprintf("bench-online-recipient-%d-%d", nodeCount, onlineUsersTotal),
		PasswordHash: passwordHash,
		Role:         store.RoleUser,
	})
	if err != nil {
		b.Fatalf("create online capacity recipient: %v", err)
	}

	waitForAPIBenchmarkCondition(b, timeout, func() bool {
		replicated, err := sourceNode.service.GetUser(ctx, recipient.Key())
		return err == nil && replicated.Username == recipient.Username
	})

	backgroundCounts := benchmarkDistributedUserCounts(onlineUsersTotal-1, nodeCount)
	for idx, count := range backgroundCounts {
		node := meshCluster.nodes[idx]
		seedBenchmarkLocalUsers(b, ctx, node, count, fmt.Sprintf("bench-online-node-%d", idx+1), passwordHash)
	}

	for _, node := range meshCluster.nodes {
		if _, err := node.service.store.PruneEventLogOnce(ctx); err != nil {
			b.Fatalf("prune online capacity event log on node %d: %v", node.nodeID, err)
		}
	}

	idleClients := make([]*websocket.Conn, 0, onlineUsersTotal-1)
	for idx, count := range backgroundCounts {
		node := meshCluster.nodes[idx]
		users, err := node.service.ListUsers(ctx)
		if err != nil {
			b.Fatalf("list local benchmark users on node %d: %v", node.nodeID, err)
		}
		loggedIn := 0
		nodeKeys := make([]store.UserKey, 0, count)
		for _, user := range users {
			if user.Role != store.RoleUser || user.Key() == recipient.Key() {
				continue
			}
			nodeKeys = append(nodeKeys, user.Key())
			loggedIn++
			if loggedIn == count {
				break
			}
		}
		if loggedIn != count {
			b.Fatalf("unexpected local benchmark user count on node %d: got=%d want=%d", node.nodeID, loggedIn, count)
		}
		idleClients = append(idleClients, dialAndLoginBenchmarkIdleClientWebSockets(b, node.serverURL, nodeKeys, "bench-password", 64)...)
	}
	for _, conn := range idleClients {
		conn := conn
		b.Cleanup(func() {
			_ = conn.Close()
		})
	}

	adminKey := store.UserKey{NodeID: sourceNode.nodeID, UserID: store.BootstrapAdminUserID}
	sender := dialBenchmarkClientWebSocket(b, sourceNode.serverURL)
	b.Cleanup(func() {
		_ = sender.Close()
	})
	recipientClient := dialBenchmarkClientWebSocket(b, targetNode.serverURL)
	b.Cleanup(func() {
		_ = recipientClient.Close()
	})

	benchmarkLoginClientWebSocket(b, sender, adminKey, "root-password")
	benchmarkLoginClientWebSocket(b, recipientClient, recipient.Key(), "bench-password")

	expectedOnlineUsersByNode := make([]int, nodeCount)
	copy(expectedOnlineUsersByNode, backgroundCounts)
	expectedOnlineUsersByNode[len(expectedOnlineUsersByNode)-1]++
	expectedOnlineUsersByNode[0]++
	waitForAPIBenchmarkOnlineUsers(b, timeout, meshCluster.nodes, expectedOnlineUsersByNode)

	// Give background sessions one poll interval to clear the trimmed setup backlog
	// so the measured path is closer to steady-state online load than first-login catchup.
	time.Sleep(clientWSPollInterval + 200*time.Millisecond)

	payload := bytes.Repeat([]byte("t"), payloadSize)
	nextRequestID := uint64(1)

	runAPIBenchmarkWarmup(b, func() {
		_, _ = mustBenchmarkClientWebSocketTransientSendMessageOnce(b, sender, recipientClient, adminKey, recipient.Key(), payload, nextRequestID, timeout)
		nextRequestID++
	})
	assertBenchmarkTransientMessagesNotPersisted(b, sourceNode.service, recipient.Key())
	assertBenchmarkTransientMessagesNotPersisted(b, targetNode.service, recipient.Key())

	b.SetBytes(int64(payloadSize))
	b.ReportMetric(float64(onlineUsersTotal), "online_users")
	b.ReportMetric(float64(onlineUsersTotal)/float64(nodeCount), "online_users_per_node")
	b.ResetTimer()
	b.StartTimer()

	var totalAccept time.Duration
	var totalPush time.Duration
	for i := 0; i < b.N; i++ {
		acceptLatency, pushLatency := mustBenchmarkClientWebSocketTransientSendMessageOnce(b, sender, recipientClient, adminKey, recipient.Key(), payload, nextRequestID, timeout)
		totalAccept += acceptLatency
		totalPush += pushLatency
		nextRequestID++
	}

	b.StopTimer()
	reportAPIBenchmarkAverageLatencyMetric(b, totalAccept, "accept_ms/op")
	reportAPIBenchmarkAverageLatencyMetric(b, totalPush, "push_ms/op")
	assertBenchmarkTransientMessagesNotPersisted(b, sourceNode.service, recipient.Key())
	assertBenchmarkTransientMessagesNotPersisted(b, targetNode.service, recipient.Key())
}

func mustBenchmarkClientWebSocketTransientSendMessageOnce(tb testing.TB, sender, recipient *benchmarkClientWebSocket, senderKey, recipientKey store.UserKey, payload []byte, requestID uint64, timeout time.Duration) (time.Duration, time.Duration) {
	tb.Helper()

	start := time.Now()
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

	acceptedEnvelope := sender.nextEnvelopeWithLabel(tb, timeout, "sender")
	pushedEnvelope := recipient.nextEnvelopeWithLabel(tb, timeout, "recipient")

	sendResp := acceptedEnvelope.envelope.GetSendMessageResponse()
	if sendResp == nil {
		tb.Fatalf("expected transient send response envelope, got %+v", acceptedEnvelope.envelope)
	}
	if sendResp.RequestId != requestID {
		tb.Fatalf("unexpected transient send response request id: got=%d want=%d", sendResp.RequestId, requestID)
	}
	accepted := sendResp.GetTransientAccepted()
	if accepted == nil {
		tb.Fatalf("expected transient accepted response body, got %+v", sendResp)
	}
	if accepted.PacketId == 0 {
		tb.Fatalf("expected transient accepted packet id")
	}
	if accepted.SourceNodeId != senderKey.NodeID || accepted.TargetNodeId != recipientKey.NodeID || !senderMatchesRef(accepted.Recipient, recipientKey) {
		tb.Fatalf("unexpected transient accepted response: %+v", accepted)
	}
	if accepted.DeliveryMode != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT {
		tb.Fatalf("unexpected transient accepted delivery mode: %s", accepted.DeliveryMode.String())
	}

	packetPushed := pushedEnvelope.envelope.GetPacketPushed()
	if packetPushed == nil || packetPushed.Packet == nil {
		tb.Fatalf("expected packet push envelope, got %+v", pushedEnvelope.envelope)
	}
	packet := packetPushed.Packet
	if packet.GetPacketId() != accepted.PacketId {
		tb.Fatalf("unexpected transient packet id: got=%d want=%d", packet.GetPacketId(), accepted.PacketId)
	}
	if packet.GetSourceNodeId() != senderKey.NodeID || packet.GetTargetNodeId() != recipientKey.NodeID {
		tb.Fatalf("unexpected transient packet route: %+v", packet)
	}
	if !senderMatchesRef(packet.GetRecipient(), recipientKey) || !senderMatchesRef(packet.GetSender(), senderKey) {
		tb.Fatalf("unexpected transient packet refs: %+v", packet)
	}
	if packet.GetDeliveryMode() != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT {
		tb.Fatalf("unexpected transient packet delivery mode: %s", packet.GetDeliveryMode().String())
	}
	if !bytes.Equal(packet.GetBody(), payload) {
		tb.Fatalf("unexpected transient packet body: got=%q want=%q", packet.GetBody(), payload)
	}

	return acceptedEnvelope.receivedAt.Sub(start), pushedEnvelope.receivedAt.Sub(start)
}

func mustBenchmarkManagerTransientPushToClientOnce(tb testing.TB, ctx context.Context, source *cluster.Manager, recipient *benchmarkClientWebSocket, packet store.TransientPacket, payload []byte, timeout time.Duration) {
	tb.Helper()

	if err := source.RouteTransientPacket(ctx, packet); err != nil {
		tb.Fatalf("route benchmark manager transient packet: %v", err)
	}
	pushedEnvelope := recipient.nextEnvelopeWithLabel(tb, timeout, "mesh-probe")
	packetPushed := pushedEnvelope.envelope.GetPacketPushed()
	if packetPushed == nil || packetPushed.Packet == nil {
		tb.Fatalf("expected packet push envelope for mesh probe, got %+v", pushedEnvelope.envelope)
	}
	got := packetPushed.Packet
	if got.GetPacketId() != packet.PacketID ||
		got.GetSourceNodeId() != packet.SourceNodeID ||
		got.GetTargetNodeId() != packet.TargetNodeID ||
		!senderMatchesRef(got.GetRecipient(), packet.Recipient) ||
		!senderMatchesRef(got.GetSender(), packet.Sender) ||
		!bytes.Equal(got.GetBody(), payload) {
		tb.Fatalf("unexpected mesh probe packet push: %+v", got)
	}
}

func assertBenchmarkTransientMessagesNotPersisted(tb testing.TB, svc *Service, userKey store.UserKey) {
	tb.Helper()

	messages, err := svc.ListMessagesByUser(context.Background(), userKey, 1)
	if err != nil {
		tb.Fatalf("list transient benchmark messages: %v", err)
	}
	if len(messages) != 0 {
		tb.Fatalf("expected transient benchmark messages to avoid persistence, got %+v", messages)
	}
}

func reportAPIBenchmarkAverageLatencyMetric(b *testing.B, total time.Duration, unit string) {
	if b.N <= 0 {
		return
	}
	b.ReportMetric(float64(total)/float64(time.Millisecond)/float64(b.N), unit)
}

func openBenchmarkClientWebSocketServer(tb testing.TB, handler http.Handler) (*httptest.Server, func()) {
	tb.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen on ipv4 loopback: %v", err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: handler},
	}
	server.Start()
	return server, server.Close
}

func dialBenchmarkClientWebSocket(tb testing.TB, serverURL string) *benchmarkClientWebSocket {
	tb.Helper()

	parsed, err := url.Parse(serverURL)
	if err != nil {
		tb.Fatalf("parse server url: %v", err)
	}
	parsed.Scheme = "ws"
	parsed.Path = "/ws/client"

	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		tb.Fatalf("dial client websocket: %v", err)
	}
	client := &benchmarkClientWebSocket{
		conn:      conn,
		envelopes: make(chan clientWebSocketBenchmarkEnvelope, 16),
		errs:      make(chan error, 1),
	}
	go client.readLoop()
	return client
}

func (c *benchmarkClientWebSocket) readLoop() {
	for {
		if err := c.conn.SetReadDeadline(time.Now().Add(clientWebSocketBenchmarkEnvelopeTimeout)); err != nil {
			c.reportReadError(fmt.Errorf("set websocket read deadline: %w", err))
			return
		}

		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			c.reportReadError(err)
			return
		}
		receivedAt := time.Now()
		if messageType != websocket.BinaryMessage {
			c.reportReadError(fmt.Errorf("unexpected websocket message type: %d", messageType))
			return
		}

		var envelope internalproto.ServerEnvelope
		if err := gproto.Unmarshal(data, &envelope); err != nil {
			c.reportReadError(fmt.Errorf("unmarshal server envelope: %w", err))
			return
		}
		c.envelopes <- clientWebSocketBenchmarkEnvelope{
			envelope:   &envelope,
			receivedAt: receivedAt,
		}
	}
}

func (c *benchmarkClientWebSocket) reportReadError(err error) {
	select {
	case c.errs <- err:
	default:
	}
}

func (c *benchmarkClientWebSocket) nextEnvelope(tb testing.TB, timeout time.Duration) clientWebSocketBenchmarkEnvelope {
	return c.nextEnvelopeWithLabel(tb, timeout, "client")
}

func (c *benchmarkClientWebSocket) nextEnvelopeWithLabel(tb testing.TB, timeout time.Duration, label string) clientWebSocketBenchmarkEnvelope {
	tb.Helper()

	select {
	case envelope := <-c.envelopes:
		return envelope
	case err := <-c.errs:
		tb.Fatalf("read websocket server envelope for %s: %v", label, err)
	case <-time.After(timeout):
		tb.Fatalf("timed out waiting for websocket server envelope for %s", label)
	}
	return clientWebSocketBenchmarkEnvelope{}
}

func (c *benchmarkClientWebSocket) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func benchmarkLoginClientWebSocket(tb testing.TB, client *benchmarkClientWebSocket, key store.UserKey, password string) {
	tb.Helper()

	benchmarkWriteClientEnvelope(tb, client.conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
				Password: password,
			},
		},
	})

	envelope := client.nextEnvelopeWithLabel(tb, clientWebSocketBenchmarkEnvelopeTimeout, "login")
	loginResp := envelope.envelope.GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != key.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		tb.Fatalf("unexpected websocket login response: %+v", envelope.envelope)
	}
}

func benchmarkWriteClientEnvelope(tb testing.TB, conn *websocket.Conn, envelope *internalproto.ClientEnvelope) {
	tb.Helper()

	data, err := gproto.Marshal(envelope)
	if err != nil {
		tb.Fatalf("marshal client envelope: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(clientWSWriteWait)); err != nil {
		tb.Fatalf("set websocket write deadline: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		tb.Fatalf("write client envelope: %v", err)
	}
}

func readBenchmarkServerEnvelopeOnce(tb testing.TB, conn *websocket.Conn, timeout time.Duration) *internalproto.ServerEnvelope {
	tb.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		tb.Fatalf("set websocket read deadline: %v", err)
	}
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		tb.Fatalf("read websocket server envelope: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		tb.Fatalf("unexpected websocket message type: %d", messageType)
	}
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(data, &envelope); err != nil {
		tb.Fatalf("unmarshal websocket server envelope: %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		tb.Fatalf("clear websocket read deadline: %v", err)
	}
	return &envelope
}

func dialAndLoginBenchmarkIdleClientWebSocket(tb testing.TB, serverURL string, key store.UserKey, password string) *websocket.Conn {
	tb.Helper()

	conn, err := dialAndLoginBenchmarkIdleClientWebSocketConn(serverURL, key, password)
	if err != nil {
		tb.Fatalf("dial idle client websocket: %v", err)
	}
	return conn
}

func dialAndLoginBenchmarkIdleClientWebSockets(tb testing.TB, serverURL string, keys []store.UserKey, password string, parallelism int) []*websocket.Conn {
	tb.Helper()

	if len(keys) == 0 {
		return nil
	}
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(keys) {
		parallelism = len(keys)
	}

	conns := make([]*websocket.Conn, len(keys))
	indexes := make(chan int, len(keys))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indexes {
				conn, err := dialAndLoginBenchmarkIdleClientWebSocketConn(serverURL, keys[idx], password)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				conns[idx] = conn
			}
		}()
	}

	for idx := range keys {
		indexes <- idx
	}
	close(indexes)
	wg.Wait()

	select {
	case err := <-errCh:
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
		tb.Fatalf("dial idle client websockets: %v", err)
	default:
	}
	return conns
}

func dialAndLoginBenchmarkIdleClientWebSocketConn(serverURL string, key store.UserKey, password string) (*websocket.Conn, error) {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("parse server url: %w", err)
	}
	parsed.Scheme = "ws"
	parsed.Path = "/ws/client"

	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	loginEnvelope := &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
				Password: password,
			},
		},
	}
	data, err := gproto.Marshal(loginEnvelope)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("marshal login envelope: %w", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(clientWSWriteWait)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set login write deadline: %w", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write login envelope: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(clientWebSocketBenchmarkEnvelopeTimeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set login read deadline: %w", err)
	}
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read login response: %w", err)
	}
	if messageType != websocket.BinaryMessage {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected login websocket message type: %d", messageType)
	}
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(data, &envelope); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("unmarshal login response: %w", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clear login read deadline: %w", err)
	}
	loginResp := envelope.GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != key.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected idle websocket login response")
	}
	return conn, nil
}

func silenceAPIBenchmarkLogs(tb testing.TB) {
	tb.Helper()

	originalLogger := log.Logger
	log.Logger = zerolog.New(io.Discard)
	tb.Cleanup(func() {
		log.Logger = originalLogger
	})
}

func (s benchmarkClusterSink) Publish(event store.Event) {
	if s.manager == nil {
		return
	}
	s.manager.Publish(event)
}

func (s benchmarkClusterSink) RouteTransientPacket(ctx context.Context, packet store.TransientPacket) error {
	if s.manager == nil {
		return nil
	}
	return s.manager.RouteTransientPacket(ctx, packet)
}

func openBenchmarkAuthenticatedLinearMeshAPICluster(tb testing.TB, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, nodeCount int) *benchmarkLinearMeshAPICluster {
	tb.Helper()

	if nodeCount < 2 {
		tb.Fatalf("benchmark linear mesh cluster requires at least 2 nodes, got %d", nodeCount)
	}

	return openBenchmarkAuthenticatedLinearMeshAPIClusterWithEventLogLimit(tb, mode, scenario, nodeCount, store.DefaultEventLogMaxEventsPerOrigin)
}

func (c *benchmarkLinearMeshAPICluster) Close() {
	for idx := len(c.closeFns) - 1; idx >= 0; idx-- {
		c.closeFns[idx]()
	}
}

func openBenchmarkAuthenticatedLinearMeshAPIClusterWithEventLogLimit(tb testing.TB, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, nodeCount int, maxEventsPerOrigin int) *benchmarkLinearMeshAPICluster {
	tb.Helper()

	if nodeCount < 2 {
		tb.Fatalf("benchmark linear mesh cluster requires at least 2 nodes, got %d", nodeCount)
	}

	meshCluster := &benchmarkLinearMeshAPICluster{
		nodes: make([]benchmarkLinearMeshAPINode, 0, nodeCount),
	}
	serverURLs := make([]string, 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeSlot := uint16(idx + 1)
		st, closeStore := openBenchmarkAPIClusterStoreWithEventLogLimit(tb, mode, scenario, fmt.Sprintf("bench-mesh-node-%d", idx+1), nodeSlot, maxEventsPerOrigin)
		meshCluster.closeFns = append(meshCluster.closeFns, closeStore)

		cfg := benchmarkAPIClusterBaseConfig(nodeSlot)
		if idx > 0 {
			cfg.Peers = []cluster.Peer{{URL: benchmarkWebSocketURL(serverURLs[idx-1]) + cluster.WebSocketPath}}
		}

		manager, err := cluster.NewManager(cfg, st)
		if err != nil {
			tb.Fatalf("new benchmark cluster manager: %v", err)
		}

		signer, err := auth.NewSigner("token-secret")
		if err != nil {
			tb.Fatalf("new signer: %v", err)
		}
		service := New(st, benchmarkClusterSink{manager: manager})
		httpAPI := NewHTTP(service, HTTPOptions{
			NodeID:   st.NodeID(),
			Signer:   signer,
			TokenTTL: time.Hour,
		})
		manager.SetTransientHandler(httpAPI.ReceiveTransientPacket)
		manager.SetLoggedInUsersProvider(httpAPI.ListLoggedInUsers)

		handler := benchmarkClusterAPIHandler(httpAPI.Handler(), manager)
		server, closeServer := openBenchmarkClientWebSocketServer(tb, handler)
		meshCluster.closeFns = append(meshCluster.closeFns, closeServer)

		if err := manager.Start(context.Background()); err != nil {
			tb.Fatalf("start benchmark cluster manager: %v", err)
		}
		meshCluster.closeFns = append(meshCluster.closeFns, func() {
			_ = manager.Close()
		})

		meshCluster.nodes = append(meshCluster.nodes, benchmarkLinearMeshAPINode{
			nodeID:    st.NodeID(),
			manager:   manager,
			service:   service,
			http:      httpAPI,
			handler:   handler,
			serverURL: server.URL,
		})
		serverURLs = append(serverURLs, server.URL)
	}
	return meshCluster
}

func openBenchmarkAPIClusterStore(tb testing.TB, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, name string, nodeSlot uint16) (*store.Store, func()) {
	return openBenchmarkAPIClusterStoreWithEventLogLimit(tb, mode, scenario, name, nodeSlot, store.DefaultEventLogMaxEventsPerOrigin)
}

func openBenchmarkAPIClusterStoreWithEventLogLimit(tb testing.TB, mode benchroot.Mode, scenario apiBenchmarkEngineScenario, name string, nodeSlot uint16, maxEventsPerOrigin int) (*store.Store, func()) {
	tb.Helper()

	dir, cleanupDir := mode.MkdirTemp(tb, "turntf-api-mesh-bench-*")
	opts := store.Options{
		NodeID:                     testNodeID(nodeSlot),
		Engine:                     scenario.engine,
		PebbleProfile:              scenario.pebbleProfile,
		PebbleMessageSyncMode:      scenario.pebbleMessageSyncMode,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	}
	if scenario.engine == store.EnginePebble {
		opts.PebblePath = filepath.Join(dir, name+".pebble")
	}
	st, err := store.Open(filepath.Join(dir, name+".db"), opts)
	if err != nil {
		cleanupDir()
		tb.Fatalf("open benchmark api cluster store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("init benchmark api cluster store: %v", err)
	}
	if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: benchmarkMustHashPassword(tb, "root-password"),
	}); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("ensure benchmark bootstrap admin: %v", err)
	}
	return st, func() {
		_ = st.Close()
		cleanupDir()
	}
}

func benchmarkAPIClusterBaseConfig(nodeSlot uint16) cluster.Config {
	return cluster.Config{
		NodeID:            testNodeID(nodeSlot),
		AdvertisePath:     cluster.WebSocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    cluster.DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}
}

func benchmarkClusterAPIHandler(apiHandler http.Handler, manager *cluster.Manager) http.Handler {
	rootMux := http.NewServeMux()
	rootMux.Handle("/", apiHandler)
	if manager != nil {
		rootMux.Handle(cluster.WebSocketPath, manager.Handler())
	}
	return rootMux
}

func benchmarkWebSocketURL(serverURL string) string {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return serverURL
	}
	parsed.Scheme = "ws"
	return parsed.String()
}

func benchmarkAPIClientTimeout(mode benchroot.Mode, tmpTimeout, diskTimeout time.Duration) time.Duration {
	if mode.Name() == benchroot.ModeDisk {
		return diskTimeout
	}
	return tmpTimeout
}

func waitForAPIBenchmarkMeshRoute(tb testing.TB, sourceNodeID int64, manager *cluster.Manager, destinationNodeID int64, trafficClass mesh.TrafficClass, timeout time.Duration) {
	tb.Helper()

	waitForAPIBenchmarkCondition(tb, timeout, func() bool {
		binding := manager.MeshRuntime()
		if binding == nil {
			return false
		}
		planner := mesh.NewPlanner(sourceNodeID)
		_, ok := planner.Compute(binding.TopologyStore().Snapshot(), destinationNodeID, trafficClass, mesh.TransportUnspecified)
		return ok
	})
}

func waitForAPIBenchmarkCondition(tb testing.TB, timeout time.Duration, fn func() bool) {
	tb.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !fn() {
		tb.Fatalf("benchmark condition not met within %s", timeout)
	}
}

func waitForAPIBenchmarkOnlineUsers(tb testing.TB, timeout time.Duration, nodes []benchmarkLinearMeshAPINode, expectedCounts []int) {
	tb.Helper()

	if len(nodes) != len(expectedCounts) {
		tb.Fatalf("expected counts mismatch: nodes=%d counts=%d", len(nodes), len(expectedCounts))
	}
	for idx, node := range nodes {
		want := expectedCounts[idx]
		waitForAPIBenchmarkCondition(tb, timeout, func() bool {
			users, err := node.http.ListLoggedInUsers(context.Background())
			return err == nil && len(users) == want
		})
	}
}

func benchmarkDistributedUserCounts(totalUsers, nodeCount int) []int {
	counts := make([]int, nodeCount)
	for i := 0; i < totalUsers; i++ {
		counts[i%nodeCount]++
	}
	return counts
}

func seedBenchmarkLocalUsers(tb testing.TB, ctx context.Context, node benchmarkLinearMeshAPINode, count int, usernamePrefix, passwordHash string) []store.UserKey {
	tb.Helper()

	keys := make([]store.UserKey, 0, count)
	for i := 0; i < count; i++ {
		user, _, err := node.service.store.CreateUser(ctx, store.CreateUserParams{
			Username:     fmt.Sprintf("%s-%d-%05d", usernamePrefix, node.nodeID, i),
			PasswordHash: passwordHash,
			Role:         store.RoleUser,
		})
		if err != nil {
			tb.Fatalf("create local benchmark user on node %d #%d: %v", node.nodeID, i, err)
		}
		keys = append(keys, user.Key())
	}
	return keys
}
