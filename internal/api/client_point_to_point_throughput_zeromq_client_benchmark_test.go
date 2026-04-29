//go:build zeromq

package api

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pebbe/zmq4"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type clientZeroMQTransientPointToPointThroughputScenario struct {
	name  string
	build func(testing.TB) *clientZeroMQTransientPointToPointThroughputTopology
}

type clientZeroMQTransientPointToPointThroughputTopology struct {
	source               benchmarkLinearMeshAPINode
	target               benchmarkLinearMeshAPINode
	sourceBindURL        string
	targetBindURL        string
	expectedTransport    mesh.TransportKind
	sourceNextHopNodeID  int64
	reverseNextHopNodeID int64
	deliveryTimeout      time.Duration
	closeFns             []func()
}

type benchmarkClientZeroMQ struct {
	socket *zmq4.Socket
}

type clientZeroMQTransientPointToPointThroughputWorker struct {
	sender       *benchmarkClientZeroMQ
	recipient    *benchmarkClientZeroMQ
	recipientKey store.UserKey
}

func BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput(b *testing.B) {
	scenarios := []clientZeroMQTransientPointToPointThroughputScenario{
		{name: "zeromq-client/direct-2nodes", build: buildClientZeroMQTransientPointToPointZeroMQDirectTopology},
		{name: "zeromq-client/linear-7nodes", build: buildClientZeroMQTransientPointToPointZeroMQLinear7NodesTopology},
	}

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
				benchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput(b, scenario, tc.payloadSize)
			})
		}
	}
}

func benchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput(b *testing.B, scenario clientZeroMQTransientPointToPointThroughputScenario, payloadSize int) {
	b.StopTimer()
	silenceAPIBenchmarkLogs(b)

	ctx := context.Background()
	timeout := clientTransientPointToPointThroughputTimeout
	topology := scenario.build(b)
	b.Cleanup(topology.Close)

	waitForAPIBenchmarkMeshRouteDecision(b, topology.source.manager, topology.target.nodeID, mesh.TrafficTransientInteractive, topology.sourceNextHopNodeID, topology.expectedTransport, timeout)
	waitForAPIBenchmarkMeshRouteDecision(b, topology.source.manager, topology.target.nodeID, mesh.TrafficControlCritical, topology.sourceNextHopNodeID, topology.expectedTransport, timeout)
	waitForAPIBenchmarkMeshRouteDecision(b, topology.source.manager, topology.target.nodeID, mesh.TrafficControlQuery, topology.sourceNextHopNodeID, topology.expectedTransport, timeout)
	waitForAPIBenchmarkMeshRouteDecision(b, topology.target.manager, topology.source.nodeID, mesh.TrafficControlCritical, topology.reverseNextHopNodeID, topology.expectedTransport, timeout)
	waitForAPIBenchmarkMeshRouteDecision(b, topology.target.manager, topology.source.nodeID, mesh.TrafficReplicationStream, topology.reverseNextHopNodeID, topology.expectedTransport, timeout)

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}

	passwordHash := benchmarkMustHashPassword(b, "bench-password")
	recipients := make([]store.UserKey, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		user, _, err := topology.target.service.CreateUser(ctx, store.CreateUserParams{
			Username:     fmt.Sprintf("bench-zmq-p2p-throughput-recipient-%d-%d", topology.target.nodeID, i),
			PasswordHash: passwordHash,
			Role:         store.RoleUser,
		})
		if err != nil {
			b.Fatalf("create zeromq point-to-point throughput recipient %d: %v", i, err)
		}
		recipients = append(recipients, user.Key())
	}

	for _, recipientKey := range recipients {
		recipientKey := recipientKey
		waitForAPIBenchmarkCondition(b, timeout, func() bool {
			_, err := topology.source.service.GetUser(ctx, recipientKey)
			return err == nil
		})
	}

	adminKey := store.UserKey{NodeID: topology.source.nodeID, UserID: store.BootstrapAdminUserID}
	workers := make([]clientZeroMQTransientPointToPointThroughputWorker, 0, workerCount)
	for idx, recipientKey := range recipients {
		sender := dialBenchmarkClientZeroMQ(b, topology.sourceBindURL)
		recipient := dialBenchmarkClientZeroMQ(b, topology.targetBindURL)
		b.Cleanup(func() { _ = sender.Close() })
		b.Cleanup(func() { _ = recipient.Close() })

		benchmarkLoginClientZeroMQ(b, sender, adminKey, "root-password")
		benchmarkLoginClientZeroMQ(b, recipient, recipientKey, "bench-password")

		waitForAPIBenchmarkLoggedInUser(b, topology.target.http, recipientKey, timeout)
		mustBenchmarkManagerTransientPushToZeroMQClientOnce(b, ctx, topology.source.manager, recipient, store.TransientPacket{
			PacketID:     (1 << 62) + uint64(idx) + 1,
			SourceNodeID: topology.source.nodeID,
			TargetNodeID: recipientKey.NodeID,
			Recipient:    recipientKey,
			Sender:       adminKey,
			Body:         []byte("p2p-probe"),
			DeliveryMode: store.DeliveryModeBestEffort,
			TTLHops:      8,
		}, []byte("p2p-probe"), timeout)

		workers = append(workers, clientZeroMQTransientPointToPointThroughputWorker{
			sender:       sender,
			recipient:    recipient,
			recipientKey: recipientKey,
		})
	}

	payload := bytes.Repeat([]byte("t"), payloadSize)
	var requestID atomic.Uint64
	runAPIBenchmarkWarmup(b, func() {
		for _, worker := range workers {
			mustBenchmarkClientZeroMQTransientSendMessageThroughputOnce(b, worker.sender, worker.recipient, adminKey, worker.recipientKey, payload, requestID.Add(1), timeout)
		}
	})
	assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.source.service, recipients)
	assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.target.service, recipients)

	var workerSlot atomic.Uint64
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()
	b.SetParallelism(1)
	b.RunParallel(func(pb *testing.PB) {
		slot := int(workerSlot.Add(1) - 1)
		if slot < 0 || slot >= len(workers) {
			b.Fatalf("unexpected zeromq throughput worker slot %d for %d workers", slot, len(workers))
		}
		worker := workers[slot]
		for pb.Next() {
			mustBenchmarkClientZeroMQTransientSendMessageThroughputOnce(b, worker.sender, worker.recipient, adminKey, worker.recipientKey, payload, requestID.Add(1), timeout)
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(payloadSize), "bytes/op")

	assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.source.service, recipients)
	assertBenchmarkTransientMessagesNotPersistedForKeys(b, topology.target.service, recipients)
}

func (t *clientZeroMQTransientPointToPointThroughputTopology) Close() {
	if t == nil {
		return
	}
	for idx := len(t.closeFns) - 1; idx >= 0; idx-- {
		if t.closeFns[idx] != nil {
			t.closeFns[idx]()
		}
	}
}

func buildClientZeroMQTransientPointToPointZeroMQDirectTopology(tb testing.TB) *clientZeroMQTransientPointToPointThroughputTopology {
	tb.Helper()

	targetCfg := benchmarkAPIClusterBaseConfig(2)
	targetCfg.ZeroMQ = cluster.ZeroMQConfig{
		Enabled: true,
		BindURL: benchmarkPointToPointAPINextZeroMQTCPAddress(tb),
	}
	targetNode, closeTarget := openBenchmarkAuthenticatedPointToPointAPINode(tb, "bench-api-zmq-client-p2p-zeromq-target", 2, &targetCfg)
	closeTargetListener := benchmarkPointToPointAPIStartZeroMQMuxListener(tb, targetNode, targetCfg.ZeroMQ)
	targetPeerURL := benchmarkPointToPointAPIZeroMQPeerURLForBindURL(targetCfg.ZeroMQ.BindURL)

	sourceCfg := benchmarkAPIClusterBaseConfig(1)
	sourceCfg.ZeroMQ = cluster.ZeroMQConfig{
		Enabled: true,
		BindURL: benchmarkPointToPointAPINextZeroMQTCPAddress(tb),
	}
	sourceCfg.Peers = []cluster.Peer{{URL: targetPeerURL}}
	sourceNode, closeSource := openBenchmarkAuthenticatedPointToPointAPINode(tb, "bench-api-zmq-client-p2p-zeromq-source", 1, &sourceCfg)
	closeSourceListener := benchmarkPointToPointAPIStartZeroMQMuxListener(tb, sourceNode, sourceCfg.ZeroMQ)

	return &clientZeroMQTransientPointToPointThroughputTopology{
		source:               sourceNode,
		target:               targetNode,
		sourceBindURL:        sourceCfg.ZeroMQ.BindURL,
		targetBindURL:        targetCfg.ZeroMQ.BindURL,
		expectedTransport:    mesh.TransportZeroMQ,
		sourceNextHopNodeID:  targetNode.nodeID,
		reverseNextHopNodeID: sourceNode.nodeID,
		deliveryTimeout:      clientTransientPointToPointThroughputTimeout,
		closeFns:             []func(){closeTarget, closeSource, closeTargetListener, closeSourceListener},
	}
}

func buildClientZeroMQTransientPointToPointZeroMQLinear7NodesTopology(tb testing.TB) *clientZeroMQTransientPointToPointThroughputTopology {
	tb.Helper()
	return buildClientZeroMQTransientPointToPointLinearZeroMQTopology(tb, 7)
}

func buildClientZeroMQTransientPointToPointLinearZeroMQTopology(tb testing.TB, nodeCount int) *clientZeroMQTransientPointToPointThroughputTopology {
	tb.Helper()
	if nodeCount < 2 {
		tb.Fatalf("zeromq client linear point-to-point throughput topology requires at least 2 nodes, got %d", nodeCount)
	}

	nodes := make([]benchmarkLinearMeshAPINode, 0, nodeCount)
	closeFns := make([]func(), 0, nodeCount*2)
	peerURLs := make([]string, 0, nodeCount)
	bindURLs := make([]string, 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeSlot := uint16(idx + 1)
		cfg := benchmarkAPIClusterBaseConfig(nodeSlot)
		cfg.ZeroMQ = cluster.ZeroMQConfig{
			Enabled: true,
			BindURL: benchmarkPointToPointAPINextZeroMQTCPAddress(tb),
		}
		if idx > 0 {
			cfg.Peers = []cluster.Peer{{URL: peerURLs[idx-1]}}
		}
		node, closeNode := openBenchmarkAuthenticatedPointToPointAPINode(tb, fmt.Sprintf("bench-api-zmq-client-p2p-zeromq-linear-%d", idx+1), nodeSlot, &cfg)
		nodes = append(nodes, node)
		closeFns = append(closeFns, closeNode)
		closeFns = append(closeFns, benchmarkPointToPointAPIStartZeroMQMuxListener(tb, node, cfg.ZeroMQ))
		peerURLs = append(peerURLs, benchmarkPointToPointAPIZeroMQPeerURLForBindURL(cfg.ZeroMQ.BindURL))
		bindURLs = append(bindURLs, cfg.ZeroMQ.BindURL)
	}

	return &clientZeroMQTransientPointToPointThroughputTopology{
		source:               nodes[0],
		target:               nodes[len(nodes)-1],
		sourceBindURL:        bindURLs[0],
		targetBindURL:        bindURLs[len(bindURLs)-1],
		expectedTransport:    mesh.TransportZeroMQ,
		sourceNextHopNodeID:  nodes[1].nodeID,
		reverseNextHopNodeID: nodes[len(nodes)-2].nodeID,
		deliveryTimeout:      clientTransientPointToPointThroughputTimeout,
		closeFns:             closeFns,
	}
}

func benchmarkPointToPointAPIStartZeroMQMuxListener(tb testing.TB, node benchmarkLinearMeshAPINode, cfg cluster.ZeroMQConfig) func() {
	tb.Helper()
	if node.manager == nil || node.http == nil {
		tb.Fatalf("expected zeromq benchmark node with manager and http")
	}

	ctx, cancel := context.WithCancel(context.Background())
	listener := cluster.NewZeroMQMuxListenerWithConfig(cfg.BindURL, cfg)
	listener.SetClusterAccept(node.manager.AcceptZeroMQConn)
	listener.SetClientAccept(func(conn cluster.TransportConn) {
		node.http.AcceptZeroMQConn(conn)
	})
	if err := listener.Start(ctx); err != nil {
		cancel()
		tb.Fatalf("start zeromq benchmark mux listener: %v", err)
	}
	node.manager.SetZeroMQListenerRunning(true)
	return func() {
		node.manager.SetZeroMQListenerRunning(false)
		cancel()
		_ = listener.Close()
	}
}

func dialBenchmarkClientZeroMQ(tb testing.TB, bindURL string) *benchmarkClientZeroMQ {
	tb.Helper()

	socket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		tb.Fatalf("new zeromq benchmark socket: %v", err)
	}
	if err := socket.SetLinger(0); err != nil {
		_ = socket.Close()
		tb.Fatalf("set zeromq benchmark linger: %v", err)
	}
	if err := socket.SetImmediate(true); err != nil {
		_ = socket.Close()
		tb.Fatalf("set zeromq benchmark immediate: %v", err)
	}
	identity := time.Now().UTC().Format("20060102150405.000000000")
	if err := socket.SetIdentity(identity); err != nil {
		_ = socket.Close()
		tb.Fatalf("set zeromq benchmark identity: %v", err)
	}
	if err := socket.Connect(bindURL); err != nil {
		_ = socket.Close()
		tb.Fatalf("connect zeromq benchmark socket: %v", err)
	}
	hello, err := gproto.Marshal(&internalproto.ZeroMQMuxHello{
		Role:            internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT,
		ProtocolVersion: internalproto.ZeroMQMuxProtocolVersion,
	})
	if err != nil {
		_ = socket.Close()
		tb.Fatalf("marshal zeromq benchmark mux hello: %v", err)
	}
	if _, err := socket.SendBytes(hello, 0); err != nil {
		_ = socket.Close()
		tb.Fatalf("send zeromq benchmark mux hello: %v", err)
	}
	return &benchmarkClientZeroMQ{socket: socket}
}

func (c *benchmarkClientZeroMQ) nextEnvelopeWithLabel(tb testing.TB, timeout time.Duration, label string) *internalproto.ServerEnvelope {
	tb.Helper()

	poller := zmq4.NewPoller()
	poller.Add(c.socket, zmq4.POLLIN)
	polled, err := poller.Poll(timeout)
	if err != nil {
		tb.Fatalf("poll zeromq server envelope for %s: %v", label, err)
	}
	if len(polled) == 0 {
		tb.Fatalf("timed out waiting for zeromq server envelope for %s", label)
	}
	frames, err := c.socket.RecvMessageBytes(0)
	if err != nil {
		tb.Fatalf("recv zeromq server envelope for %s: %v", label, err)
	}
	if len(frames) == 0 {
		tb.Fatalf("expected zeromq server envelope for %s", label)
	}
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(frames[len(frames)-1], &envelope); err != nil {
		tb.Fatalf("unmarshal zeromq server envelope for %s: %v", label, err)
	}
	return &envelope
}

func (c *benchmarkClientZeroMQ) Close() error {
	if c == nil || c.socket == nil {
		return nil
	}
	return c.socket.Close()
}

func benchmarkLoginClientZeroMQ(tb testing.TB, client *benchmarkClientZeroMQ, key store.UserKey, password string) {
	tb.Helper()

	benchmarkWriteClientEnvelopeZMQ(tb, client.socket, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
				Password: password,
			},
		},
	})

	envelope := client.nextEnvelopeWithLabel(tb, clientTransientPointToPointThroughputTimeout, "zeromq-login")
	loginResp := envelope.GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != key.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		tb.Fatalf("unexpected zeromq login response: %+v", envelope)
	}
}

func benchmarkWriteClientEnvelopeZMQ(tb testing.TB, socket *zmq4.Socket, envelope *internalproto.ClientEnvelope) {
	tb.Helper()

	data, err := gproto.Marshal(envelope)
	if err != nil {
		tb.Fatalf("marshal zeromq client envelope: %v", err)
	}
	if _, err := socket.SendBytes(data, 0); err != nil {
		tb.Fatalf("send zeromq client envelope: %v", err)
	}
}

func mustBenchmarkClientZeroMQTransientSendMessageThroughputOnce(tb testing.TB, sender, recipient *benchmarkClientZeroMQ, senderKey, recipientKey store.UserKey, payload []byte, requestID uint64, timeout time.Duration) {
	tb.Helper()

	benchmarkWriteClientEnvelopeZMQ(tb, sender.socket, &internalproto.ClientEnvelope{
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

	acceptedEnvelope := sender.nextEnvelopeWithLabel(tb, timeout, "throughput-zmq-sender")
	pushedEnvelope := recipient.nextEnvelopeWithLabel(tb, timeout, "throughput-zmq-recipient")

	sendResp := acceptedEnvelope.GetSendMessageResponse()
	if sendResp == nil {
		tb.Fatalf("expected zeromq throughput transient send response envelope, got %+v", acceptedEnvelope)
	}
	if sendResp.RequestId != requestID {
		tb.Fatalf("unexpected zeromq throughput transient request id: got=%d want=%d", sendResp.RequestId, requestID)
	}
	accepted := sendResp.GetTransientAccepted()
	if accepted == nil {
		tb.Fatalf("expected zeromq throughput transient accepted response body, got %+v", sendResp)
	}
	if accepted.PacketId == 0 {
		tb.Fatalf("expected zeromq throughput transient accepted packet id")
	}
	if accepted.SourceNodeId != senderKey.NodeID || accepted.TargetNodeId != recipientKey.NodeID || !senderMatchesRef(accepted.Recipient, recipientKey) {
		tb.Fatalf("unexpected zeromq throughput transient accepted response: %+v", accepted)
	}

	packetPushed := pushedEnvelope.GetPacketPushed()
	if packetPushed == nil || packetPushed.Packet == nil {
		tb.Fatalf("expected zeromq throughput packet push envelope, got %+v", pushedEnvelope)
	}
	packet := packetPushed.Packet
	if packet.GetPacketId() != accepted.PacketId {
		tb.Fatalf("unexpected zeromq throughput transient packet id: got=%d want=%d", packet.GetPacketId(), accepted.PacketId)
	}
	if packet.GetSourceNodeId() != senderKey.NodeID || packet.GetTargetNodeId() != recipientKey.NodeID {
		tb.Fatalf("unexpected zeromq throughput transient packet route: %+v", packet)
	}
	if !senderMatchesRef(packet.GetRecipient(), recipientKey) || !senderMatchesRef(packet.GetSender(), senderKey) {
		tb.Fatalf("unexpected zeromq throughput transient packet refs: %+v", packet)
	}
	if packet.GetDeliveryMode() != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT {
		tb.Fatalf("unexpected zeromq throughput transient packet delivery mode: %s", packet.GetDeliveryMode().String())
	}
	if !bytes.Equal(packet.GetBody(), payload) {
		tb.Fatalf("unexpected zeromq throughput transient packet body: got=%q want=%q", packet.GetBody(), payload)
	}
}

func mustBenchmarkManagerTransientPushToZeroMQClientOnce(tb testing.TB, ctx context.Context, source *cluster.Manager, recipient *benchmarkClientZeroMQ, packet store.TransientPacket, payload []byte, timeout time.Duration) {
	tb.Helper()

	if err := source.RouteTransientPacket(ctx, packet); err != nil {
		tb.Fatalf("route benchmark manager transient packet to zeromq client: %v", err)
	}
	pushedEnvelope := recipient.nextEnvelopeWithLabel(tb, timeout, "zmq-mesh-probe")
	packetPushed := pushedEnvelope.GetPacketPushed()
	if packetPushed == nil || packetPushed.Packet == nil {
		tb.Fatalf("expected zeromq packet push envelope for mesh probe, got %+v", pushedEnvelope)
	}
	got := packetPushed.Packet
	if got.GetPacketId() != packet.PacketID ||
		got.GetSourceNodeId() != packet.SourceNodeID ||
		got.GetTargetNodeId() != packet.TargetNodeID ||
		!senderMatchesRef(got.GetRecipient(), packet.Recipient) ||
		!senderMatchesRef(got.GetSender(), packet.Sender) ||
		!bytes.Equal(got.GetBody(), payload) {
		tb.Fatalf("unexpected zeromq mesh probe packet push: %+v", got)
	}
}
