package api

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

const (
	persistentClientBenchmarkPayloadSize = 256
	persistentDirectSequenceBase         = int64(1 << 20)
	persistentBroadcastSequenceBase      = int64(2 << 20)
	persistentChannelSequenceBase        = int64(3 << 20)
)

type benchmarkPersistentLoginFixture struct {
	http           *HTTP
	serverURL      string
	userKey        store.UserKey
	password       string
	expectedBodies [][]byte
	timeout        time.Duration
	closeFns       []func()
}

type benchmarkPersistentDelivery struct {
	message    *internalproto.Message
	receivedAt time.Time
}

type benchmarkPersistentClient struct {
	key        store.UserKey
	conn       *websocket.Conn
	deliveries chan benchmarkPersistentDelivery
	errs       chan error
	stop       chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

type benchmarkPersistentResponse struct {
	envelope   *internalproto.ServerEnvelope
	receivedAt time.Time
}

type benchmarkPersistentSender struct {
	conn      *websocket.Conn
	responses chan benchmarkPersistentResponse
	errs      chan error
	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type benchmarkPersistentDispatchScenario struct {
	name       string
	target     store.UserKey
	recipients []*benchmarkPersistentClient
}

type benchmarkPersistentDispatchFixture struct {
	cluster          *benchmarkLinearMeshAPICluster
	broadcastKey     store.UserKey
	channelKey       store.UserKey
	directRecipient  *benchmarkPersistentClient
	clients          []*benchmarkPersistentClient
	channelClients   []*benchmarkPersistentClient
	sender           *benchmarkPersistentSender
	onlineUsersTotal int
	nodeCount        int
	timeout          time.Duration
	nextRequestID    uint64
}

func TestClientWebSocketPersistentLoginBenchmarkFixture(t *testing.T) {
	mode := benchroot.Modes(t)[0]
	fixture := openBenchmarkPersistentLoginFixture(t, mode, 2)
	defer fixture.Close()

	loginLatency, catchupLatency := fixture.LoginOnce(t)
	if loginLatency <= 0 {
		t.Fatalf("expected positive login latency, got %s", loginLatency)
	}
	if catchupLatency < loginLatency {
		t.Fatalf("catchup completed before login: login=%s catchup=%s", loginLatency, catchupLatency)
	}
}

func BenchmarkClientWebSocketPersistentLoginAuthenticated(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, historyCount := range []int{0, 100, 1000} {
				b.Run(fmt.Sprintf("%s/history-%d/256B", store.EngineSQLite, historyCount), func(b *testing.B) {
					silenceAPIBenchmarkLogs(b)
					fixture := openBenchmarkPersistentLoginFixture(b, mode, historyCount)
					b.Cleanup(fixture.Close)

					_, _ = fixture.LoginOnce(b)
					b.ReportMetric(float64(historyCount), "history_messages/op")
					if historyCount > 0 {
						b.SetBytes(int64(historyCount * persistentClientBenchmarkPayloadSize))
					}
					b.ResetTimer()

					var totalLogin time.Duration
					var totalCatchup time.Duration
					for i := 0; i < b.N; i++ {
						loginLatency, catchupLatency := fixture.LoginOnce(b)
						totalLogin += loginLatency
						totalCatchup += catchupLatency
					}

					b.StopTimer()
					reportAPIBenchmarkAverageLatencyMetric(b, totalLogin, "login_ms/op")
					reportAPIBenchmarkAverageLatencyMetric(b, totalCatchup, "catchup_ms/op")
				})
			}
		})
	}
}

func BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, nodeCount := range []int{3, 7} {
				for _, onlineUsersTotal := range []int{1000, 5000, 10000} {
					b.Run(fmt.Sprintf("%s/%d-nodes/%d-online", store.EngineSQLite, nodeCount, onlineUsersTotal), func(b *testing.B) {
						silenceAPIBenchmarkLogs(b)
						fixture := openBenchmarkPersistentDispatchFixture(b, mode, nodeCount, onlineUsersTotal)
						b.Cleanup(fixture.Close)

						for _, scenario := range fixture.Scenarios() {
							scenario := scenario
							b.Run(scenario.name+"/256B", func(b *testing.B) {
								fixture.RunScenario(b, scenario)
							})
						}
					})
				}
			}
		})
	}
}

func openBenchmarkPersistentLoginFixture(tb testing.TB, mode benchroot.Mode, historyCount int) *benchmarkPersistentLoginFixture {
	tb.Helper()

	if historyCount < 0 || historyCount > 1000 {
		tb.Fatalf("persistent login history must be between 0 and 1000, got %d", historyCount)
	}
	scenario := apiBenchmarkEngineScenario{name: store.EngineSQLite, engine: store.EngineSQLite}
	testAPI, closeAPI := openBenchmarkAuthenticatedTestAPIWithMessageWindowSize(tb, mode, scenario, 1000)
	server, closeServer := openBenchmarkClientWebSocketServer(tb, testAPI.handler)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := benchmarkLoginToken(tb, testAPI.handler, adminKey, "root-password")
	userKey := benchmarkCreateUserAs(tb, testAPI.handler, adminToken, fmt.Sprintf("bench-persistent-login-%d", historyCount), "bench-password", store.RoleUser)
	expectedBodies := make([][]byte, 0, historyCount)
	for i := 0; i < historyCount; i++ {
		prefix := []byte(fmt.Sprintf("persistent-history-%04d-", i))
		body := append(prefix, bytes.Repeat([]byte("h"), persistentClientBenchmarkPayloadSize-len(prefix))...)
		if _, _, err := testAPI.http.service.CreateMessage(context.Background(), store.CreateMessageParams{
			UserKey: userKey,
			Sender:  adminKey,
			Body:    body,
		}); err != nil {
			closeServer()
			closeAPI()
			tb.Fatalf("seed persistent login history %d: %v", i, err)
		}
		expectedBodies = append(expectedBodies, body)
	}

	return &benchmarkPersistentLoginFixture{
		http:           testAPI.http,
		serverURL:      server.URL,
		userKey:        userKey,
		password:       "bench-password",
		expectedBodies: expectedBodies,
		timeout:        benchmarkAPIClientTimeout(mode, 10*time.Second, 30*time.Second),
		closeFns:       []func(){closeServer, closeAPI},
	}
}

func (f *benchmarkPersistentLoginFixture) Close() {
	for idx := len(f.closeFns) - 1; idx >= 0; idx-- {
		f.closeFns[idx]()
	}
}

func (f *benchmarkPersistentLoginFixture) LoginOnce(tb testing.TB) (time.Duration, time.Duration) {
	tb.Helper()

	parsed, err := url.Parse(f.serverURL)
	if err != nil {
		tb.Fatalf("parse persistent login server url: %v", err)
	}
	parsed.Scheme = "ws"
	parsed.Path = clientWSPath

	start := time.Now()
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		tb.Fatalf("dial persistent login websocket: %v", err)
	}

	loginEnvelope := &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: f.userKey.NodeID, UserId: f.userKey.UserID},
				Password:        f.password,
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	}
	data, err := gproto.Marshal(loginEnvelope)
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("marshal persistent login envelope: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(clientWSWriteWait)); err != nil {
		_ = conn.Close()
		tb.Fatalf("set persistent login write deadline: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		_ = conn.Close()
		tb.Fatalf("write persistent login envelope: %v", err)
	}

	loginResponse := readBenchmarkServerEnvelopeOnce(tb, conn, f.timeout).GetLoginResponse()
	loginLatency := time.Since(start)
	if loginResponse == nil ||
		loginResponse.GetUser().GetNodeId() != f.userKey.NodeID ||
		loginResponse.GetUser().GetUserId() != f.userKey.UserID ||
		loginResponse.GetProtocolVersion() != internalproto.ClientProtocolVersion {
		_ = conn.Close()
		tb.Fatalf("unexpected persistent login response: %+v", loginResponse)
	}

	catchupLatency := loginLatency
	for idx, expectedBody := range f.expectedBodies {
		pushed := readBenchmarkServerEnvelopeOnce(tb, conn, f.timeout).GetMessagePushed()
		if pushed == nil || pushed.Message == nil {
			_ = conn.Close()
			tb.Fatalf("expected persistent history message %d", idx)
		}
		message := pushed.Message
		if message.GetRecipient().GetNodeId() != f.userKey.NodeID ||
			message.GetRecipient().GetUserId() != f.userKey.UserID ||
			!bytes.Equal(message.GetBody(), expectedBody) {
			_ = conn.Close()
			tb.Fatalf("unexpected persistent history message %d: %+v", idx, message)
		}
		catchupLatency = time.Since(start)
	}
	benchmarkWriteClientEnvelope(tb, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 1}},
	})
	completionEnvelope := readBenchmarkServerEnvelopeOnce(tb, conn, f.timeout)
	if pong := completionEnvelope.GetPong(); pong == nil || pong.GetRequestId() != 1 {
		_ = conn.Close()
		tb.Fatalf("unexpected envelope after persistent history: %+v", completionEnvelope)
	}

	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(clientWSWriteWait))
	_ = conn.Close()
	waitForAPIBenchmarkCondition(tb, f.timeout, func() bool {
		return !f.http.hasPersistentSessions()
	})
	return loginLatency, catchupLatency
}

func openBenchmarkPersistentDispatchFixture(tb testing.TB, mode benchroot.Mode, nodeCount, onlineUsersTotal int) *benchmarkPersistentDispatchFixture {
	tb.Helper()

	if onlineUsersTotal < 1 {
		tb.Fatalf("persistent dispatch online users must be positive, got %d", onlineUsersTotal)
	}
	ctx := context.Background()
	timeout := benchmarkAPIClientTimeout(mode, 120*time.Second, 120*time.Second)
	scenario := apiBenchmarkEngineScenario{name: store.EngineSQLite, engine: store.EngineSQLite}
	meshCluster := openBenchmarkAuthenticatedLinearMeshAPIClusterWithEventLogLimit(tb, mode, scenario, nodeCount, 32)

	sourceNode := meshCluster.nodes[0]
	targetNode := meshCluster.nodes[len(meshCluster.nodes)-1]
	waitForAPIBenchmarkMeshRoute(tb, sourceNode.nodeID, sourceNode.manager, targetNode.nodeID, mesh.TrafficReplicationStream, timeout)
	waitForAPIBenchmarkMeshRoute(tb, targetNode.nodeID, targetNode.manager, sourceNode.nodeID, mesh.TrafficReplicationStream, timeout)
	waitForAPIBenchmarkMeshRoute(tb, targetNode.nodeID, targetNode.manager, sourceNode.nodeID, mesh.TrafficControlCritical, timeout)

	passwordHash := benchmarkMustHashPassword(tb, "bench-password")
	directUser, _, err := targetNode.service.CreateUser(ctx, store.CreateUserParams{
		Username:     fmt.Sprintf("bench-persistent-direct-%d-%d", nodeCount, onlineUsersTotal),
		PasswordHash: passwordHash,
		Role:         store.RoleUser,
	})
	if err != nil {
		meshCluster.Close()
		tb.Fatalf("create persistent direct recipient: %v", err)
	}
	channelUser, _, err := sourceNode.service.CreateUser(ctx, store.CreateUserParams{
		Username: fmt.Sprintf("bench-persistent-channel-%d-%d", nodeCount, onlineUsersTotal),
		Role:     store.RoleChannel,
	})
	if err != nil {
		meshCluster.Close()
		tb.Fatalf("create persistent benchmark channel: %v", err)
	}
	waitForAPIBenchmarkCondition(tb, timeout, func() bool {
		got, err := sourceNode.service.GetUser(ctx, directUser.Key())
		return err == nil && got.Username == directUser.Username
	})
	usersChunk, err := sourceNode.service.store.BuildSnapshotChunk(ctx, store.SnapshotUsersPartition)
	if err != nil {
		meshCluster.Close()
		tb.Fatalf("build persistent benchmark users snapshot: %v", err)
	}
	for idx := 1; idx < len(meshCluster.nodes); idx++ {
		if err := meshCluster.nodes[idx].service.store.ApplySnapshotChunk(ctx, usersChunk); err != nil {
			meshCluster.Close()
			tb.Fatalf("apply persistent benchmark users snapshot on node %d: %v", meshCluster.nodes[idx].nodeID, err)
		}
	}

	broadcastNode := meshCluster.nodes[1]
	broadcastUsersChunk, err := broadcastNode.service.store.BuildSnapshotChunk(ctx, store.SnapshotUsersPartition)
	if err != nil {
		meshCluster.Close()
		tb.Fatalf("build persistent benchmark broadcast users snapshot: %v", err)
	}
	for idx := range meshCluster.nodes {
		if idx == 1 {
			continue
		}
		if err := meshCluster.nodes[idx].service.store.ApplySnapshotChunk(ctx, broadcastUsersChunk); err != nil {
			meshCluster.Close()
			tb.Fatalf("apply persistent benchmark broadcast users snapshot on node %d: %v", meshCluster.nodes[idx].nodeID, err)
		}
	}

	broadcastKey := store.UserKey{NodeID: broadcastNode.nodeID, UserID: store.BroadcastUserID}
	for _, node := range meshCluster.nodes {
		node := node
		waitForAPIBenchmarkCondition(tb, timeout, func() bool {
			gotDirect, directErr := node.service.GetUser(ctx, directUser.Key())
			gotChannel, channelErr := node.service.GetUser(ctx, channelUser.Key())
			gotBroadcast, broadcastErr := node.service.GetUser(ctx, broadcastKey)
			return directErr == nil && channelErr == nil && broadcastErr == nil &&
				gotDirect.Username == directUser.Username &&
				gotChannel.Role == store.RoleChannel && gotBroadcast.Role == store.RoleBroadcast
		})
	}
	// 客户端按 producer node + seq 去重，而消息序号按 target 分配；不同基线可让一个 sender 连续覆盖三类 target。
	seedMessages := []store.Message{
		{
			Recipient: directUser.Key(),
			NodeID:    sourceNode.nodeID,
			Seq:       persistentDirectSequenceBase,
			Sender:    store.UserKey{NodeID: sourceNode.nodeID, UserID: store.BootstrapAdminUserID},
			Body:      []byte("persistent-direct-sequence-seed"),
			CreatedAt: channelUser.CreatedAt,
		},
		{
			Recipient: broadcastKey,
			NodeID:    sourceNode.nodeID,
			Seq:       persistentBroadcastSequenceBase,
			Sender:    store.UserKey{NodeID: sourceNode.nodeID, UserID: store.BootstrapAdminUserID},
			Body:      []byte("persistent-broadcast-sequence-seed"),
			CreatedAt: channelUser.CreatedAt,
		},
		{
			Recipient: channelUser.Key(),
			NodeID:    sourceNode.nodeID,
			Seq:       persistentChannelSequenceBase,
			Sender:    store.UserKey{NodeID: sourceNode.nodeID, UserID: store.BootstrapAdminUserID},
			Body:      []byte("persistent-channel-sequence-seed"),
			CreatedAt: channelUser.CreatedAt,
		},
	}
	seedBenchmarkPersistentMessageSequences(tb, ctx, meshCluster.nodes, sourceNode, seedMessages)

	backgroundCounts := benchmarkDistributedUserCounts(onlineUsersTotal-1, nodeCount)
	keysByNode := make([][]store.UserKey, nodeCount)
	allKeys := make([]store.UserKey, 0, onlineUsersTotal)
	for idx, count := range backgroundCounts {
		node := meshCluster.nodes[idx]
		keys := seedBenchmarkLocalUsers(tb, ctx, node, count, fmt.Sprintf("bench-persistent-node-%d", idx+1), passwordHash)
		keysByNode[idx] = append(keysByNode[idx], keys...)
		allKeys = append(allKeys, keys...)
	}
	keysByNode[len(keysByNode)-1] = append(keysByNode[len(keysByNode)-1], directUser.Key())
	allKeys = append(allKeys, directUser.Key())

	channelSubscriberCount := onlineUsersTotal / 10
	if channelSubscriberCount == 0 {
		channelSubscriberCount = 1
	}
	channelSubscriberKeys := make(map[store.UserKey]struct{}, channelSubscriberCount)
	for idx := 0; idx < len(allKeys) && len(channelSubscriberKeys) < channelSubscriberCount; idx += 10 {
		key := allKeys[idx]
		channelSubscriberKeys[key] = struct{}{}
		nodeIndex := benchmarkPersistentNodeIndex(meshCluster.nodes, key.NodeID)
		if nodeIndex < 0 {
			meshCluster.Close()
			tb.Fatalf("persistent benchmark node %d was not found", key.NodeID)
		}
		node := meshCluster.nodes[nodeIndex]
		if _, _, err := node.service.store.SubscribeChannel(ctx, store.ChannelSubscriptionParams{
			Subscriber: key,
			Channel:    channelUser.Key(),
		}); err != nil {
			meshCluster.Close()
			tb.Fatalf("subscribe persistent benchmark user %+v: %v", key, err)
		}
	}

	for _, node := range meshCluster.nodes {
		if _, err := node.service.store.PruneEventLogOnce(ctx); err != nil {
			meshCluster.Close()
			tb.Fatalf("prune persistent benchmark event log on node %d: %v", node.nodeID, err)
		}
	}

	clients := make([]*benchmarkPersistentClient, 0, onlineUsersTotal)
	for idx, keys := range keysByNode {
		nodeClients := dialAndLoginBenchmarkPersistentClients(tb, meshCluster.nodes[idx].serverURL, keys, "bench-password", 64)
		clients = append(clients, nodeClients...)
	}
	cleanupClients := func() {
		for _, client := range clients {
			client.Close()
		}
	}

	directClient := benchmarkPersistentClientForKey(clients, directUser.Key())
	if directClient == nil {
		cleanupClients()
		meshCluster.Close()
		tb.Fatalf("persistent direct recipient client was not connected")
	}
	channelClients := make([]*benchmarkPersistentClient, 0, channelSubscriberCount)
	for _, client := range clients {
		if _, ok := channelSubscriberKeys[client.key]; ok {
			channelClients = append(channelClients, client)
		}
	}
	if len(channelClients) != channelSubscriberCount {
		cleanupClients()
		meshCluster.Close()
		tb.Fatalf("unexpected persistent channel client count: got=%d want=%d", len(channelClients), channelSubscriberCount)
	}
	for _, client := range clients {
		expectedSeeds := map[int64][]byte{
			persistentBroadcastSequenceBase: seedMessages[1].Body,
		}
		if client.key == directUser.Key() {
			expectedSeeds[persistentDirectSequenceBase] = seedMessages[0].Body
		}
		for range len(expectedSeeds) {
			delivery := client.NextDelivery(tb, timeout)
			expectedBody, ok := expectedSeeds[delivery.message.GetSeq()]
			if !ok || delivery.message.GetNodeId() != sourceNode.nodeID || !bytes.Equal(delivery.message.GetBody(), expectedBody) {
				cleanupClients()
				meshCluster.Close()
				tb.Fatalf("unexpected persistent sequence seed for %+v: %+v", client.key, delivery.message)
			}
			delete(expectedSeeds, delivery.message.GetSeq())
		}
	}

	expectedOnlineUsersByNode := make([]int, nodeCount)
	for idx := range expectedOnlineUsersByNode {
		expectedOnlineUsersByNode[idx] = len(keysByNode[idx])
	}
	waitForAPIBenchmarkOnlineUsers(tb, timeout, meshCluster.nodes, expectedOnlineUsersByNode)

	senderKey := store.UserKey{NodeID: sourceNode.nodeID, UserID: store.BootstrapAdminUserID}
	sender, err := dialAndLoginBenchmarkPersistentSender(sourceNode.serverURL, senderKey, "root-password")
	if err != nil {
		cleanupClients()
		meshCluster.Close()
		tb.Fatalf("dial persistent benchmark sender: %v", err)
	}
	time.Sleep(clientWSPollInterval + 200*time.Millisecond)
	return &benchmarkPersistentDispatchFixture{
		cluster:          meshCluster,
		broadcastKey:     broadcastKey,
		channelKey:       channelUser.Key(),
		directRecipient:  directClient,
		clients:          clients,
		channelClients:   channelClients,
		sender:           sender,
		onlineUsersTotal: onlineUsersTotal,
		nodeCount:        nodeCount,
		timeout:          timeout,
		nextRequestID:    1,
	}
}

func seedBenchmarkPersistentMessageSequences(tb testing.TB, ctx context.Context, nodes []benchmarkLinearMeshAPINode, source benchmarkLinearMeshAPINode, messages []store.Message) {
	tb.Helper()

	chunk, err := source.service.store.BuildSnapshotChunk(ctx, store.MessageSnapshotPartition(source.nodeID))
	if err != nil {
		tb.Fatalf("build persistent message sequence seed chunk: %v", err)
	}
	chunk.Rows = make([]*internalproto.SnapshotRow, 0, len(messages))
	for _, message := range messages {
		chunk.Rows = append(chunk.Rows, &internalproto.SnapshotRow{
			Body: &internalproto.SnapshotRow_Message{
				Message: &internalproto.SnapshotMessageRow{
					Recipient:    &internalproto.ClusterUserRef{NodeId: message.Recipient.NodeID, UserId: message.Recipient.UserID},
					NodeId:       message.NodeID,
					Seq:          message.Seq,
					Sender:       &internalproto.ClusterUserRef{NodeId: message.Sender.NodeID, UserId: message.Sender.UserID},
					Body:         message.Body,
					CreatedAtHlc: message.CreatedAt.String(),
				},
			},
		})
	}
	for _, node := range nodes {
		if err := node.service.store.ApplySnapshotChunk(ctx, chunk); err != nil {
			tb.Fatalf("apply persistent message sequence seed chunk on node %d: %v", node.nodeID, err)
		}
	}
}

func benchmarkPersistentNodeIndex(nodes []benchmarkLinearMeshAPINode, nodeID int64) int {
	for idx, node := range nodes {
		if node.nodeID == nodeID {
			return idx
		}
	}
	return -1
}

func benchmarkPersistentClientForKey(clients []*benchmarkPersistentClient, key store.UserKey) *benchmarkPersistentClient {
	for _, client := range clients {
		if client.key == key {
			return client
		}
	}
	return nil
}

func (f *benchmarkPersistentDispatchFixture) Close() {
	if f.sender != nil {
		f.sender.Close()
	}
	for _, client := range f.clients {
		client.Close()
	}
	if f.cluster != nil {
		f.cluster.Close()
	}
}

func (f *benchmarkPersistentDispatchFixture) Scenarios() []benchmarkPersistentDispatchScenario {
	return []benchmarkPersistentDispatchScenario{
		{name: "direct", target: f.directRecipient.key, recipients: []*benchmarkPersistentClient{f.directRecipient}},
		{name: "broadcast", target: f.broadcastKey, recipients: f.clients},
		{name: "channel-10pct", target: f.channelKey, recipients: f.channelClients},
	}
}

func (f *benchmarkPersistentDispatchFixture) RunScenario(b *testing.B, scenario benchmarkPersistentDispatchScenario) {
	payloadPrefix := []byte("persistent-" + scenario.name + "-")
	payload := append(payloadPrefix, bytes.Repeat([]byte("p"), persistentClientBenchmarkPayloadSize-len(payloadPrefix))...)

	requestID := f.takeRequestID()
	_, _, _ = f.sendOnce(b, f.sender, scenario, payload, requestID)
	b.SetBytes(int64(len(payload) * len(scenario.recipients)))
	b.ReportMetric(float64(len(scenario.recipients)), "delivered/op")
	b.ReportMetric(float64(f.onlineUsersTotal), "online_users")
	b.ReportMetric(float64(f.onlineUsersTotal)/float64(f.nodeCount), "online_users_per_node")
	b.ResetTimer()

	var totalWrite time.Duration
	var totalFirstPush time.Duration
	var totalLastPush time.Duration
	for i := 0; i < b.N; i++ {
		requestID = f.takeRequestID()
		writeLatency, firstPushLatency, lastPushLatency := f.sendOnce(b, f.sender, scenario, payload, requestID)
		totalWrite += writeLatency
		totalFirstPush += firstPushLatency
		totalLastPush += lastPushLatency
	}

	b.StopTimer()
	reportAPIBenchmarkAverageLatencyMetric(b, totalWrite, "write_ms/op")
	reportAPIBenchmarkAverageLatencyMetric(b, totalFirstPush, "first_push_ms/op")
	reportAPIBenchmarkAverageLatencyMetric(b, totalLastPush, "last_push_ms/op")
}

func (f *benchmarkPersistentDispatchFixture) takeRequestID() uint64 {
	requestID := f.nextRequestID
	f.nextRequestID++
	return requestID
}

func (f *benchmarkPersistentDispatchFixture) sendOnce(tb testing.TB, sender *benchmarkPersistentSender, scenario benchmarkPersistentDispatchScenario, payload []byte, requestID uint64) (time.Duration, time.Duration, time.Duration) {
	tb.Helper()

	start := time.Now()
	benchmarkWriteClientEnvelope(tb, sender.conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:    requestID,
				Target:       &internalproto.UserRef{NodeId: scenario.target.NodeID, UserId: scenario.target.UserID},
				Body:         payload,
				DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_PERSISTENT,
			},
		},
	})

	responseResult := sender.NextResponse(tb, f.timeout)
	responseEnvelope := responseResult.envelope
	responseReceivedAt := responseResult.receivedAt
	response := responseEnvelope.GetSendMessageResponse()
	if response == nil || response.GetRequestId() != requestID || response.GetMessage() == nil {
		tb.Fatalf("unexpected persistent send response: %+v", responseEnvelope)
	}
	message := response.GetMessage()
	if message.GetRecipient().GetNodeId() != scenario.target.NodeID ||
		message.GetRecipient().GetUserId() != scenario.target.UserID ||
		!bytes.Equal(message.GetBody(), payload) {
		tb.Fatalf("unexpected persistent send response message: %+v", message)
	}

	var firstReceivedAt time.Time
	var lastReceivedAt time.Time
	for _, client := range scenario.recipients {
		delivery := client.NextDelivery(tb, f.timeout)
		if delivery.message.GetNodeId() != message.GetNodeId() ||
			delivery.message.GetSeq() != message.GetSeq() ||
			delivery.message.GetRecipient().GetNodeId() != scenario.target.NodeID ||
			delivery.message.GetRecipient().GetUserId() != scenario.target.UserID ||
			!bytes.Equal(delivery.message.GetBody(), payload) {
			tb.Fatalf("unexpected persistent delivery for %+v: %+v", client.key, delivery.message)
		}
		if firstReceivedAt.IsZero() || delivery.receivedAt.Before(firstReceivedAt) {
			firstReceivedAt = delivery.receivedAt
		}
		if delivery.receivedAt.After(lastReceivedAt) {
			lastReceivedAt = delivery.receivedAt
		}
	}

	return responseReceivedAt.Sub(start), firstReceivedAt.Sub(start), lastReceivedAt.Sub(start)
}

func dialAndLoginBenchmarkPersistentSender(serverURL string, key store.UserKey, password string) (*benchmarkPersistentSender, error) {
	conn, err := dialAndLoginBenchmarkIdleClientWebSocketConnWithOptions(serverURL, key, password, false)
	if err != nil {
		return nil, err
	}
	sender := &benchmarkPersistentSender{
		conn:      conn,
		responses: make(chan benchmarkPersistentResponse, 1),
		errs:      make(chan error, 1),
		stop:      make(chan struct{}),
	}
	sender.wg.Add(1)
	go sender.readLoop()
	return sender, nil
}

func (s *benchmarkPersistentSender) readLoop() {
	defer s.wg.Done()

	for {
		messageType, data, err := s.conn.ReadMessage()
		if err != nil {
			s.reportError(err)
			return
		}
		if messageType != websocket.BinaryMessage {
			s.reportError(fmt.Errorf("unexpected persistent sender websocket message type: %d", messageType))
			return
		}
		var envelope internalproto.ServerEnvelope
		if err := gproto.Unmarshal(data, &envelope); err != nil {
			s.reportError(fmt.Errorf("unmarshal persistent sender envelope: %w", err))
			return
		}
		if envelope.GetMessagePushed() != nil {
			continue
		}
		if envelope.GetSendMessageResponse() == nil {
			s.reportError(fmt.Errorf("unexpected persistent sender envelope: %T", envelope.GetBody()))
			return
		}
		select {
		case s.responses <- benchmarkPersistentResponse{envelope: &envelope, receivedAt: time.Now()}:
		case <-s.stop:
			return
		}
	}
}

func (s *benchmarkPersistentSender) reportError(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

func (s *benchmarkPersistentSender) NextResponse(tb testing.TB, timeout time.Duration) benchmarkPersistentResponse {
	tb.Helper()

	select {
	case response := <-s.responses:
		return response
	case err := <-s.errs:
		tb.Fatalf("read persistent sender response: %v", err)
	case <-time.After(timeout):
		tb.Fatalf("timed out waiting for persistent sender response")
	}
	return benchmarkPersistentResponse{}
}

func (s *benchmarkPersistentSender) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		close(s.stop)
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
	s.wg.Wait()
}

func dialAndLoginBenchmarkPersistentClients(tb testing.TB, serverURL string, keys []store.UserKey, password string, parallelism int) []*benchmarkPersistentClient {
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

	clients := make([]*benchmarkPersistentClient, len(keys))
	indexes := make(chan int, len(keys))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indexes {
				conn, err := dialAndLoginBenchmarkIdleClientWebSocketConnWithOptions(serverURL, keys[idx], password, false)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				client := &benchmarkPersistentClient{
					key:        keys[idx],
					conn:       conn,
					deliveries: make(chan benchmarkPersistentDelivery, 1),
					errs:       make(chan error, 1),
					stop:       make(chan struct{}),
				}
				clients[idx] = client
				client.wg.Add(2)
				go client.readLoop()
				go client.keepAliveLoop()
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
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
		tb.Fatalf("dial persistent client websockets: %v", err)
	default:
	}
	return clients
}

func (c *benchmarkPersistentClient) readLoop() {
	defer c.wg.Done()

	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			c.reportError(err)
			return
		}
		if messageType != websocket.BinaryMessage {
			c.reportError(fmt.Errorf("unexpected persistent websocket message type: %d", messageType))
			return
		}
		var envelope internalproto.ServerEnvelope
		if err := gproto.Unmarshal(data, &envelope); err != nil {
			c.reportError(fmt.Errorf("unmarshal persistent server envelope: %w", err))
			return
		}
		if envelope.GetPong() != nil {
			continue
		}
		pushed := envelope.GetMessagePushed()
		if pushed == nil || pushed.Message == nil {
			c.reportError(fmt.Errorf("unexpected persistent server envelope: %T", envelope.GetBody()))
			return
		}
		select {
		case c.deliveries <- benchmarkPersistentDelivery{message: pushed.Message, receivedAt: time.Now()}:
		case <-c.stop:
			return
		}
	}
}

func (c *benchmarkPersistentClient) keepAliveLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	data, err := gproto.Marshal(&internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 1}},
	})
	if err != nil {
		c.reportError(fmt.Errorf("marshal persistent benchmark ping: %w", err))
		return
	}
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(clientWSWriteWait)); err != nil {
				c.reportError(fmt.Errorf("set persistent benchmark ping deadline: %w", err))
				return
			}
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				c.reportError(fmt.Errorf("write persistent benchmark ping: %w", err))
				return
			}
		}
	}
}

func (c *benchmarkPersistentClient) reportError(err error) {
	select {
	case c.errs <- err:
	default:
	}
}

func (c *benchmarkPersistentClient) NextDelivery(tb testing.TB, timeout time.Duration) benchmarkPersistentDelivery {
	tb.Helper()

	select {
	case delivery := <-c.deliveries:
		return delivery
	case err := <-c.errs:
		tb.Fatalf("read persistent delivery for %+v: %v", c.key, err)
	case <-time.After(timeout):
		tb.Fatalf("timed out waiting for persistent delivery for %+v", c.key)
	}
	return benchmarkPersistentDelivery{}
}

func (c *benchmarkPersistentClient) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		close(c.stop)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
	c.wg.Wait()
}
