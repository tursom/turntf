package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

func BenchmarkMeshSnapshotRepairPebbleLinear3Nodes(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, tc := range []struct {
				name      string
				userCount int
				msgCount  int
			}{
				{name: "users/100", userCount: 100},
				{name: "messages/500", msgCount: 500},
			} {
				b.Run(tc.name, func(b *testing.B) {
					benchmarkMeshSnapshotRepairPebbleLinear(b, mode, tc.userCount, tc.msgCount)
				})
			}
		})
	}
}

func BenchmarkMeshTruncatedCatchupRepairPebble(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, tc := range []struct {
				name             string
				retain           int
				generateMessages int
			}{
				{name: "retain-2/generate-32", retain: 2, generateMessages: 32},
				{name: "retain-8/generate-256", retain: 8, generateMessages: 256},
			} {
				b.Run(tc.name, func(b *testing.B) {
					benchmarkMeshTruncatedCatchupRepairPebble(b, mode, tc.retain, tc.generateMessages)
				})
			}
		})
	}
}

func benchmarkMeshSnapshotRepairPebbleLinear(b *testing.B, mode benchroot.Mode, userCount, messageCount int) {
	snapshotTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)
	ctx := context.Background()
	silenceClusterLogs(b)

	b.StopTimer()
	warmupUsers, warmupMessages := benchmarkSnapshotWarmupWorkload(userCount, messageCount)
	runClusterBenchmarkWarmup(b, func() {
		_ = runBenchmarkMeshSnapshotRepairOnce(b, ctx, mode, warmupUsers, warmupMessages, snapshotTimeout, false)
	})
	var totalSnapshot time.Duration
	for i := 0; i < b.N; i++ {
		totalSnapshot += runBenchmarkMeshSnapshotRepairOnce(b, ctx, mode, userCount, messageCount, snapshotTimeout, true)
	}

	reportAverageLatencyMetric(b, totalSnapshot, "snapshot_ms/op")
}

func benchmarkMeshTruncatedCatchupRepairPebble(b *testing.B, mode benchroot.Mode, retain, generateMessages int) {
	repairTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)
	ctx := context.Background()
	silenceClusterLogs(b)

	b.StopTimer()
	runClusterBenchmarkWarmup(b, func() {
		_ = runBenchmarkMeshTruncatedCatchupRepairOnce(b, ctx, mode, retain, benchmarkTruncatedCatchupWarmupMessages(generateMessages), repairTimeout, false)
	})
	var totalRepair time.Duration
	for i := 0; i < b.N; i++ {
		totalRepair += runBenchmarkMeshTruncatedCatchupRepairOnce(b, ctx, mode, retain, generateMessages, repairTimeout, true)
	}

	reportAverageLatencyMetric(b, totalRepair, "truncated_repair_ms/op")
}

func seedSnapshotUsersForBenchmark(tb testing.TB, st *store.Store, count int) []store.UserKey {
	tb.Helper()

	ctx := context.Background()
	keys := make([]store.UserKey, 0, count)
	for i := 0; i < count; i++ {
		user, _, err := st.CreateUser(ctx, store.CreateUserParams{
			Username:     fmt.Sprintf("snapshot-user-%03d", i),
			PasswordHash: "snapshot-hash",
		})
		if err != nil {
			tb.Fatalf("create snapshot user %d: %v", i, err)
		}
		keys = append(keys, user.Key())
	}
	return keys
}

func seedSnapshotMessagesForBenchmark(tb testing.TB, st *store.Store, count int) (store.User, []byte) {
	tb.Helper()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, store.CreateUserParams{
		Username:     "snapshot-message-user",
		PasswordHash: "snapshot-hash",
	})
	if err != nil {
		tb.Fatalf("create snapshot message user: %v", err)
	}

	var latestBody []byte
	for i := 0; i < count; i++ {
		body := []byte(fmt.Sprintf("snapshot-message-%03d", i))
		if _, _, err := st.CreateMessage(ctx, store.CreateMessageParams{
			UserKey: user.Key(),
			Sender:  user.Key(),
			Body:    body,
		}); err != nil {
			tb.Fatalf("create snapshot message %d: %v", i, err)
		}
		latestBody = body
	}
	return user, latestBody
}

func benchmarkSnapshotRepairConverged(ctx context.Context, st *store.Store, userKeys []store.UserKey, messageCount int, latestBody []byte) bool {
	for _, key := range userKeys {
		if _, err := st.GetUser(ctx, key); err != nil {
			return false
		}
	}
	if messageCount == 0 {
		return true
	}
	messages, err := st.ListMessagesByUser(ctx, userKeys[len(userKeys)-1], messageCount)
	return err == nil &&
		len(messages) == messageCount &&
		string(messages[0].Body) == string(latestBody)
}

func seedRetainedMessagesForBenchmark(tb testing.TB, st *store.Store, count int) (store.User, []byte) {
	tb.Helper()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, store.CreateUserParams{
		Username:     "truncated-user",
		PasswordHash: "truncated-hash",
	})
	if err != nil {
		tb.Fatalf("create truncated benchmark user: %v", err)
	}

	var latestBody []byte
	for i := 0; i < count; i++ {
		body := []byte(fmt.Sprintf("truncated-message-%03d", i))
		if _, _, err := st.CreateMessage(ctx, store.CreateMessageParams{
			UserKey: user.Key(),
			Sender:  clusterSenderKey(9, 1),
			Body:    body,
		}); err != nil {
			tb.Fatalf("create truncated benchmark message %d: %v", i, err)
		}
		latestBody = body
	}
	return user, latestBody
}

func handleSnapshotRepairRequests(tb testing.TB, source *store.Store, targetMgr *Manager, sess *session, expected map[string]struct{}, requestTimeout time.Duration) {
	tb.Helper()

	for len(expected) > 0 {
		select {
		case envelope := <-sess.send:
			chunk := envelope.GetSnapshotChunk()
			if chunk == nil || !chunk.Request {
				continue
			}
			if _, ok := expected[chunk.Partition]; !ok {
				tb.Fatalf("unexpected snapshot request for partition %q", chunk.Partition)
			}
			delete(expected, chunk.Partition)

			response, err := source.BuildSnapshotChunk(context.Background(), chunk.Partition)
			if err != nil {
				tb.Fatalf("build snapshot chunk %q: %v", chunk.Partition, err)
			}
			response.SnapshotVersion = internalproto.SnapshotVersion
			if err := targetMgr.handleSnapshotChunk(sess, &internalproto.Envelope{
				NodeId: testNodeID(1),
				Body: &internalproto.Envelope_SnapshotChunk{
					SnapshotChunk: response,
				},
			}); err != nil {
				tb.Fatalf("apply snapshot chunk %q: %v", chunk.Partition, err)
			}
		case <-time.After(requestTimeout):
			tb.Fatalf("timed out waiting for snapshot repair requests; remaining=%d", len(expected))
		}
	}
}

func benchmarkSnapshotWarmupWorkload(userCount, messageCount int) (int, int) {
	if userCount > 0 {
		return min(userCount, 16), 0
	}
	return 0, min(messageCount, 32)
}

func benchmarkTruncatedCatchupWarmupMessages(generateMessages int) int {
	return min(generateMessages, 16)
}

func runBenchmarkMeshSnapshotRepairOnce(tb *testing.B, ctx context.Context, mode benchroot.Mode, userCount, messageCount int, snapshotTimeout time.Duration, measure bool) time.Duration {
	tb.Helper()

	cluster := openBenchmarkPebbleLinearCluster(tb, mode, 3, store.DefaultMessageWindowSize, store.DefaultEventLogMaxEventsPerOrigin)
	source := cluster.managers[0]
	target := cluster.managers[2]
	targetNodeID := testNodeID(3)

	waitForMeshRoute(tb, source, targetNodeID, mesh.TrafficSnapshotBulk)
	waitForMeshRoute(tb, target, testNodeID(1), mesh.TrafficSnapshotBulk)

	var userKeys []store.UserKey
	var latestBody []byte
	if userCount > 0 {
		userKeys = seedSnapshotUsersForBenchmark(tb, source.store, userCount)
	}
	if messageCount > 0 {
		var messageUser store.User
		messageUser, latestBody = seedSnapshotMessagesForBenchmark(tb, source.store, messageCount)
		userKeys = append(userKeys, messageUser.Key())
	}

	if measure {
		tb.StartTimer()
	}
	start := time.Now()

	envelope, err := source.buildSnapshotDigestEnvelope()
	if err != nil {
		cluster.Close()
		tb.Fatalf("build snapshot digest: %v", err)
	}
	if err := source.routeMeshSnapshotManifest(ctx, targetNodeID, envelope.GetSnapshotDigest()); err != nil {
		cluster.Close()
		tb.Fatalf("route snapshot manifest: %v", err)
	}

	waitForBenchmark(tb, snapshotTimeout, func() bool {
		return benchmarkSnapshotRepairConverged(ctx, target.store, userKeys, messageCount, latestBody)
	})

	elapsed := time.Since(start)
	if measure {
		tb.StopTimer()
	}
	cluster.Close()
	return elapsed
}

func runBenchmarkMeshTruncatedCatchupRepairOnce(tb *testing.B, ctx context.Context, mode benchroot.Mode, retain, generateMessages int, repairTimeout time.Duration, measure bool) time.Duration {
	tb.Helper()

	sourceStore, closeSource := openBenchmarkClusterStore(tb, mode, "source", 1, store.DefaultMessageWindowSize, retain)
	targetStore, closeTarget := openBenchmarkClusterStore(tb, mode, "target", 2, store.DefaultMessageWindowSize, store.DefaultEventLogMaxEventsPerOrigin)
	targetMgr, closeManager := openBenchmarkReplicationManager(tb, 2, targetStore, store.DefaultMessageWindowSize)

	user, latestBody := seedRetainedMessagesForBenchmark(tb, sourceStore, generateMessages)
	if _, err := sourceStore.PruneEventLogOnce(ctx); err != nil {
		closeManager()
		closeTarget()
		closeSource()
		tb.Fatalf("prune source event log: %v", err)
	}
	truncatedBefore, err := sourceStore.EventLogTruncatedBefore(ctx, sourceStore.NodeID())
	if err != nil {
		closeManager()
		closeTarget()
		closeSource()
		tb.Fatalf("read truncated boundary: %v", err)
	}
	if truncatedBefore == 0 {
		closeManager()
		closeTarget()
		closeSource()
		tb.Fatalf("expected non-zero truncation boundary")
	}

	sess := readySnapshotTestSession(targetMgr, testNodeID(1), store.DefaultMessageWindowSize)
	requestID, ok := sess.beginPendingPull(testNodeID(1), 0)
	if !ok {
		closeManager()
		closeTarget()
		closeSource()
		tb.Fatalf("expected pending pull to start")
	}
	envelope := &internalproto.Envelope{
		NodeId: testNodeID(1),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				PullRequestId:          requestID,
				OriginNodeId:           testNodeID(1),
				TruncatedBeforeEventId: uint64(truncatedBefore),
			},
		},
	}

	if measure {
		tb.StartTimer()
	}
	start := time.Now()

	if err := targetMgr.handleEventBatch(sess, envelope); err != nil {
		closeManager()
		closeTarget()
		closeSource()
		tb.Fatalf("handle truncated event batch: %v", err)
	}
	if sess.hasPendingPull(testNodeID(1)) {
		closeManager()
		closeTarget()
		closeSource()
		tb.Fatalf("expected pending pull to be cleared")
	}

	expectedPartitions := map[string]struct{}{
		store.SnapshotUsersPartition:                  {},
		store.SnapshotSubscriptionsPartition:          {},
		store.SnapshotBlacklistsPartition:             {},
		store.MessageSnapshotPartition(testNodeID(1)): {},
	}
	handleSnapshotRepairRequests(tb, sourceStore, targetMgr, sess, expectedPartitions, repairTimeout)

	waitForBenchmark(tb, repairTimeout, func() bool {
		if cursor, err := targetStore.GetOriginCursor(ctx, testNodeID(1)); err != nil || cursor.AppliedEventID != truncatedBefore {
			return false
		}
		messages, err := targetStore.ListMessagesByUser(ctx, user.Key(), generateMessages)
		return err == nil &&
			len(messages) == generateMessages &&
			string(messages[0].Body) == string(latestBody)
	})

	elapsed := time.Since(start)
	if measure {
		tb.StopTimer()
	}
	closeManager()
	closeTarget()
	closeSource()
	return elapsed
}

type benchmarkLinearCluster struct {
	managers []*Manager
	closeFns []func()
}

func openBenchmarkPebbleLinearCluster(tb testing.TB, mode benchroot.Mode, nodeCount int, messageWindowSize int, maxEventsPerOrigin int) *benchmarkLinearCluster {
	tb.Helper()

	cluster := &benchmarkLinearCluster{
		managers: make([]*Manager, 0, nodeCount),
	}
	serverURLs := make([]string, 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeSlot := uint16(idx + 1)
		st, closeStore := openBenchmarkClusterStore(tb, mode, fmt.Sprintf("bench-node-%d", idx+1), nodeSlot, messageWindowSize, maxEventsPerOrigin)
		cluster.closeFns = append(cluster.closeFns, closeStore)

		cfg := mixedTransportBaseConfig(nodeSlot)
		cfg.MessageWindowSize = messageWindowSize
		if idx > 0 {
			cfg.Peers = []Peer{{URL: websocketURL(serverURLs[idx-1]) + websocketPath}}
		}

		mgr, serverURL, closeManager := openBenchmarkMixedTransportManager(tb, cfg, st)
		cluster.closeFns = append(cluster.closeFns, closeManager)
		cluster.managers = append(cluster.managers, mgr)
		serverURLs = append(serverURLs, serverURL)
	}
	return cluster
}

func (c *benchmarkLinearCluster) Close() {
	for idx := len(c.closeFns) - 1; idx >= 0; idx-- {
		c.closeFns[idx]()
	}
}

func openBenchmarkClusterStore(tb testing.TB, mode benchroot.Mode, nodeID string, slot uint16, messageWindowSize int, maxEventsPerOrigin int) (*store.Store, func()) {
	tb.Helper()

	dir, cleanupDir := mode.MkdirTemp(tb, "turntf-cluster-bench-*")
	dbPath := filepath.Join(dir, nodeID+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:                     testNodeID(slot),
		Engine:                     store.EnginePebble,
		PebblePath:                 filepath.Join(dir, nodeID+".pebble"),
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	})
	if err != nil {
		cleanupDir()
		tb.Fatalf("open benchmark cluster store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("init benchmark cluster store: %v", err)
	}
	return st, func() {
		_ = st.Close()
		cleanupDir()
	}
}

func openBenchmarkMixedTransportManager(tb testing.TB, cfg Config, st *store.Store) (*Manager, string, func()) {
	tb.Helper()

	mgr, err := NewManager(cfg, st)
	if err != nil {
		tb.Fatalf("new benchmark manager: %v", err)
	}
	server := &httptest.Server{
		Listener: mustListen(tb),
		Config:   &http.Server{Handler: mgr.Handler()},
	}
	server.Start()
	if err := mgr.Start(context.Background()); err != nil {
		server.Close()
		tb.Fatalf("start benchmark manager: %v", err)
	}
	return mgr, server.URL, func() {
		_ = mgr.Close()
		server.Close()
	}
}

func openBenchmarkReplicationManager(tb testing.TB, nodeSlot uint16, st *store.Store, messageWindowSize int) (*Manager, func()) {
	tb.Helper()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(nodeSlot),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: messageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}, st)
	if err != nil {
		tb.Fatalf("new benchmark replication manager: %v", err)
	}
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	return mgr, func() {
		if mgr.cancel != nil {
			mgr.cancel()
		}
		_ = mgr.Close()
	}
}
