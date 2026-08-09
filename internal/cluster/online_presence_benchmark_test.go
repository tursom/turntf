package cluster

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tursom/turntf/internal/app"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const onlinePresenceBenchmarkUserCount = 10_000

var (
	benchmarkOnlinePresenceUpdateSink *internalproto.OnlinePresenceUpdate
	benchmarkLegacyPresenceSink       *benchmarkLegacyOnlinePresenceSnapshot
)

type benchmarkLegacyOnlinePresenceSnapshot struct {
	originNodeID  int64
	items         []*internalproto.ClusterOnlineNodePresence
	loggedInUsers []*internalproto.ClusterLoggedInUser
}

type benchmarkLegacyPresenceMirror struct {
	presenceByUser      map[store.UserKey]map[int64]store.OnlineNodePresence
	loggedInUsersByNode map[int64][]app.LoggedInUserSummary
}

func BenchmarkOnlinePresenceSync10K(b *testing.B) {
	sender, users, usersByShard := newOnlinePresenceBenchmarkSender(b)

	b.Run("sender", func(b *testing.B) {
		b.Run("legacy-full", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sender.mu.Lock()
				benchmarkLegacyPresenceSink = buildBenchmarkLegacyOnlinePresenceSnapshotLocked(sender)
				sender.mu.Unlock()
			}
		})
		b.Run("delta-one-user", func(b *testing.B) {
			oneUser := users[:1]
			shard := onlinePresenceShardIndex(oneUser[0])
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sender.mu.Lock()
				benchmarkOnlinePresenceUpdateSink = sender.buildOnlinePresenceUpdateLocked(
					internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
					shard,
					oneUser,
				)
				sender.mu.Unlock()
			}
		})
		b.Run("authoritative-shard", func(b *testing.B) {
			shard := largestOnlinePresenceBenchmarkShard(usersByShard)
			shardUsers := usersByShard[shard]
			b.ReportMetric(float64(len(shardUsers)), "users/op")
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkOnlinePresenceUpdateSink = sender.takeAuthoritativePresenceShard(shard)
			}
		})
	})

	b.Run("receiver", func(b *testing.B) {
		legacy := buildBenchmarkLegacyOnlinePresenceSnapshot(sender)
		b.Run("legacy-full", func(b *testing.B) {
			mirror := &benchmarkLegacyPresenceMirror{
				presenceByUser:      make(map[store.UserKey]map[int64]store.OnlineNodePresence),
				loggedInUsersByNode: make(map[int64][]app.LoggedInUserSummary),
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				mirror.apply(legacy)
			}
		})
		b.Run("delta-one-user", func(b *testing.B) {
			receiver := newOnlinePresenceUpdateTestManager(b)
			update := benchmarkPresenceUpdateForUsers(
				internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
				[]store.UserKey{users[0]},
			)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				update.Generation = uint64(i + 1)
				if applied, err := receiver.applyOnlinePresenceUpdate(update); err != nil || !applied {
					b.Fatalf("apply one-user delta: applied=%v err=%v", applied, err)
				}
			}
		})
		b.Run("authoritative-shard", func(b *testing.B) {
			receiver := newOnlinePresenceUpdateTestManager(b)
			shard := largestOnlinePresenceBenchmarkShard(usersByShard)
			shardUsers := usersByShard[shard]
			update := benchmarkPresenceUpdateForUsers(
				internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD,
				shardUsers,
			)
			b.ReportMetric(float64(len(shardUsers)), "users/op")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				update.Generation = uint64(i + 1)
				if applied, err := receiver.applyOnlinePresenceUpdate(update); err != nil || !applied {
					b.Fatalf("apply authoritative shard: applied=%v err=%v", applied, err)
				}
			}
		})
	})
}

func newOnlinePresenceBenchmarkSender(tb testing.TB) (*Manager, []store.UserKey, [onlinePresenceShardCount][]store.UserKey) {
	tb.Helper()
	mgr := newOnlinePresenceUpdateTestManager(tb)
	users := make([]store.UserKey, 0, onlinePresenceBenchmarkUserCount)
	var usersByShard [onlinePresenceShardCount][]store.UserKey
	for userID := int64(1); userID <= onlinePresenceBenchmarkUserCount; userID++ {
		user := store.UserKey{NodeID: mgr.cfg.NodeID, UserID: userID}
		session := store.OnlineSession{
			User:       user,
			SessionRef: store.SessionRef{ServingNodeID: mgr.cfg.NodeID, SessionID: fmt.Sprintf("session-%d", userID)},
			Transport:  "ws",
		}
		mgr.localOnlineSessions[user] = map[string]store.OnlineSession{session.SessionRef.SessionID: session}
		mgr.localLoggedInUsers[user] = app.LoggedInUserSummary{
			NodeID: user.NodeID, UserID: user.UserID, Username: fmt.Sprintf("user-%d", userID),
		}
		shard := onlinePresenceShardIndex(user)
		mgr.localPresenceUsersByShard[shard][user] = struct{}{}
		users = append(users, user)
		usersByShard[shard] = append(usersByShard[shard], user)
	}
	return mgr, users, usersByShard
}

func largestOnlinePresenceBenchmarkShard(usersByShard [onlinePresenceShardCount][]store.UserKey) int {
	largest := 0
	for shard := 1; shard < onlinePresenceShardCount; shard++ {
		if len(usersByShard[shard]) > len(usersByShard[largest]) {
			largest = shard
		}
	}
	return largest
}

func buildBenchmarkLegacyOnlinePresenceSnapshot(mgr *Manager) *benchmarkLegacyOnlinePresenceSnapshot {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return buildBenchmarkLegacyOnlinePresenceSnapshotLocked(mgr)
}

func buildBenchmarkLegacyOnlinePresenceSnapshotLocked(mgr *Manager) *benchmarkLegacyOnlinePresenceSnapshot {
	items := make([]*internalproto.ClusterOnlineNodePresence, 0, len(mgr.localOnlineSessions))
	users := make([]app.LoggedInUserSummary, 0, len(mgr.localLoggedInUsers))
	for user, bucket := range mgr.localOnlineSessions {
		items = append(items, &internalproto.ClusterOnlineNodePresence{
			User:          clusterUserRef(user),
			ServingNodeId: mgr.cfg.NodeID,
			SessionCount:  int32(len(bucket)),
			TransportHint: localTransportHint(bucket),
		})
		users = append(users, mgr.localLoggedInUsers[user])
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].GetUser().GetNodeId() != items[j].GetUser().GetNodeId() {
			return items[i].GetUser().GetNodeId() < items[j].GetUser().GetNodeId()
		}
		return items[i].GetUser().GetUserId() < items[j].GetUser().GetUserId()
	})
	return &benchmarkLegacyOnlinePresenceSnapshot{
		originNodeID:  mgr.cfg.NodeID,
		items:         items,
		loggedInUsers: clusterLoggedInUsers(users),
	}
}

func benchmarkPresenceUpdateForUsers(mode internalproto.OnlinePresenceUpdateMode, users []store.UserKey) *internalproto.OnlinePresenceUpdate {
	update := &internalproto.OnlinePresenceUpdate{
		OriginNodeId:  testNodeID(2),
		RuntimeEpoch:  100,
		Mode:          mode,
		ShardIndex:    uint32(onlinePresenceShardIndex(users[0])),
		ShardCount:    onlinePresenceShardCount,
		Generation:    1,
		Items:         make([]*internalproto.ClusterOnlineNodePresence, 0, len(users)),
		LoggedInUsers: make([]*internalproto.ClusterLoggedInUser, 0, len(users)),
	}
	for _, user := range users {
		update.Items = append(update.Items, &internalproto.ClusterOnlineNodePresence{
			User: clusterUserRef(user), ServingNodeId: testNodeID(2), SessionCount: 1, TransportHint: "ws",
		})
		update.LoggedInUsers = append(update.LoggedInUsers, &internalproto.ClusterLoggedInUser{
			NodeId: user.NodeID, UserId: user.UserID, Username: fmt.Sprintf("user-%d", user.UserID),
		})
	}
	return update
}

func (mirror *benchmarkLegacyPresenceMirror) apply(snapshot *benchmarkLegacyOnlinePresenceSnapshot) {
	clearPresenceForNodeLocked(mirror.presenceByUser, snapshot.originNodeID)
	delete(mirror.loggedInUsersByNode, snapshot.originNodeID)
	for _, item := range snapshot.items {
		user := store.UserKey{NodeID: item.GetUser().GetNodeId(), UserID: item.GetUser().GetUserId()}
		mirror.presenceByUser[user] = map[int64]store.OnlineNodePresence{
			snapshot.originNodeID: {
				User: user, ServingNodeID: snapshot.originNodeID, SessionCount: item.GetSessionCount(), TransportHint: item.GetTransportHint(),
			},
		}
	}
	mirror.loggedInUsersByNode[snapshot.originNodeID] = loggedInUsersFromCluster(snapshot.loggedInUsers)
}
