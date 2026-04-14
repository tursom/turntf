package cluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/store"
)

func TestSimulationNetworkPartitionRecoveryConverges(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	seedUser, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "partition-seed",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create seed user: %v", err)
	}
	scenario.WaitForUserVisible(10*time.Second, "node-b", seedUser.Key())

	scenario.Partition("node-a", "node-b")
	scenario.WaitForPeersInactive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	userA, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "written-on-a-during-partition",
		PasswordHash: "hash-a",
	})
	if err != nil {
		t.Fatalf("create user on node A during partition: %v", err)
	}
	if _, _, err := nodeA.service.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: userA.Key(),
		Sender:  clusterSenderKey(1, store.BootstrapAdminUserID),
		Body:    []byte("message-from-a"),
	}); err != nil {
		t.Fatalf("create message on node A during partition: %v", err)
	}

	userB, _, err := nodeB.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "written-on-b-during-partition",
		PasswordHash: "hash-b",
	})
	if err != nil {
		t.Fatalf("create user on node B during partition: %v", err)
	}
	if _, _, err := nodeB.service.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: userB.Key(),
		Sender:  clusterSenderKey(2, store.BootstrapAdminUserID),
		Body:    []byte("message-from-b"),
	}); err != nil {
		t.Fatalf("create message on node B during partition: %v", err)
	}

	scenario.Heal("node-a", "node-b")
	scenario.WaitForPeersActive(15*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	scenario.WaitForUserVisible(15*time.Second, "node-a", userB.Key())
	scenario.WaitForUserVisible(15*time.Second, "node-b", userA.Key())
	scenario.WaitForMessagesEqual(15*time.Second, "node-a", userB.Key(), 10, "message-from-b")
	scenario.WaitForMessagesEqual(15*time.Second, "node-b", userA.Key(), 10, "message-from-a")
	scenario.WaitForPeerAck(15*time.Second, "node-a", nodeB.id, 3)
	scenario.WaitForPeerAck(15*time.Second, "node-b", nodeA.id, 3)
}

func TestSimulationOutOfOrderDuplicateReplayConverges(t *testing.T) {
	t.Run("out-of-order-and-duplicate-broadcasts", func(t *testing.T) {
		scenario := newClusterTestScenario(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
		)
		nodeA := scenario.Node("node-a")
		nodeB := scenario.Node("node-b")

		scenario.Start()
		scenario.WaitForPeersActive(10*time.Second,
			expectClusterPeer(nodeA, nodeB.id),
			expectClusterPeer(nodeB, nodeA.id),
		)

		scenario.Hold("node-a", "node-b", clusterTestBroadcastEventBatch())
		scenario.Hold("node-a", "node-b", clusterTestBroadcastEventBatch())
		first, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "out-of-order-first",
			PasswordHash: "hash-1",
		})
		if err != nil {
			t.Fatalf("create first user: %v", err)
		}
		second, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "out-of-order-second",
			PasswordHash: "hash-2",
		})
		if err != nil {
			t.Fatalf("create second user: %v", err)
		}
		scenario.WaitForHeld("node-a", "node-b", 2, 10*time.Second)
		scenario.FlushHeld("node-a", "node-b", clusterTestFlushLIFO)

		scenario.DuplicateNext("node-a", "node-b", clusterTestBroadcastEventBatch())
		third, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "duplicated-broadcast",
			PasswordHash: "hash-3",
		})
		if err != nil {
			t.Fatalf("create third user: %v", err)
		}

		scenario.WaitForUserVisible(10*time.Second, "node-b", first.Key())
		scenario.WaitForUserVisible(10*time.Second, "node-b", second.Key())
		scenario.WaitForUserVisible(10*time.Second, "node-b", third.Key())
		scenario.WaitForOriginCursor(10*time.Second, "node-b", nodeA.id, 3)
	})

	t.Run("duplicate-pull-replay", func(t *testing.T) {
		scenario := newClusterTestScenario(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
		)
		nodeA := scenario.Node("node-a")
		nodeB := scenario.Node("node-b")

		scenario.Start("node-a")
		user, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "pull-replay-user",
			PasswordHash: "hash-1",
		})
		if err != nil {
			t.Fatalf("create offline pull replay user: %v", err)
		}
		for i := 1; i <= 3; i++ {
			if _, _, err := nodeA.store.CreateMessage(context.Background(), store.CreateMessageParams{
				UserKey: user.Key(),
				Sender:  clusterSenderKey(1, store.BootstrapAdminUserID),
				Body:    []byte("pull-replay-message-" + strconv.Itoa(i)),
			}); err != nil {
				t.Fatalf("create offline pull replay message %d: %v", i, err)
			}
		}

		scenario.DuplicateNext("node-a", "node-b", clusterTestPullEventBatch())
		scenario.Start("node-b")
		scenario.WaitForPeersActive(10*time.Second,
			expectClusterPeer(nodeA, nodeB.id),
			expectClusterPeer(nodeB, nodeA.id),
		)

		scenario.WaitForMessagesEqual(15*time.Second, "node-b", user.Key(), 10,
			"pull-replay-message-3",
			"pull-replay-message-2",
			"pull-replay-message-1",
		)
		scenario.WaitForOriginCursor(15*time.Second, "node-b", nodeA.id, 4)
	})
}

func TestSimulationBroadcastDuringPullCatchupConverges(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start("node-a")
	offlineUsers := make([]store.User, 0, 2)
	for i := 1; i <= 2; i++ {
		user, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "offline-catchup-" + strconv.Itoa(i),
			PasswordHash: "hash-" + strconv.Itoa(i),
		})
		if err != nil {
			t.Fatalf("create offline catchup user %d: %v", i, err)
		}
		offlineUsers = append(offlineUsers, user)
	}

	scenario.Hold("node-a", "node-b", clusterTestPullEventBatch())
	scenario.Start("node-b")
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)
	scenario.WaitForHeld("node-a", "node-b", 1, 10*time.Second)

	liveUser, liveEvent, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "live-during-pull",
		PasswordHash: "hash-live",
	})
	if err != nil {
		t.Fatalf("create live user during pull: %v", err)
	}
	nodeA.manager.Publish(liveEvent)

	scenario.WaitForUserVisible(10*time.Second, "node-b", liveUser.Key())
	scenario.FlushHeld("node-a", "node-b", clusterTestFlushFIFO)

	for _, user := range offlineUsers {
		scenario.WaitForUserVisible(15*time.Second, "node-b", user.Key())
	}
	scenario.WaitForOriginCursor(15*time.Second, "node-b", nodeA.id, 3)
	scenario.WaitForEventCount(15*time.Second, "node-b", 0, 10, 3)
}

func TestSimulationSnapshotRepairDuringEventStreamConverges(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	seedStore := newReplicationTestStore(t, "snapshot-seed", 1)
	snapshotUser, _, err := seedStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "snapshot-only-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create snapshot seed user: %v", err)
	}
	userChunk, err := seedStore.BuildSnapshotChunk(context.Background(), store.SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build seed user snapshot chunk: %v", err)
	}
	if err := nodeA.store.ApplySnapshotChunk(context.Background(), userChunk); err != nil {
		t.Fatalf("apply seed user snapshot chunk to node A: %v", err)
	}

	scenario.Hold("node-a", "node-b", clusterTestSnapshotChunkResponse())
	eventually(t, 10*time.Second, func() bool {
		if sess := nodeA.CurrentSession(nodeB.id); sess != nil {
			nodeA.manager.sendSnapshotDigest(sess)
		}
		return scenario.HeldCount("node-a", "node-b") >= 1
	})

	liveUser, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "live-during-snapshot",
		PasswordHash: "hash-live",
	})
	if err != nil {
		t.Fatalf("create live user during snapshot repair: %v", err)
	}
	scenario.WaitForUserVisible(10*time.Second, "node-b", liveUser.Key())

	scenario.FlushHeld("node-a", "node-b", clusterTestFlushFIFO)
	scenario.WaitForSnapshotRepairCompleted(15*time.Second, "node-b", snapshotUser.Key())
	scenario.WaitForUserVisible(15*time.Second, "node-b", liveUser.Key())
}

func TestSimulationLinkJitterLateJoinerConverges(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}},
		clusterTestNodeSpec{Name: "node-c", Slot: 3, PeerNames: []string{"node-b"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")
	nodeC := scenario.Node("node-c")

	scenario.Start("node-a", "node-b")
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	user, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "late-joiner-jitter",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create late joiner seed user: %v", err)
	}
	scenario.WaitForUserVisible(10*time.Second, "node-b", user.Key())

	scenario.Partition("node-b", "node-c")
	scenario.Start("node-c")
	scenario.Jitter("node-b", "node-c", 3)
	scenario.Heal("node-b", "node-c")
	scenario.WaitForPeersActive(20*time.Second,
		expectClusterPeer(nodeB, nodeC.id),
		expectClusterPeer(nodeC, nodeB.id),
	)

	nodeC.manager.broadcastRoutingUpdate()
	nodeB.manager.broadcastRoutingUpdate()
	waitForClusterRoutes(t, 20*time.Second, nodeA, nodeC.id)
	scenario.WaitForUserVisible(20*time.Second, "node-c", user.Key())
}

func TestSimulationDroppedBroadcastRepairsThroughSnapshot(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	scenario.DropNext("node-a", "node-b", clusterTestBroadcastEventBatch())
	user, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "dropped-broadcast-repair",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create dropped broadcast user: %v", err)
	}
	scenario.WaitForEnvelope("node-a", "node-b", clusterTestBroadcastEventBatch(), 10*time.Second)

	scenario.TriggerSnapshotDigest("node-a", "node-b")
	scenario.WaitForSnapshotRepairCompleted(15*time.Second, "node-b", user.Key())
}

func TestSimulationDelayedBroadcastConverges(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)

	scenario.DelayNext("node-a", "node-b", 150*time.Millisecond, clusterTestBroadcastEventBatch())
	user, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "delayed-broadcast",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create delayed broadcast user: %v", err)
	}

	scenario.WaitForUserVisible(10*time.Second, "node-b", user.Key())
}

func TestSimulationScriptedTimeSyncCanAdvancePeerOffset(t *testing.T) {
	scenario := newClusterTestScenario(t,
		clusterTestNodeSpec{
			Name:            "node-a",
			Slot:            1,
			PeerNames:       []string{"node-b"},
			TimeSyncSamples: []timeSyncSample{{offsetMs: 11, rttMs: 1}},
		},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := scenario.Node("node-a")
	nodeB := scenario.Node("node-b")

	scenario.Start()
	scenario.WaitForPeersActive(10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeA.id),
	)
	scenario.WaitForPeerClockOffset(10*time.Second, "node-a", nodeB.id, 11)

	scenario.SetTimeSyncSamples("node-a", timeSyncSample{offsetMs: 37, rttMs: 2})
	scenario.TriggerTimeSync("node-a", "node-b")
	scenario.WaitForPeerClockOffset(10*time.Second, "node-a", nodeB.id, 37)
}
