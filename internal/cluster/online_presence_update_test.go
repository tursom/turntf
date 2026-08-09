package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerApplyOnlinePresenceDeltaUpsertsAndRemovesUser(t *testing.T) {
	t.Parallel()

	mgr := newOnlinePresenceUpdateTestManager(t)
	user := store.UserKey{NodeID: testNodeID(2), UserID: 1001}
	shard := onlinePresenceShardIndex(user)

	applied, err := mgr.applyOnlinePresenceUpdate(testOnlinePresenceUpdate(
		internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
		shard,
		1,
		user,
	))
	if err != nil || !applied {
		t.Fatalf("apply presence delta: applied=%v err=%v", applied, err)
	}
	assertRemotePresenceUser(t, mgr, user, true)

	stale := testOnlinePresenceRemoval(shard, 1, user)
	applied, err = mgr.applyOnlinePresenceUpdate(stale)
	if err != nil {
		t.Fatalf("apply stale presence delta: %v", err)
	}
	if applied {
		t.Fatal("expected duplicate shard generation to be ignored")
	}
	assertRemotePresenceUser(t, mgr, user, true)

	applied, err = mgr.applyOnlinePresenceUpdate(testOnlinePresenceRemoval(shard, 2, user))
	if err != nil || !applied {
		t.Fatalf("apply presence removal: applied=%v err=%v", applied, err)
	}
	assertRemotePresenceUser(t, mgr, user, false)
}

func TestManagerAuthoritativePresenceShardRepairsMissedDeltaWithoutChangingOtherShards(t *testing.T) {
	t.Parallel()

	mgr := newOnlinePresenceUpdateTestManager(t)
	first := store.UserKey{NodeID: testNodeID(2), UserID: 2001}
	firstShard := onlinePresenceShardIndex(first)
	second := first
	for second.UserID++; onlinePresenceShardIndex(second) == firstShard; second.UserID++ {
	}

	for _, user := range []store.UserKey{first, second} {
		applied, err := mgr.applyOnlinePresenceUpdate(testOnlinePresenceUpdate(
			internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
			onlinePresenceShardIndex(user),
			1,
			user,
		))
		if err != nil || !applied {
			t.Fatalf("apply initial presence for %+v: applied=%v err=%v", user, applied, err)
		}
	}

	applied, err := mgr.applyOnlinePresenceUpdate(&internalproto.OnlinePresenceUpdate{
		OriginNodeId: testNodeID(2),
		RuntimeEpoch: 100,
		Mode:         internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD,
		ShardIndex:   uint32(firstShard),
		ShardCount:   onlinePresenceShardCount,
		Generation:   2,
	})
	if err != nil || !applied {
		t.Fatalf("apply authoritative shard: applied=%v err=%v", applied, err)
	}
	assertRemotePresenceUser(t, mgr, first, false)
	assertRemotePresenceUser(t, mgr, second, true)
}

func TestValidateOnlinePresenceUpdateRejectsIncompleteAndDuplicateEntries(t *testing.T) {
	t.Parallel()

	user := store.UserKey{NodeID: testNodeID(2), UserID: 5001}
	shard := onlinePresenceShardIndex(user)
	tests := []struct {
		name   string
		mutate func(*internalproto.OnlinePresenceUpdate)
	}{
		{
			name: "missing logged-in summary",
			mutate: func(update *internalproto.OnlinePresenceUpdate) {
				update.LoggedInUsers = nil
			},
		},
		{
			name: "duplicate presence",
			mutate: func(update *internalproto.OnlinePresenceUpdate) {
				update.Items = append(update.Items, update.Items[0])
			},
		},
		{
			name: "duplicate logged-in summary",
			mutate: func(update *internalproto.OnlinePresenceUpdate) {
				update.LoggedInUsers = append(update.LoggedInUsers, update.LoggedInUsers[0])
			},
		},
		{
			name: "missing transport hint",
			mutate: func(update *internalproto.OnlinePresenceUpdate) {
				update.Items[0].TransportHint = " "
			},
		},
		{
			name: "missing username",
			mutate: func(update *internalproto.OnlinePresenceUpdate) {
				update.LoggedInUsers[0].Username = " "
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			update := testOnlinePresenceUpdate(
				internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
				shard,
				1,
				user,
			)
			tt.mutate(update)
			if _, err := validateOnlinePresenceUpdate(update); err == nil {
				t.Fatalf("expected invalid update to be rejected: %+v", update)
			}
		})
	}
}

func TestManagerCoalescesLocalSessionChangesIntoOneUserDelta(t *testing.T) {
	t.Parallel()

	mgr := newOnlinePresenceUpdateTestManager(t)
	user := store.UserKey{NodeID: testNodeID(1), UserID: 3001}
	summary := app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "local-user"}
	first := store.OnlineSession{
		User: user, SessionRef: store.SessionRef{ServingNodeID: testNodeID(1), SessionID: "first"}, Transport: "ws",
	}
	second := store.OnlineSession{
		User: user, SessionRef: store.SessionRef{ServingNodeID: testNodeID(1), SessionID: "second"}, Transport: "ws",
	}
	mgr.RegisterLocalSession(first, summary)
	mgr.RegisterLocalSession(second, summary)

	update := mgr.takeOnlinePresenceDelta(onlinePresenceShardIndex(user))
	if update == nil || len(update.GetItems()) != 1 || len(update.GetLoggedInUsers()) != 1 || len(update.GetRemovedUsers()) != 0 {
		t.Fatalf("unexpected coalesced registration delta: %+v", update)
	}
	if got := update.GetItems()[0].GetSessionCount(); got != 2 {
		t.Fatalf("unexpected coalesced session count: got %d want 2", got)
	}
	if duplicate := mgr.takeOnlinePresenceDelta(onlinePresenceShardIndex(user)); duplicate != nil {
		t.Fatalf("expected dirty user to be drained once, got %+v", duplicate)
	}

	mgr.UnregisterLocalSession(user, first.SessionRef)
	mgr.UnregisterLocalSession(user, second.SessionRef)
	removal := mgr.takeOnlinePresenceDelta(onlinePresenceShardIndex(user))
	if removal == nil || len(removal.GetRemovedUsers()) != 1 || len(removal.GetItems()) != 0 || len(removal.GetLoggedInUsers()) != 0 {
		t.Fatalf("unexpected coalesced removal delta: %+v", removal)
	}
}

func TestManagerPresenceLoopFlushesCoalescedUserAfterDelay(t *testing.T) {
	mgr := newOnlinePresenceUpdateTestManager(t)
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.wg.Add(1)
	go mgr.presenceLoop()
	t.Cleanup(func() {
		mgr.cancel()
		mgr.wg.Wait()
	})

	user := store.UserKey{NodeID: testNodeID(1), UserID: 3002}
	summary := app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "local-user"}
	register := func(sessionID string) {
		mgr.RegisterLocalSession(store.OnlineSession{
			User: user, SessionRef: store.SessionRef{ServingNodeID: testNodeID(1), SessionID: sessionID}, Transport: "ws",
		}, summary)
	}

	register("first")
	time.Sleep(onlinePresenceDeltaFlushInterval / 2)
	register("second")

	waitFor(t, 5*onlinePresenceDeltaFlushInterval, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		shard := onlinePresenceShardIndex(user)
		return mgr.localPresenceGenerations[shard] == 1 && len(mgr.dirtyPresenceUsersByShard[shard]) == 0
	})

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if got := len(mgr.localOnlineSessions[user]); got != 2 {
		t.Fatalf("unexpected coalesced local session count: got %d want 2", got)
	}
}

func TestManagerRejectsInvalidOnlinePresenceUpdateAtomically(t *testing.T) {
	t.Parallel()

	mgr := newOnlinePresenceUpdateTestManager(t)
	user := store.UserKey{NodeID: testNodeID(2), UserID: 4001}
	shard := onlinePresenceShardIndex(user)
	valid := testOnlinePresenceUpdate(
		internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
		shard,
		1,
		user,
	)
	if applied, err := mgr.applyOnlinePresenceUpdate(valid); err != nil || !applied {
		t.Fatalf("apply valid update: applied=%v err=%v", applied, err)
	}

	invalid := testOnlinePresenceUpdate(
		internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
		(shard+1)%onlinePresenceShardCount,
		3,
		user,
	)
	if applied, err := mgr.applyOnlinePresenceUpdate(invalid); err == nil || applied {
		t.Fatalf("expected cross-shard update rejection: applied=%v err=%v", applied, err)
	}
	assertRemotePresenceUser(t, mgr, user, true)

	if applied, err := mgr.applyOnlinePresenceUpdate(testOnlinePresenceRemoval(shard, 2, user)); err != nil || !applied {
		t.Fatalf("expected valid generation after rejected update: applied=%v err=%v", applied, err)
	}
	assertRemotePresenceUser(t, mgr, user, false)
}

func newOnlinePresenceUpdateTestManager(t testing.TB) *Manager {
	t.Helper()
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr
}

func testOnlinePresenceUpdate(mode internalproto.OnlinePresenceUpdateMode, shard int, generation uint64, user store.UserKey) *internalproto.OnlinePresenceUpdate {
	return &internalproto.OnlinePresenceUpdate{
		OriginNodeId: testNodeID(2),
		RuntimeEpoch: 100,
		Mode:         mode,
		ShardIndex:   uint32(shard),
		ShardCount:   onlinePresenceShardCount,
		Generation:   generation,
		Items: []*internalproto.ClusterOnlineNodePresence{{
			User:          &internalproto.ClusterUserRef{NodeId: user.NodeID, UserId: user.UserID},
			ServingNodeId: testNodeID(2),
			SessionCount:  1,
			TransportHint: "ws",
		}},
		LoggedInUsers: []*internalproto.ClusterLoggedInUser{{
			NodeId:   user.NodeID,
			UserId:   user.UserID,
			Username: "user",
		}},
	}
}

func testOnlinePresenceRemoval(shard int, generation uint64, user store.UserKey) *internalproto.OnlinePresenceUpdate {
	return &internalproto.OnlinePresenceUpdate{
		OriginNodeId: testNodeID(2),
		RuntimeEpoch: 100,
		Mode:         internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
		ShardIndex:   uint32(shard),
		ShardCount:   onlinePresenceShardCount,
		Generation:   generation,
		RemovedUsers: []*internalproto.ClusterUserRef{{NodeId: user.NodeID, UserId: user.UserID}},
	}
}

func assertRemotePresenceUser(t *testing.T, mgr *Manager, user store.UserKey, want bool) {
	t.Helper()
	presence, err := mgr.QueryOnlineUserPresence(context.Background(), user)
	if err != nil {
		t.Fatalf("query presence for %+v: %v", user, err)
	}
	if got := len(presence) == 1; got != want {
		t.Fatalf("unexpected presence for %+v: got=%+v want_present=%v", user, presence, want)
	}
	users, err := mgr.QueryLoggedInUsers(context.Background(), testNodeID(2))
	if err != nil {
		t.Fatalf("query logged-in users: %v", err)
	}
	found := false
	for _, item := range users {
		if item.NodeID == user.NodeID && item.UserID == user.UserID {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("unexpected logged-in mirror for %+v: users=%+v want_present=%v", user, users, want)
	}
}
