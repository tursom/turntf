package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestPresenceDigestDetectsMissingDeltaAfterNewerGeneration(t *testing.T) {
	sender := newOnlinePresenceUpdateTestManager(t)
	receiver := newOnlinePresenceUpdateTestManager(t)
	sender.cfg.NodeID = testNodeID(2)
	first := store.UserKey{NodeID: sender.cfg.NodeID, UserID: 1001}
	second := first
	for second.UserID++; onlinePresenceShardIndex(second) != onlinePresenceShardIndex(first); second.UserID++ {
	}
	register := func(user store.UserKey) {
		sender.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: store.SessionRef{ServingNodeID: sender.cfg.NodeID, SessionID: "test"}, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "user"})
	}
	register(first)
	for i := 0; i < onlinePresenceShardCount; i++ {
		if _, err := receiver.applyOnlinePresenceUpdate(sender.takeAuthoritativePresenceShard(i)); err != nil {
			t.Fatal(err)
		}
	}
	shard := onlinePresenceShardIndex(first)
	sender.UnregisterLocalSession(first, store.SessionRef{ServingNodeID: sender.cfg.NodeID, SessionID: "test"})
	_ = sender.takeOnlinePresenceDelta(shard) // 模拟该删除增量丢失。
	register(second)
	if _, err := receiver.applyOnlinePresenceUpdate(sender.takeOnlinePresenceDelta(shard)); err != nil {
		t.Fatal(err)
	}
	assertRemotePresenceUser(t, receiver, first, true)
	sender.mu.Lock()
	digest := sender.presenceControlLocked(internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST)
	digest.StateDigest = sender.presenceDigestLocked(sender.cfg.NodeID)
	sender.mu.Unlock()
	if _, err := receiver.applyOnlinePresenceUpdate(digest); err != nil {
		t.Fatal(err)
	}
	if _, ok := receiver.pendingPresenceRepairs[presenceRepairKey{nodeID: sender.cfg.NodeID}]; !ok {
		t.Fatal("higher generation hid a missed delta from digest repair")
	}
	for i := 0; i < onlinePresenceShardCount; i++ {
		sender.mu.Lock()
		full := sender.snapshotPresenceShardLocked(i)
		sender.mu.Unlock()
		if _, err := receiver.applyOnlinePresenceUpdate(full); err != nil {
			t.Fatal(err)
		}
	}
	assertRemotePresenceUser(t, receiver, first, false)
	assertRemotePresenceUser(t, receiver, second, true)
	if !bytes.Equal(digest.StateDigest, receiver.presenceDigestLocked(sender.cfg.NodeID)) {
		t.Fatal("digest did not converge after authoritative repair")
	}
	clear(receiver.pendingPresenceRepairs)
	if _, err := receiver.applyOnlinePresenceUpdate(digest); err != nil || len(receiver.pendingPresenceRepairs) != 0 {
		t.Fatalf("unchanged state requested redundant repair: %v", err)
	}
	// 发给某个邻居的全量修复不能吞掉其他邻居尚未收到的增量。
	if len(sender.dirtyPresenceUsersByShard[shard]) != 0 {
		t.Fatal("test delta was not consumed")
	}
	register(first)
	sender.mu.Lock()
	_ = sender.snapshotPresenceShardLocked(shard)
	sender.mu.Unlock()
	if sender.takeOnlinePresenceDelta(shard) == nil {
		t.Fatal("targeted full repair swallowed dirty user delta")
	}
}

func TestPresenceDigestRepairAcrossRealDirectNodes(t *testing.T) {
	source, target := startMeshManagerPair(t, true)
	waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlCritical)
	waitForMeshRoute(t, target, source.cfg.NodeID, mesh.TrafficControlCritical)
	user := store.UserKey{NodeID: target.cfg.NodeID, UserID: 9001}
	target.RegisterLocalSession(store.OnlineSession{User: user, SessionRef: store.SessionRef{ServingNodeID: target.cfg.NodeID, SessionID: "digest-repair"}, Transport: "ws"}, app.LoggedInUserSummary{NodeID: user.NodeID, UserID: user.UserID, Username: "digest-repair"})
	waitFor(t, 2*time.Second, func() bool {
		p, _ := source.QueryOnlineUserPresence(context.Background(), user)
		return len(p) == 1
	})
	if !target.MeshRuntime().runtime.SupportsPresenceDigest(source.cfg.NodeID) {
		t.Fatal("new direct peer did not negotiate digest")
	}
	source.mu.Lock()
	delete(source.onlinePresenceByUser, user)
	source.mu.Unlock()
	target.repairOnlinePresenceShard(0, false)
	waitFor(t, 2*time.Second, func() bool {
		p, _ := source.QueryOnlineUserPresence(context.Background(), user)
		return len(p) == 1 && p[0].ServingNodeID == target.cfg.NodeID
	})
	stats := target.meshStatusSnapshot().PresenceSync
	if stats.DigestSent == 0 || stats.RepairRequestsReceived == 0 {
		t.Fatalf("actual repair did not use negotiated digest/request: %+v", stats)
	}
}

func TestPresenceControlRejectsMalformedAndStaleMessages(t *testing.T) {
	m := newOnlinePresenceUpdateTestManager(t)
	base := &internalproto.OnlinePresenceUpdate{OriginNodeId: testNodeID(2), RuntimeEpoch: 100, Generation: 1, ShardCount: onlinePresenceShardCount, Mode: internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST, StateDigest: make([]byte, 31)}
	if _, err := m.applyOnlinePresenceUpdate(base); err == nil || len(m.pendingPresenceRepairs) != 0 {
		t.Fatal("malformed digest was accepted")
	}
	base.StateDigest = make([]byte, 32)
	m.remoteRuntimeEpochs[base.OriginNodeId] = 101
	if _, err := m.applyOnlinePresenceUpdate(base); err != nil || len(m.pendingPresenceRepairs) != 0 {
		t.Fatal("old epoch digest triggered repair")
	}
}
