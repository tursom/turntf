package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func discoveryBatchFixture(t *testing.T) (*Manager, *store.Store, *sql.DB, func() int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "batch.db")
	st, err := store.Open(path, store.Options{NodeID: testNodeID(1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("turntf-sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
CREATE TABLE discovery_batch_writes(n INTEGER NOT NULL);
INSERT INTO discovery_batch_writes VALUES(0);
CREATE TRIGGER discovery_batch_insert AFTER INSERT ON discovered_peers BEGIN UPDATE discovery_batch_writes SET n=n+1; END;
CREATE TRIGGER discovery_batch_update AFTER UPDATE ON discovered_peers BEGIN UPDATE discovery_batch_writes SET n=n+1; END;
`); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT n FROM discovery_batch_writes`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return newDiscoveryTestManager(t, st), st, db, count
}

func TestDiscoveryBatchMergesRoundAndKeepsRuntimeFresh(t *testing.T) {
	m, st, _, count := discoveryBatchFixture(t)
	ctx := context.Background()
	url := "ws://batch.example/internal/cluster/ws"
	otherURL := "ws://other.example/internal/cluster/ws"
	key := strings.Repeat("0", 40)
	update := &internalproto.MembershipUpdate{Peers: []*internalproto.PeerAdvertisement{
		{NodeId: testNodeID(2), Url: url, Generation: 9, ZeromqCurveServerPublicKey: key},
		{NodeId: testNodeID(2), Url: url, Generation: 3},
		{NodeId: testNodeID(3), Url: otherURL, Generation: 4},
		{NodeId: testNodeID(4), Url: url}, // URL 绑定冲突仍逐项拒绝。
		{NodeId: 0, Url: otherURL},
		nil,
		{NodeId: testNodeID(1), Url: "ws://self.example/internal/cluster/ws", Generation: 7},
	}}
	if err := m.handleMembershipUpdateBody(testNodeID(5), update); err != nil {
		t.Fatal(err)
	}
	if count() != 2 || m.discoveryRejects != 2 || m.membershipUpdatesRecv != 1 {
		t.Fatalf("writes=%d rejects=%d recv=%d", count(), m.discoveryRejects, m.membershipUpdatesRecv)
	}
	rows, err := st.ListDiscoveredPeers(ctx)
	if err != nil || len(rows) != 2 || rows[0].Generation != 9 || rows[0].ZeroMQCurveServerPublicKey != key {
		t.Fatalf("round merge: rows=%+v err=%v", rows, err)
	}
	oldSeen := m.discoveredPeers[url].lastSeenAt
	update.Peers = update.Peers[:3]
	update.Peers[0].Generation = 10
	if err := m.handleMembershipUpdateBody(testNodeID(5), update); err != nil {
		t.Fatal(err)
	}
	peer := m.discoveredPeers[url]
	if count() != 2 || peer.generation != 10 || peer.lastSeenAt.Before(oldSeen) {
		t.Fatalf("runtime throttled: writes=%d peer=%+v", count(), peer)
	}
	rows, err = st.ListDiscoveredPeers(ctx)
	if err != nil || rows[0].Generation != 9 {
		t.Fatalf("generation-only persisted early: rows=%+v err=%v", rows, err)
	}
	// source 是关键字段，即使一分钟内也必须立即写入，并带上最新代际。
	if err := m.handleMembershipUpdateBody(testNodeID(6), update); err != nil {
		t.Fatal(err)
	}
	if count() != 4 {
		t.Fatalf("source change: writes=%d", count())
	}
	rows, err = st.ListDiscoveredPeers(ctx)
	if err != nil || rows[0].Generation != 10 || rows[0].SourcePeerNodeID != testNodeID(6) {
		t.Fatalf("latest generation lost: %+v %v", rows, err)
	}
}

func TestDiscoveryBatchFailureRetriesAndRestart(t *testing.T) {
	m, st, db, count := discoveryBatchFixture(t)
	ctx := context.Background()
	update := &internalproto.MembershipUpdate{Peers: []*internalproto.PeerAdvertisement{
		{NodeId: testNodeID(2), Url: "ws://first.example/internal/cluster/ws", Generation: 5},
		{NodeId: testNodeID(3), Url: "ws://second.example/internal/cluster/ws", Generation: 6},
	}}
	if _, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER discovery_batch_fail BEFORE INSERT ON discovered_peers WHEN NEW.node_id = %d BEGIN SELECT RAISE(ABORT, 'batch failure'); END`, testNodeID(3))); err != nil {
		t.Fatal(err)
	}
	if err := m.handleMembershipUpdateBody(testNodeID(4), update); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListDiscoveredPeers(ctx)
	if err != nil || len(rows) != 0 || count() != 0 || len(m.discoveredPeers) != 2 || m.discoveryPersistFailures != 2 {
		t.Fatalf("failed batch: rows=%+v writes=%d runtime=%d failures=%d err=%v", rows, count(), len(m.discoveredPeers), m.discoveryPersistFailures, err)
	}
	if _, err := db.Exec(`DROP TRIGGER discovery_batch_fail`); err != nil {
		t.Fatal(err)
	}
	if err := m.handleMembershipUpdateBody(testNodeID(4), update); err != nil {
		t.Fatal(err)
	}
	if count() != 2 {
		t.Fatalf("retry skipped write: %d", count())
	}
	url := update.Peers[0].Url
	m.recordDiscoveredState(testNodeID(2), url, discoveryStateConnected, "", true)
	if count() != 3 {
		t.Fatalf("connection not persisted: %d", count())
	}
	rows, err = st.ListDiscoveredPeers(ctx)
	if err != nil || rows[0].LastConnectedAt == nil {
		t.Fatalf("connection time absent: %+v %v", rows, err)
	}
	connected := *rows[0].LastConnectedAt
	// 重复广告在 connected 状态下也不能因重新生成 HLC logical 而触发写入。
	for i := 0; i < 5; i++ {
		if err := m.handleMembershipUpdateBody(testNodeID(4), update); err != nil {
			t.Fatal(err)
		}
	}
	if count() != 3 {
		t.Fatalf("same connection rewrote %d rows", count())
	}
	// 状态/source/error 都不变，真实连接时间改变仍必须立即写入。
	live := m.discoveredPeers[url]
	live.lastConnectedAt = live.lastConnectedAt.Add(time.Millisecond)
	m.persistDiscoveredPeer(*live, true)
	if count() != 4 {
		t.Fatalf("new connection skipped: %d", count())
	}
	connected.WallTimeMs++
	reloaded := newDiscoveryTestManager(t, st)
	peer := reloaded.discoveredPeers[url]
	if peer == nil || peer.state != discoveryStateCandidate || peer.generation != 5 || peer.lastConnectedAt.UnixMilli() != connected.WallTimeMs || recoveryAdvertises(reloaded, testNodeID(2), url) {
		t.Fatalf("bad restart state: %+v", peer)
	}
	// 重启后的 candidate 是真实状态变化，应立即落盘并保留连接历史。
	if err := reloaded.handleMembershipUpdateBody(testNodeID(4), update); err != nil {
		t.Fatal(err)
	}
	rows, err = st.ListDiscoveredPeers(ctx)
	if err != nil || count() != 5 || rows[0].State != discoveryStateCandidate || rows[0].LastConnectedAt == nil || *rows[0].LastConnectedAt != connected {
		t.Fatalf("restart observation: writes=%d rows=%+v err=%v", count(), rows, err)
	}
}

func TestDiscoveryConnectionTimestampProjectionIsStable(t *testing.T) {
	wall := int64(1740000000000)
	hlc := clock.NewClockWithSource(testNodeID(1), func() int64 { return wall })
	connected := time.UnixMilli(wall - 1000)
	first := clockTimestampPointer(hlc, &connected)
	second := clockTimestampPointer(hlc, &connected)
	if first == nil || second == nil || *first != *second || first.WallTimeMs != connected.UnixMilli() || first.NodeID != testNodeID(1) {
		t.Fatalf("unstable projection: %v %v", first, second)
	}
	if clockTimestampPointer(hlc, nil) != nil {
		t.Fatal("nil timestamp changed")
	}
}
