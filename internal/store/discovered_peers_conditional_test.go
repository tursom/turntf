package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tursom/turntf/internal/clock"
)

// 计数器与被测写入共用事务；只统计真实 INSERT/UPDATE 行，跳过的 upsert 不触发。
func discoveryWriteCounter(t *testing.T, st *Store) func() int {
	t.Helper()
	if _, err := st.db.Exec(`
CREATE TABLE discovery_test_writes(n INTEGER NOT NULL);
INSERT INTO discovery_test_writes VALUES(0);
CREATE TRIGGER discovery_test_insert AFTER INSERT ON discovered_peers BEGIN UPDATE discovery_test_writes SET n=n+1; END;
CREATE TRIGGER discovery_test_update AFTER UPDATE ON discovered_peers BEGIN UPDATE discovery_test_writes SET n=n+1; END;
`); err != nil {
		t.Fatal(err)
	}
	return func() int {
		t.Helper()
		var n int
		if err := st.db.QueryRow(`SELECT n FROM discovery_test_writes`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
}

func discoveryTestPeer() DiscoveredPeer {
	ts := clock.Timestamp{WallTimeMs: 1740000000000, NodeID: testNodeID(1)}
	return DiscoveredPeer{NodeID: testNodeID(2), URL: "ws://peer.example/internal/cluster/ws", State: "candidate", SourcePeerNodeID: testNodeID(3), Generation: 7, FirstSeenAt: ts, LastSeenAt: ts}
}

func discoveryRows(t *testing.T, st *Store) []DiscoveredPeer {
	t.Helper()
	rows, err := st.ListDiscoveredPeers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestDiscoveredPeerConditionalWritesMinuteBoundary(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	count := discoveryWriteCounter(t, st)
	ctx := context.Background()
	peer := discoveryTestPeer()
	base := peer.LastSeenAt.WallTimeMs
	for _, step := range []struct {
		ms             int64
		generation     uint64
		writes         int
		wantMS         int64
		wantGeneration uint64
	}{
		{0, 7, 1, 0, 7},
		{0, 8, 1, 0, 7},
		{59999, 9, 1, 0, 7},
		{60000, 10, 2, 60000, 10},
		{60001, 11, 2, 60000, 10},
		{119999, 12, 2, 60000, 10},
		{120000, 13, 3, 120000, 13},
		// 乱序时间和代际不会因比较 HLC logical 而绕过间隔。
		{1, 2, 3, 120000, 13},
	} {
		peer.LastSeenAt.WallTimeMs = base + step.ms
		peer.LastSeenAt.Logical++
		peer.Generation = step.generation
		if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
			t.Fatal(err)
		}
		got := discoveryRows(t, st)[0]
		if count() != step.writes || got.LastSeenAt.WallTimeMs != base+step.wantMS || got.Generation != step.wantGeneration || got.FirstSeenAt != peer.FirstSeenAt {
			t.Fatalf("offset %d: writes=%d row=%+v", step.ms, count(), got)
		}
	}
}

func TestDiscoveredPeerCriticalChangesAndMerge(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	count := discoveryWriteCounter(t, st)
	ctx := context.Background()
	peer := discoveryTestPeer()
	connected := peer.LastSeenAt
	connected.Logical = 42
	peer.LastConnectedAt = &connected
	peer.ZeroMQCurveServerPublicKey = "key-one"
	if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
		t.Fatal(err)
	}
	// 未提供字段不能擦除密钥或完整连接 HLC。
	peer.ZeroMQCurveServerPublicKey = "  "
	peer.LastConnectedAt = nil
	peer.Generation = 6
	if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
		t.Fatal(err)
	}
	if count() != 1 {
		t.Fatalf("empty fields caused %d writes", count())
	}
	for i, mutate := range []func(){
		func() { peer.State = "failed" },
		func() { peer.SourcePeerNodeID = 0 },
		func() { peer.LastError = " dial failed " },
		func() { peer.LastError = "" },
		func() { peer.ZeroMQCurveServerPublicKey = " key-two " },
		func() { next := connected; next.Logical++; peer.LastConnectedAt = &next },
		func() { next := connected; next.WallTimeMs++; peer.LastConnectedAt = &next },
	} {
		mutate()
		peer.LastSeenAt.WallTimeMs++
		if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
			t.Fatal(err)
		}
		if count() != i+2 {
			t.Fatalf("critical change %d: writes=%d", i, count())
		}
		if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
			t.Fatal(err)
		}
		if count() != i+2 {
			t.Fatalf("duplicate critical change %d wrote again", i)
		}
	}
	row := discoveryRows(t, st)[0]
	if row.Generation != 7 || row.FirstSeenAt != peer.FirstSeenAt || row.ZeroMQCurveServerPublicKey != "key-two" || row.LastConnectedAt == nil || *row.LastConnectedAt != *peer.LastConnectedAt {
		t.Fatalf("merge lost fields: %+v", row)
	}
	// URL 为复合主键的一部分，新 URL 立即插入，保留旧 URL。
	peer.URL += "/new"
	if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
		t.Fatal(err)
	}
	if len(discoveryRows(t, st)) != 2 || count() != 9 {
		t.Fatalf("URL change: writes=%d", count())
	}
}

func TestDiscoveredPeersBatchMinuteBoundaryAndConcurrentWriters(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	count := discoveryWriteCounter(t, st)
	ctx := context.Background()
	first := discoveryTestPeer()
	second := first
	second.NodeID = testNodeID(4)
	peers := []DiscoveredPeer{first, second}
	if err := st.UpsertDiscoveredPeers(ctx, peers); err != nil {
		t.Fatal(err)
	}
	for i := range peers {
		peers[i].LastSeenAt.WallTimeMs += 59999
		peers[i].Generation++
	}
	if err := st.UpsertDiscoveredPeers(ctx, peers); err != nil {
		t.Fatal(err)
	}
	if count() != 2 {
		t.Fatalf("batch wrote before minute: %d", count())
	}
	for i := range peers {
		peers[i].LastSeenAt.WallTimeMs++
	}
	// 多个请求同时到达边界也只能各写一行，判断依据必须是已提交记录。
	errs := make(chan error, 8)
	for i := 0; i < cap(errs); i++ {
		go func() { errs <- st.UpsertDiscoveredPeers(ctx, peers) }()
	}
	for i := 0; i < cap(errs); i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if count() != 4 {
		t.Fatalf("concurrent minute refresh: writes=%d", count())
	}
	for _, row := range discoveryRows(t, st) {
		if row.Generation != first.Generation+1 || row.LastSeenAt.WallTimeMs != first.LastSeenAt.WallTimeMs+60000 {
			t.Fatalf("batch boundary lost fields: %+v", row)
		}
	}
}

func TestDiscoveredPeerDefaultsAndNormalization(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	count := discoveryWriteCounter(t, st)
	now := int64(1740000000000)
	st.clock = clock.NewClockWithSource(testNodeID(1), func() int64 { return now })
	peer := DiscoveredPeer{NodeID: testNodeID(2), URL: " ws://peer.example/ws ", State: " candidate ", ZeroMQCurveServerPublicKey: " key ", LastError: " error "}
	if err := st.UpsertDiscoveredPeers(context.Background(), []DiscoveredPeer{peer}); err != nil {
		t.Fatal(err)
	}
	first := discoveryRows(t, st)[0]
	if first.FirstSeenAt.WallTimeMs != now || first.LastSeenAt != first.FirstSeenAt || first.URL != "ws://peer.example/ws" || first.State != "candidate" || first.ZeroMQCurveServerPublicKey != "key" || first.LastError != "error" {
		t.Fatalf("defaults/normalization changed: %+v", first)
	}
	if err := st.UpsertDiscoveredPeer(context.Background(), peer); err != nil {
		t.Fatal(err)
	}
	if count() != 1 {
		t.Fatal("normalized duplicate wrote a row")
	}
	now += 60000
	peer.FirstSeenAt = st.clock.Now()
	if err := st.UpsertDiscoveredPeer(context.Background(), peer); err != nil {
		t.Fatal(err)
	}
	row := discoveryRows(t, st)[0]
	if row.FirstSeenAt != first.FirstSeenAt || row.LastSeenAt.WallTimeMs != now || count() != 2 {
		t.Fatalf("firstSeen changed or default lastSeen not refreshed: %+v", row)
	}
}

func TestDiscoveredPeerStateConditionalWrites(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	count := discoveryWriteCounter(t, st)
	peer := discoveryTestPeer()
	now := peer.LastSeenAt.WallTimeMs
	st.clock = clock.NewClockWithSource(testNodeID(1), func() int64 { return now })
	ctx := context.Background()
	peer.LastConnectedAt = &peer.FirstSeenAt
	peer.ZeroMQCurveServerPublicKey = "key"
	if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		ms             int64
		state, message string
		connected      bool
		writes         int
	}{
		{59999, "candidate", "", false, 1},
		{60000, "candidate", "", false, 2},
		{60001, "failed", "oops", false, 3},
		{60002, "failed", "", false, 4},
		{60003, "connected", "", true, 5},
		{60004, "connected", "", true, 6},
		{60005, "connected", "", false, 6},
	} {
		now = peer.FirstSeenAt.WallTimeMs + step.ms
		if err := st.RecordDiscoveredPeerState(ctx, peer.NodeID, peer.URL, step.state, step.message, step.connected); err != nil {
			t.Fatal(err)
		}
		if count() != step.writes {
			t.Fatalf("offset %d: writes=%d", step.ms, count())
		}
	}
	row := discoveryRows(t, st)[0]
	if row.ZeroMQCurveServerPublicKey != "key" || row.LastConnectedAt == nil || row.LastConnectedAt.WallTimeMs != peer.FirstSeenAt.WallTimeMs+60004 {
		t.Fatalf("state merge lost fields: %+v", row)
	}
	if err := st.RecordDiscoveredPeerState(ctx, testNodeID(4), peer.URL, "failed", "", false); err != nil {
		t.Fatal(err)
	}
	if count() != 6 {
		t.Fatal("state UPDATE created missing peer")
	}
}

func TestDiscoveredPeersBatchValidationAndRollback(t *testing.T) {
	ctx := context.Background()
	for _, failure := range []string{"node", "url", "state", "source", "exec", "commit", "canceled", "prepare"} {
		t.Run(failure, func(t *testing.T) {
			st := openTestStore(t)
			defer st.Close()
			st.db.SetMaxOpenConns(1)
			count := discoveryWriteCounter(t, st)
			first := discoveryTestPeer()
			if err := st.UpsertDiscoveredPeer(ctx, first); err != nil {
				t.Fatal(err)
			}
			first.State = "connected"
			second := first
			second.NodeID = testNodeID(4)
			callCtx := ctx
			validation := true
			switch failure {
			case "node":
				second.NodeID = 0
			case "url":
				second.URL = " "
			case "state":
				second.State = " "
			case "source":
				second.SourcePeerNodeID = -1
			case "exec":
				validation = false
				if _, err := st.db.Exec(fmt.Sprintf(`CREATE TRIGGER discovery_fail BEFORE INSERT ON discovered_peers WHEN NEW.node_id = %d BEGIN SELECT RAISE(ABORT, 'injected discovery failure'); END`, second.NodeID)); err != nil {
					t.Fatal(err)
				}
			case "commit":
				validation = false
				if _, err := st.db.Exec(fmt.Sprintf(`
CREATE TABLE discovery_parent(id INTEGER PRIMARY KEY);
CREATE TABLE discovery_child(id INTEGER REFERENCES discovery_parent(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER discovery_fail AFTER INSERT ON discovered_peers WHEN NEW.node_id = %d BEGIN INSERT INTO discovery_child VALUES(99); END;`, second.NodeID)); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				validation = false
				var cancel context.CancelFunc
				callCtx, cancel = context.WithCancel(ctx)
				cancel()
			case "prepare":
				validation = false
				if _, err := st.db.Exec(`ALTER TABLE discovered_peers RENAME TO discovery_saved`); err != nil {
					t.Fatal(err)
				}
			}
			err := st.UpsertDiscoveredPeers(callCtx, []DiscoveredPeer{first, second})
			if err == nil || (validation && !errors.Is(err, ErrInvalidInput)) {
				t.Fatalf("unexpected error: %v", err)
			}
			if failure == "prepare" {
				if _, err := st.db.Exec(`ALTER TABLE discovery_saved RENAME TO discovered_peers`); err != nil {
					t.Fatal(err)
				}
			}
			rows := discoveryRows(t, st)
			if count() != 1 || len(rows) != 1 || rows[0].State != "candidate" {
				t.Fatalf("partial transaction: writes=%d rows=%+v", count(), rows)
			}
			if failure == "exec" || failure == "commit" {
				if _, err := st.db.Exec(`DROP TRIGGER discovery_fail`); err != nil {
					t.Fatal(err)
				}
			}
			// 修复输入/数据库后重试：前次失败不能吞掉任何一条更新。
			second = first
			second.NodeID = testNodeID(4)
			if err := st.UpsertDiscoveredPeers(ctx, []DiscoveredPeer{first, second}); err != nil {
				t.Fatal(err)
			}
			if count() != 3 || len(discoveryRows(t, st)) != 2 {
				t.Fatalf("retry did not persist: writes=%d", count())
			}
			if err := st.UpsertDiscoveredPeers(ctx, []DiscoveredPeer{first, second}); err != nil {
				t.Fatal(err)
			}
			if count() != 3 {
				t.Fatal("duplicate batch wrote rows")
			}
		})
	}
	// 空批次无需数据库；校验先于开始事务，即使数据库关闭也返回参数错误。
	st := openTestStore(t)
	st.Close()
	if err := st.UpsertDiscoveredPeers(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertDiscoveredPeers(ctx, []DiscoveredPeer{discoveryTestPeer(), {}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("validation after db access: %v", err)
	}
}

func TestDiscoveredPeersBatchOrderedMergeAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "discovery.db")
	open := func() *Store {
		st, err := Open(path, Options{NodeID: testNodeID(1)})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Init(ctx); err != nil {
			t.Fatal(err)
		}
		return st
	}
	st := open()
	peer := discoveryTestPeer()
	peer.State = "connected"
	peer.ZeroMQCurveServerPublicKey = "key"
	connected := peer.LastSeenAt
	peer.LastConnectedAt = &connected
	later := peer
	later.ZeroMQCurveServerPublicKey = ""
	later.LastConnectedAt = nil
	later.State = "failed"
	later.LastError = "lost"
	later.Generation--
	if err := st.UpsertDiscoveredPeers(ctx, []DiscoveredPeer{peer, later}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st = open()
	defer st.Close()
	rows := discoveryRows(t, st)
	if len(rows) != 1 || rows[0].State != "failed" || rows[0].LastError != "lost" || rows[0].Generation != peer.Generation || rows[0].ZeroMQCurveServerPublicKey != "key" || rows[0].LastConnectedAt == nil || *rows[0].LastConnectedAt != connected {
		t.Fatalf("reopen lost merge: %+v", rows)
	}
}
