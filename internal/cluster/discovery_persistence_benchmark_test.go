package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// BenchmarkDiscoveryMembershipPersistence 可单文件复制到用户指定 baseline。
// 计时包含广告验证、运行态合并、持久化和事务提交，不包含 fixture/计数查询。
// 分别覆盖同来源重复广告、真实 source 变化、64 peer 批量更新、同轮重复广告。
// SQL trigger 验证真实行写入；baseline 没有批量方法时也能编译并验证旧行为。
// 不在本会话执行；主会话可顺序比较两棵树。
func BenchmarkDiscoveryMembershipPersistence(b *testing.B) {
	silenceClusterLogs(b)
	for _, scenario := range []struct {
		name          string
		peers, copies int
		changed       bool
	}{
		{"duplicate_advertisement", 1, 1, false},
		{"source_changes", 1, 1, true},
		{"duplicate_batch", 64, 1, false},
		{"changed_batch", 64, 1, true},
		{"same_round_merge", 16, 4, true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			ctx := context.Background()
			path := filepath.Join(b.TempDir(), "membership.db")
			st, err := store.Open(path, store.Options{NodeID: 4096})
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			if err := st.Init(ctx); err != nil {
				b.Fatal(err)
			}
			db, err := sql.Open("turntf-sqlite3", path)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`
CREATE TABLE discovery_membership_bench_writes(n INTEGER NOT NULL);
INSERT INTO discovery_membership_bench_writes VALUES(0);
CREATE TRIGGER discovery_membership_bench_insert AFTER INSERT ON discovered_peers BEGIN UPDATE discovery_membership_bench_writes SET n=n+1; END;
CREATE TRIGGER discovery_membership_bench_update AFTER UPDATE ON discovered_peers BEGIN UPDATE discovery_membership_bench_writes SET n=n+1; END;
`); err != nil {
				b.Fatal(err)
			}
			m, err := NewManager(Config{NodeID: 4096, AdvertisePath: websocketPath, ClusterSecret: "secret", MessageWindowSize: store.DefaultMessageWindowSize, MaxClockSkewMs: DefaultMaxClockSkewMs}, st)
			if err != nil {
				b.Fatal(err)
			}
			update := &internalproto.MembershipUpdate{}
			for j := 0; j < scenario.peers; j++ {
				for k := 0; k < scenario.copies; k++ {
					update.Peers = append(update.Peers, &internalproto.PeerAdvertisement{NodeId: int64(8192 + j), Url: fmt.Sprintf("ws://peer-%d.example/internal/cluster/ws", j), Generation: uint64(k + 1)})
				}
			}
			started := time.Now()
			if err := m.handleMembershipUpdateBody(12288, update); err != nil {
				b.Fatal(err)
			}
			// 以 connected 起步，覆盖重复广告不能重新伪造连接时间的情况。
			for _, peer := range m.discoveredPeers {
				m.recordDiscoveredState(peer.nodeID, peer.url, discoveryStateConnected, "", true)
			}
			if _, err := db.Exec(`UPDATE discovery_membership_bench_writes SET n=0`); err != nil {
				b.Fatal(err)
			}
			_, optimized := any(st).(interface {
				UpsertDiscoveredPeers(context.Context, []store.DiscoveredPeer) error
			})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, advertisement := range update.Peers {
					advertisement.Generation = uint64(i + scenario.copies + 1)
				}
				source := int64(12288)
				if scenario.changed {
					source += int64(i%2 + 1)
				}
				if err := m.handleMembershipUpdateBody(source, update); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			var writes int64
			if err := db.QueryRow(`SELECT n FROM discovery_membership_bench_writes`).Scan(&writes); err != nil {
				b.Fatal(err)
			}
			if !optimized {
				want := int64(b.N) * int64(len(update.Peers))
				if writes != want {
					b.Fatalf("baseline SQL row writes=%d want=%d", writes, want)
				}
			} else if scenario.changed {
				want := int64(b.N) * int64(scenario.peers)
				if writes != want {
					b.Fatalf("SQL row writes=%d want=%d", writes, want)
				}
			} else {
				// 正常校准轮没有写入；长于一分钟时允许每 peer 每分钟刷新一次。
				limit := int64(scenario.peers) * int64(time.Since(started)/time.Minute)
				if writes > limit {
					b.Fatalf("duplicate advertisements wrote %d rows (minute limit %d)", writes, limit)
				}
			}
			if m.discoveryPersistFailures != 0 {
				b.Fatalf("persistence failures=%d", m.discoveryPersistFailures)
			}
			for _, peer := range m.discoveredPeers {
				if peer.generation != uint64(b.N+scenario.copies) {
					b.Fatalf("runtime generation=%d", peer.generation)
				}
			}
			b.ReportMetric(float64(writes)/float64(b.N), "sql-row-writes/op")
			b.ReportMetric(float64(len(update.Peers)), "advertisements/op")
		})
	}
}
