package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tursom/turntf/internal/clock"
)

// BenchmarkDiscoveredPeerPersistence 可单文件 overlay 到
// /tmp/turntf-idle-cpu/baseline；只通过局部接口断言选择批量 API，旧版逐条写入。
// trigger 直接统计已提交的 INSERT/UPDATE 行；计数查询和断言不在计时区间。
// 每次输入明确使用同一 lastSeen，不受校准轮数、HLC logical 或耗时影响。
// 本会话只编译此基准，由主会话顺序执行，避免污染其他 CPU 测量。
func BenchmarkDiscoveredPeerPersistence(b *testing.B) {
	for _, scenario := range []struct {
		name    string
		peers   int
		changed bool
	}{
		{"duplicate_advertisement", 1, false},
		{"critical_field_changes", 1, true},
		{"duplicate_batch", 64, false},
		{"changed_batch", 64, true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			ctx := context.Background()
			st, err := Open(filepath.Join(b.TempDir(), "discovery.db"), Options{NodeID: 4096})
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			if err := st.Init(ctx); err != nil {
				b.Fatal(err)
			}
			if _, err := st.db.Exec(`
CREATE TABLE discovery_bench_writes(n INTEGER NOT NULL);
INSERT INTO discovery_bench_writes VALUES(0);
CREATE TRIGGER discovery_bench_insert AFTER INSERT ON discovered_peers BEGIN UPDATE discovery_bench_writes SET n=n+1; END;
CREATE TRIGGER discovery_bench_update AFTER UPDATE ON discovered_peers BEGIN UPDATE discovery_bench_writes SET n=n+1; END;
`); err != nil {
				b.Fatal(err)
			}
			batch, optimized := any(st).(interface {
				UpsertDiscoveredPeers(context.Context, []DiscoveredPeer) error
			})
			write := func(peers []DiscoveredPeer) error {
				if optimized {
					return batch.UpsertDiscoveredPeers(ctx, peers)
				}
				for _, peer := range peers {
					if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
						return err
					}
				}
				return nil
			}
			peers := make([]DiscoveredPeer, scenario.peers)
			stamp := clock.Timestamp{WallTimeMs: 1740000000000, NodeID: 4096}
			for i := range peers {
				peers[i] = DiscoveredPeer{NodeID: int64(8192 + i), URL: fmt.Sprintf("ws://peer-%d.example/internal/cluster/ws", i), State: "connected", SourcePeerNodeID: 12288, FirstSeenAt: stamp, LastSeenAt: stamp, LastConnectedAt: &stamp, Generation: 1, ZeroMQCurveServerPublicKey: "key-one"}
			}
			if err := write(peers); err != nil {
				b.Fatal(err)
			}
			if _, err := st.db.Exec(`UPDATE discovery_bench_writes SET n=0`); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := range peers {
					peers[j].Generation = uint64(i + 2)
					if scenario.changed {
						// 每轮改变一种关键字段，所有输入均应立即持久化。
						switch i % 4 {
						case 0:
							peers[j].SourcePeerNodeID++
						case 1:
							peers[j].LastError = fmt.Sprintf("failure-%d", i)
						case 2:
							peers[j].ZeroMQCurveServerPublicKey = fmt.Sprintf("key-%d", i)
						case 3:
							connected := stamp
							connected.WallTimeMs += int64(i + 1)
							peers[j].LastConnectedAt = &connected
						}
					}
				}
				if err := write(peers); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			var writes int64
			if err := st.db.QueryRow(`SELECT n FROM discovery_bench_writes`).Scan(&writes); err != nil {
				b.Fatal(err)
			}
			want := int64(b.N) * int64(len(peers))
			if optimized && !scenario.changed {
				want = 0
			}
			if writes != want {
				b.Fatalf("actual SQL row writes=%d want=%d", writes, want)
			}
			b.ReportMetric(float64(writes)/float64(b.N), "sql-row-writes/op")
			b.ReportMetric(float64(len(peers)), "peers/op")
		})
	}
}
