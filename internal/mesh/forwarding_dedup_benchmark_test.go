package mesh

import (
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"
)

// 本文件可单独 overlay 到 6b9a81e：只使用该基线已有的 Engine 字段和方法。
// 所有填充均走 markSeen；不能直接填 seen，否则候选的到期索引不会建立。
// 使用虚拟 UTC 时间，无 sleep。性能运行由主会话统一安排。
func meshDedupBenchEngine(now *time.Time) *Engine {
	e := NewEngine(1, nil, nil, nil, nil, nil)
	e.now = func() time.Time { return *now }
	return e
}

func meshDedupBenchFill(b *testing.B, e *Engine, count int) {
	b.Helper()
	p := ForwardedPacket{SourceNodeId: 1, SourceRuntimeEpoch: 7}
	for i := 1; i <= count; i++ {
		p.PacketId = uint64(i)
		if !e.markSeen(&p) {
			b.Fatal("fill rejected a fresh key")
		}
	}
}

// 有界批次隔离纯插入成本，避免 b.N 大时 seen 无限增长；建 Engine 不计时，
// map/索引的实际增长、分配和（回拨时的）堆维护均计时。每 op 为一次 mark。
func BenchmarkMeshDedupInsert(b *testing.B) {
	for _, rollback := range []bool{false, true} {
		name := "ordered"
		if rollback {
			name = "rollback"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for done := 0; done < b.N; {
				b.StopTimer()
				now := time.Unix(1000, 0).UTC()
				e := meshDedupBenchEngine(&now)
				p := ForwardedPacket{SourceNodeId: 1, SourceRuntimeEpoch: 7}
				count := min(16384, b.N-done)
				b.StartTimer()
				for i := 0; i < count; i++ {
					p.PacketId = uint64(i + 1)
					if !e.markSeen(&p) {
						b.Fatal("insert rejected")
					}
					if rollback {
						now = now.Add(-time.Nanosecond)
					}
				}
				done += count
				runtime.KeepAlive(e)
			}
		})
	}
}

// 每次都触发原清理条件，但所有条目均未过期。重复查询避免测试本身
// 增加 live/stale 索引；这里只隔离空清理，不能单独据此决定保留候选。
func BenchmarkMeshDedupNoExpirySweep(b *testing.B) {
	for _, size := range []int{1, 1024, 16384, 262144} {
		b.Run(fmt.Sprintf("seen=%d", size), func(b *testing.B) {
			now := time.Unix(1000, 0).UTC()
			e := meshDedupBenchEngine(&now)
			meshDedupBenchFill(b, e, size)
			p := ForwardedPacket{SourceNodeId: 1, SourceRuntimeEpoch: 7, PacketId: 1}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.nextSeenSweepAt = time.Time{}
				if e.markSeen(&p) {
					b.Fatal("live duplicate accepted")
				}
			}
		})
	}
}

// 延迟仅保留前 8192 次样本，最大值覆盖全部采样点；避免基准自身无限占用内存。
// 计时器有固定开销，延迟用于比较锁内集中清理，吞吐用未逐条采样的 Insert。
type meshDedupBenchLatency struct {
	samples []int64
	max     int64
}

func (l *meshDedupBenchLatency) add(elapsed time.Duration) {
	ns := elapsed.Nanoseconds()
	if ns > l.max {
		l.max = ns
	}
	if len(l.samples) < cap(l.samples) {
		l.samples = append(l.samples, ns)
	}
}

func (l *meshDedupBenchLatency) report(b *testing.B) {
	if len(l.samples) == 0 {
		return
	}
	sort.Slice(l.samples, func(i, j int) bool { return l.samples[i] < l.samples[j] })
	b.ReportMetric(float64(l.samples[(len(l.samples)-1)*50/100]), "sweep-p50-ns")
	b.ReportMetric(float64(l.samples[(len(l.samples)-1)*99/100]), "sweep-p99-ns")
	b.ReportMetric(float64(l.max), "sweep-max-ns")
}

// 每 op 固定模拟 120 秒、1000 包/秒，跨四个默认 TTL 窗口，避免自适应
// b.N 太小而没覆盖到过期。ns/op 是整个窗口，ns/mark 便于看每次插入成本。
// 每个实际清理触发点采样一次延迟，包含 markSeen 的锁及完整清理成本。
func BenchmarkMeshDedupSustained(b *testing.B) {
	const packets = 120000
	for _, churn := range []bool{false, true} {
		name := "steady"
		if churn {
			name = "unmark-reinsert"
		}
		b.Run(name, func(b *testing.B) {
			latency := meshDedupBenchLatency{samples: make([]int64, 0, 8192)}
			marks := 0
			b.ReportAllocs()
			b.ResetTimer()
			for window := 0; window < b.N; window++ {
				now := time.Unix(1000, 0).UTC()
				e := meshDedupBenchEngine(&now)
				p := ForwardedPacket{SourceNodeId: 1, SourceRuntimeEpoch: 7}
				for i := 0; i < packets; i++ {
					p.PacketId = uint64(i + 1)
					// 判断仅用于采样，不改变 markSeen 内部的清理时点。
					sweep := e.nextSeenSweepAt.IsZero() || !now.Before(e.nextSeenSweepAt)
					var started time.Time
					if sweep {
						started = time.Now()
					}
					if !e.markSeen(&p) {
						b.Fatal("fresh sustained insert rejected")
					}
					if sweep {
						latency.add(time.Since(started))
					}
					marks++
					if churn && i%4 == 0 {
						e.unmarkSeen(&p)
						now = now.Add(time.Nanosecond)
						if !e.markSeen(&p) {
							b.Fatal("reinsert rejected")
						}
						marks++
					}
					now = now.Add(time.Millisecond)
				}
				runtime.KeepAlive(e)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(marks), "ns/mark")
			latency.report(b)
		})
	}
}

// 每 op 是一次集中到期调用；填充不计时，但必须通过 markSeen 建立索引。
// 包含最后一次插入，报告 ns/op、每条到期记录成本及 p99/max，暴露锁内长暂停。
func BenchmarkMeshDedupExpiryBurst(b *testing.B) {
	for _, size := range []int{1024, 16384, 262144} {
		b.Run(fmt.Sprintf("seen=%d", size), func(b *testing.B) {
			latency := meshDedupBenchLatency{samples: make([]int64, 0, 8192)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				now := time.Unix(1000, 0).UTC()
				e := meshDedupBenchEngine(&now)
				meshDedupBenchFill(b, e, size)
				now = now.Add(e.seenTTL + time.Nanosecond)
				p := ForwardedPacket{SourceNodeId: 1, SourceRuntimeEpoch: 7, PacketId: 1}
				b.StartTimer()
				started := time.Now()
				fresh := e.markSeen(&p)
				elapsed := time.Since(started)
				b.StopTimer()
				latency.add(elapsed)
				if !fresh || len(e.seen) != 1 {
					b.Fatalf("burst failed: fresh=%v entries=%d", fresh, len(e.seen))
				}
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/expired")
			latency.report(b)
		})
	}
}

// 显式 GC 后的 HeapAlloc 差值量化总持有内存（包含 map/索引/分配器取整），
// 与基线相减才是额外索引成本。每 op 重建一次表；ns/op 包含 GC，不作吞吐判断。
// churn 展示已撤销索引的保留成本；rollback 展示乱序堆的额外成本。
// HeapAlloc 是进程级指标，应单独、顺序运行此基准，勿与其他负载并发。
func BenchmarkMeshDedupRetainedMemory(b *testing.B) {
	for _, size := range []int{1024, 16384, 262144} {
		for _, mode := range []string{"ordered", "unmark-reinsert", "rollback"} {
			b.Run(fmt.Sprintf("seen=%d/%s", size, mode), func(b *testing.B) {
				var retained int64
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					now := time.Unix(1000, 0).UTC()
					e := meshDedupBenchEngine(&now)
					p := ForwardedPacket{SourceNodeId: 1, SourceRuntimeEpoch: 7}
					for i := 0; i < size; i++ {
						p.PacketId = uint64(i + 1)
						if !e.markSeen(&p) {
							b.Fatal("memory fill rejected")
						}
						if mode == "unmark-reinsert" {
							e.unmarkSeen(&p)
							if !e.markSeen(&p) {
								b.Fatal("memory reinsert rejected")
							}
						}
						if mode == "rollback" {
							now = now.Add(-time.Nanosecond)
						}
					}
					runtime.GC()
					runtime.ReadMemStats(&after)
					retained += int64(after.HeapAlloc) - int64(before.HeapAlloc)
					runtime.KeepAlive(e)
				}
				b.ReportMetric(float64(retained)/float64(b.N), "retained-B")
				b.ReportMetric(float64(retained)/float64(b.N)/float64(size), "retained-B/live-key")
			})
		}
	}
}
