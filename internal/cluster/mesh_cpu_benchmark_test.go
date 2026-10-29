package cluster

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"
)

// BenchmarkMeshTransientPointToPointLatency 与吞吐基准使用同一条七节点转发路径，
// 每次等待目标校验完成后再发送，分别报告平均值和分位数，避免只用并行 ns/op
// 推断单条消息的端到端延迟。采样数组在计时前分配，不计入每次投递的分配量。
func BenchmarkMeshTransientPointToPointLatency(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "256B", size: 256},
		{name: "4KiB", size: 4 << 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			silenceClusterLogs(b)
			topology := buildTransientPointToPointWebSocketLinear7NodesTopology(b)
			b.Cleanup(topology.Close)
			ctx := context.Background()
			payload := bytes.Repeat([]byte("t"), tc.size)
			runClusterBenchmarkWarmup(b, func() {
				runTransientPointToPointBenchmarkDelivery(b, ctx, topology, payload, 1)
			})
			latencies := make([]time.Duration, b.N)
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				runTransientPointToPointBenchmarkDelivery(b, ctx, topology, payload, uint64(i)+2)
				latencies[i] = time.Since(started)
			}
			b.StopTimer()
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			// nearest-rank 分位数；b.N 始终大于零。
			for _, percentile := range []struct {
				percent int
				metric  string
			}{
				{50, "delivery_p50_us/op"},
				{95, "delivery_p95_us/op"},
				{99, "delivery_p99_us/op"},
			} {
				index := (b.N*percentile.percent+99)/100 - 1
				b.ReportMetric(float64(latencies[index])/float64(time.Microsecond), percentile.metric)
			}
		})
	}
}
