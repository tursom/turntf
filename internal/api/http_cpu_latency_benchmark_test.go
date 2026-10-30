package api

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

// 比较同一 HTTP handler 路径的稳态写入及尾延迟；不包含真实网络往返。
// 先填满消息窗口，采样数组在计时前分配，避免把准备成本混入延迟和 B/op。
func BenchmarkHTTPCreateMessageCPULatency(b *testing.B) {
	for _, scenario := range []apiBenchmarkEngineScenario{
		{name: "sqlite", engine: store.EngineSQLite},
		{name: "pebble", engine: store.EnginePebble, pebbleProfile: store.PebbleProfileBalanced, pebbleMessageSyncMode: store.PebbleMessageSyncModeNoSync},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			a, closeAPI := openBenchmarkAuthenticatedTestAPI(b, benchroot.Modes(b)[0], scenario)
			b.Cleanup(closeAPI)
			admin := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
			token := benchmarkLoginToken(b, a.handler, admin, "root-password")
			key := benchmarkCreateUserAs(b, a.handler, token, "cpu-latency-user", "bench-password", store.RoleUser)
			body := bytes.Repeat([]byte("m"), 256)
			for i := 0; i < a.http.service.store.MessageWindowSize(); i++ {
				if _, _, err := a.http.service.CreateMessage(ctx, store.CreateMessageParams{UserKey: key, Sender: admin, Body: body}); err != nil {
					b.Fatal(err)
				}
			}
			headers := map[string]string{"Authorization": "Bearer " + token}
			latencies := make([]time.Duration, b.N)
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				mustBenchmarkHTTPCreateMessageAuthenticatedOnce(b, a.handler, headers, key, body)
				latencies[i] = time.Since(started)
			}
			b.StopTimer()
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			for _, percentile := range []struct {
				n    int
				name string
			}{{95, "write_p95_us/op"}, {99, "write_p99_us/op"}} {
				index := (b.N*percentile.n+99)/100 - 1
				b.ReportMetric(float64(latencies[index])/float64(time.Microsecond), percentile.name)
			}
		})
	}
}
