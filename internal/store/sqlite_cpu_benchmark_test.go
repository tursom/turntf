package store

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/tursom/turntf/internal/clock"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

// 本文件仅使用 6b9a81e 已有接口和 fixture，能够单独 overlay 到基线。
// Self/Direct 分别覆盖一次/两次用户读取，窗口预填后每个事务均发生真实裁剪。
func BenchmarkSQLiteCPUCreateMessage(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		b.Run(mode.Name(), func(b *testing.B) {
			for _, direct := range []bool{false, true} {
				for _, profileBytes := range []int{0, 16 << 10} {
					b.Run(fmt.Sprintf("direct-%t/profile-%d", direct, profileBytes), func(b *testing.B) {
						ctx := context.Background()
						st, cleanup := openBenchmarkStore(b, mode, storeBenchmarkEngineScenario{engine: EngineSQLite}, "sqlite-cpu-create", 1, 128, DefaultEventLogMaxEventsPerOrigin)
						b.Cleanup(cleanup)
						profile := `{"data":"` + strings.Repeat("p", profileBytes) + `"}`
						owner, _, err := st.CreateUser(ctx, CreateUserParams{Username: "owner", PasswordHash: "hash", Profile: profile})
						if err != nil {
							b.Fatal(err)
						}
						sender := owner.Key()
						if direct {
							user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "sender", PasswordHash: "hash", Profile: profile})
							if err != nil {
								b.Fatal(err)
							}
							sender = user.Key()
						}
						params := CreateMessageParams{UserKey: owner.Key(), Sender: sender, Body: bytes.Repeat([]byte("m"), 256)}
						write := func() {
							if _, _, err := st.CreateMessage(ctx, params); err != nil {
								b.Fatal(err)
							}
						}
						for i := 0; i < 129; i++ {
							write()
						}
						b.ReportAllocs()
						b.SetBytes(int64(len(params.Body)))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							write()
						}
					})
				}
			}
		})
	}
}

// 隔离投影写入与 trim，不含用户读取、事件编码和序列号分配。
// 固定 HLC、交错 producer 覆盖 node ASC/seq DESC 的混合排序。
func BenchmarkSQLiteCPUProjectionTrim(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		b.Run(mode.Name(), func(b *testing.B) {
			for _, window := range []int{128, 1024} {
				b.Run(fmt.Sprintf("window-%d", window), func(b *testing.B) {
					ctx := context.Background()
					st, cleanup := openBenchmarkStore(b, mode, storeBenchmarkEngineScenario{engine: EngineSQLite}, "sqlite-cpu-projection", 1, window, DefaultEventLogMaxEventsPerOrigin)
					b.Cleanup(cleanup)
					user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "projection", PasswordHash: "hash"})
					if err != nil {
						b.Fatal(err)
					}
					projection := st.backend.MessageProjection()
					message := Message{Recipient: user.Key(), Sender: user.Key(), Body: bytes.Repeat([]byte("m"), 256), CreatedAt: clock.Timestamp{WallTimeMs: 1740000000000, NodeID: 1}}
					var counter int64
					write := func() {
						counter++
						message.Seq = counter
						message.NodeID = 1 + counter%3
						if err := projection.ApplyMessageCreated(ctx, message); err != nil {
							b.Fatal(err)
						}
					}
					for i := 0; i <= window; i++ {
						write()
					}
					b.ReportAllocs()
					b.SetBytes(int64(len(message.Body)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						write()
					}
				})
			}
		})
	}
}
