package api

import (
	"bytes"
	"context"
	"testing"

	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

// 模拟节点有普通持久会话、当前消息没有任何本地候选（含管理员）的场景。
// 夹具在计时前建立；同一基准也可在修改前的实现上运行。
func BenchmarkPersistentDispatchEmptyCandidates(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"256B", 256}, {"4KiB", 4 << 10}, {"64KiB", 64 << 10}} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			a, closeAPI := openBenchmarkAuthenticatedTestAPI(b, benchroot.Modes(b)[0], apiBenchmarkEngineScenarios()[0])
			b.Cleanup(closeAPI)
			online, _, err := a.http.service.CreateUser(ctx, store.CreateUserParams{
				Username: "empty-dispatch-online-user", PasswordHash: "!", Role: store.RoleUser,
			})
			if err != nil {
				b.Fatal(err)
			}
			conn := &persistentPayloadCaptureConn{}
			session := newPersistentPayloadCaptureSession(a.http, online, conn)
			if err := a.http.persistentSessions.Register(ctx, session); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { a.http.persistentSessions.Unregister(session) })
			key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
			message := store.Message{
				Recipient: key, Sender: key, NodeID: key.NodeID, Seq: 1,
				Body: bytes.Repeat([]byte("m"), tc.size), CreatedAt: a.http.service.store.Clock().Now(),
			}
			plan, err := a.http.persistentCandidatesForMessage(ctx, message)
			if err != nil || len(plan.candidates) != 0 || !a.http.hasPersistentSessions() {
				b.Fatalf("invalid empty-candidate fixture: candidates=%d err=%v", len(plan.candidates), err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := a.http.dispatchPersistentMessage(ctx, int64(i+1), message); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if conn.payloadCount() != 0 {
				b.Fatal("unrelated online user received a message")
			}
		})
	}
}
