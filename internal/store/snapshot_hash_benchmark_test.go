package store

import (
	"bytes"
	"context"
	"testing"

	clusterproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

func BenchmarkSnapshotHashRows(b *testing.B) {
	for _, tc := range []struct {
		name string
		rows int
		size int
	}{{"100-256B", 100, 256}, {"1000-256B", 1000, 256}, {"500-4KiB", 500, 4 << 10}} {
		b.Run(tc.name, func(b *testing.B) {
			rows := make([]*clusterproto.SnapshotRow, tc.rows)
			body := bytes.Repeat([]byte("m"), tc.size)
			for i := range rows {
				rows[i] = &clusterproto.SnapshotRow{Body: &clusterproto.SnapshotRow_Message{Message: &clusterproto.SnapshotMessageRow{
					Recipient: &clusterproto.ClusterUserRef{NodeId: 1, UserId: 2}, NodeId: 1, Seq: int64(i + 1),
					Body: body, CreatedAtHlc: "1700000000000-00000-0000000000000000001",
				}}}
			}
			b.ReportAllocs()
			b.SetBytes(int64(tc.rows * tc.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, err := hashSnapshotRows(rows)
				if err != nil || len(value) != 32 {
					b.Fatalf("invalid digest: %x %v", value, err)
				}
			}
		})
	}
}

func BenchmarkSnapshotDigestCPU(b *testing.B) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		b.Run(engine, func(b *testing.B) {
			ctx := context.Background()
			st, closeStore := openBenchmarkStore(b, benchroot.Modes(b)[0], storeBenchmarkEngineScenario{
				engine: engine, pebbleProfile: PebbleProfileBalanced, pebbleMessageSyncMode: PebbleMessageSyncModeNoSync,
			}, "snapshot-cpu", 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
			b.Cleanup(closeStore)
			user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "snapshot-cpu-user", PasswordHash: "!", Role: RoleUser})
			if err != nil {
				b.Fatal(err)
			}
			body := bytes.Repeat([]byte("m"), 4<<10)
			for i := 0; i < DefaultMessageWindowSize; i++ {
				mustCreateBenchmarkMessage(b, st, ctx, user.Key(), body)
			}
			producers := []int64{st.NodeID()}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				digest, err := st.BuildSnapshotDigest(ctx, producers)
				if err != nil || len(digest.GetPartitions()) != 5 {
					b.Fatalf("invalid snapshot digest: %v %v", digest, err)
				}
			}
		})
	}
}
