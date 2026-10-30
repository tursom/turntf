package api

import (
	"bytes"
	"testing"

	"github.com/tursom/turntf/internal/clock"
	"github.com/tursom/turntf/internal/store"
)

var persistentEncodingBenchmarkSink *encodedPersistentMessage

func BenchmarkPersistentMessageEncoding(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"256B", 256}, {"4KiB", 4 << 10}, {"64KiB", 64 << 10}} {
		b.Run(tc.name, func(b *testing.B) {
			message := store.Message{
				Recipient: store.UserKey{NodeID: 1, UserID: 2}, Sender: store.UserKey{NodeID: 1, UserID: 3},
				NodeID: 1, Seq: 1, Body: bytes.Repeat([]byte("m"), tc.size),
				CreatedAt: clock.Timestamp{WallTimeMs: 1700000000000, NodeID: 1},
			}
			b.ReportAllocs()
			b.SetBytes(int64(tc.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoded, err := encodePersistentMessage(message)
				if err != nil {
					b.Fatal(err)
				}
				persistentEncodingBenchmarkSink = encoded
			}
		})
	}
}
