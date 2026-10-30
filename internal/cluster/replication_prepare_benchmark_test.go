package cluster

import (
	"fmt"
	"testing"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// BenchmarkReplicationPrepareFanout 可单文件 overlay 到 6b9a81e：只使用该基线已有的
// Manager、session、queuePublishedEvent 和 flushReplicationBatches，不依赖准备 helper
// 或本次新增的功能测试。每 op 发布 32 个事件并排空各 peer 的发送队列；计时覆盖
// 准备、分桶、阈值刷新、分发和排空，不含 fixture 创建、网络编码或存储。
// SetBytes 的吞吐字节数按原始事件 payload 计，不乘 peer 数；这是 CPU/分配微基准，端到端延迟
// 与吞吐仍需主会话通过实际传输路径测量。本实验会话只编译，不运行基准。
func BenchmarkReplicationPrepareFanout(b *testing.B) {
	for _, payload := range []int{128, 4096, 40 << 10} {
		for _, peerCount := range []int{0, 1, 4, 16, 64} {
			b.Run(fmt.Sprintf("payload=%d/peers=%d", payload, peerCount), func(b *testing.B) {
				mgr := &Manager{
					cfg:                Config{NodeID: 1},
					peers:              make(map[int64]*peerState, peerCount),
					replicationBatches: newReplicationBatcher(),
				}
				sessions := make([]*session, peerCount)
				for i := range sessions {
					sess := &session{peerID: int64(i + 2), send: make(chan *internalproto.Envelope, maxBatchEvents)}
					sessions[i] = sess
					mgr.peers[sess.peerID] = &peerState{active: sess}
				}
				events := make([]store.Event, maxBatchEvents)
				hlc := clock.NewClock(1)
				for i := range events {
					stamp := hlc.Now()
					events[i] = store.Event{
						Sequence: int64(i + 1), EventID: int64(i + 1), OriginNodeID: 1,
						EventType: store.EventTypeMessageCreated, Aggregate: "message", HLC: stamp,
						Body: &internalproto.MessageCreatedEvent{
							Recipient: &internalproto.ClusterUserRef{NodeId: 1, UserId: 1},
							Sender:    &internalproto.ClusterUserRef{NodeId: 1, UserId: 2},
							NodeId:    1, Seq: int64(i + 1), Body: make([]byte, payload), CreatedAtHlc: stamp.String(),
						},
					}
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(events) * payload))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, event := range events {
						mgr.queuePublishedEvent(event)
					}
					// 大 payload 可能按字节提前刷新；尾批仍走原关闭刷新入口。
					mgr.flushReplicationBatches()
					for _, sess := range sessions {
						count := 0
						for len(sess.send) > 0 {
							count += len((<-sess.send).GetEventBatch().Events)
						}
						if count != len(events) {
							b.Fatalf("peer %d: dispatched %d events, want %d", sess.peerID, count, len(events))
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(events)), "events/op")
			})
		}
	}
}
