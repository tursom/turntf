package cluster

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/store"
)

// 本文件只用既有接口，可 overlay 到本轮开始前的工作树快照。
// 测量真实 publishLoop：包含准备、入桶和送入会话队列，不包含网络和存储写入。
func benchmarkPublishLoop(b *testing.B) (*Manager, *session) {
	b.Helper()
	m := newReplicationTestManager(b, newReplicationTestStore(b, "publish-cpu", 1))
	s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.wg.Add(1)
	go m.publishLoop()
	b.Cleanup(func() { m.cancel(); m.wg.Wait() })
	return m, s
}

func BenchmarkPeriodicCatchupCursors(b *testing.B) {
	for _, size := range []int{1, 6} {
		b.Run(fmt.Sprintf("peers-%d-origins-%d", size, size), func(b *testing.B) {
			m := newReplicationTestManager(b, newReplicationTestStore(b, "catchup-cpu", 2))
			sessions := make([]*session, size)
			for i := range sessions {
				sessions[i] = readySnapshotTestSession(m, testNodeID(uint16(i+10)), store.DefaultMessageWindowSize)
			}
			for i := 0; i < size; i++ {
				origin := testNodeID(uint16(i+100))
				if err := m.store.RecordOriginApplied(context.Background(), origin, 100); err != nil {
					b.Fatal(err)
				}
				for _, s := range sessions {
					s.noteRemoteOriginEvent(origin, 100)
				}
			}
			// 在计时外选择入口：基线运行原来的逐 peer 路径，候选运行正式共享入口。
			// interface 允许同一文件编译到没有新 helper 的基线，不额外复制业务逻辑。
			round := func() error {
				for _, s := range sessions {
					if _, err := m.requestCatchupIfNeeded(s); err != nil {
						return err
					}
				}
				return nil
			}
			if periodic, ok := any(m).(interface { requestPeriodicCatchup([]*session) error }); ok {
				round = func() error { return periodic.requestPeriodicCatchup(sessions) }
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := round(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPublishLoopSparseLatency(b *testing.B) {
	b.StopTimer()
	m, s := benchmarkPublishLoop(b)
	event := batcherTestMessageEvent(testNodeID(1), 1, 1, 256)
	latency := make([]time.Duration, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		event.Sequence, event.EventID = int64(i+1), int64(i+1)
		m.Publish(event)
		select {
		case envelope := <-s.send:
			if batch := envelope.GetEventBatch(); batch == nil || len(batch.Events) != 1 {
				b.Fatalf("invalid sparse delivery: %+v", envelope)
			}
		case <-time.After(time.Second):
			b.Fatal("isolated event stalled")
		}
		latency[i] = time.Since(started)
	}
	b.StopTimer()
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	for _, p := range []struct {
		n    int
		unit string
	}{{95, "publish_p95_us/op"}, {99, "publish_p99_us/op"}} {
		index := (b.N*p.n+99)/100 - 1
		b.ReportMetric(float64(latency[index])/float64(time.Microsecond), p.unit)
	}
}

func BenchmarkPublishLoopFullBatches(b *testing.B) {
	b.StopTimer()
	m, s := benchmarkPublishLoop(b)
	event := batcherTestMessageEvent(testNodeID(1), 1, 1, 256)
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < maxBatchEvents; j++ {
			event.Sequence, event.EventID = int64(i*maxBatchEvents+j+1), int64(i*maxBatchEvents+j+1)
			m.Publish(event)
		}
		count := 0
		for count < maxBatchEvents {
			select {
			case envelope := <-s.send:
				batch := envelope.GetEventBatch()
				if batch == nil || len(batch.Events) == 0 {
					b.Fatalf("invalid full batch: %+v", envelope)
				}
				count += len(batch.Events)
			case <-time.After(time.Second):
				b.Fatal("full batch stalled")
			}
		}
		if count != maxBatchEvents {
			b.Fatalf("delivered %d events, want %d", count, maxBatchEvents)
		}
	}
	b.StopTimer()
	b.ReportMetric(maxBatchEvents, "events/op")
}
