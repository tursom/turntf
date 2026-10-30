package cluster

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestReplicationBatchDeadlinesPreserveEachFirstEnqueue(t *testing.T) {
	b := newReplicationBatcher()
	now := time.Now()
	if !b.nextFlushDeadline().IsZero() {
		t.Fatal("empty batcher scheduled work")
	}
	event := batcherTestMessageEvent(testNodeID(1), 1, 1, 16)
	b.enqueue(testNodeID(2), event, now)
	b.enqueue(testNodeID(3), event, now.Add(time.Millisecond))
	b.enqueue(testNodeID(2), event, now.Add(time.Millisecond))
	if got := b.nextFlushDeadline(); !got.Equal(now.Add(maxBatchDelay)) {
		t.Fatalf("later enqueue postponed the first batch: %v", got)
	}
	if got := b.flushDue(now.Add(maxBatchDelay)); len(got) != 1 || got[0].peerID != testNodeID(2) || len(got[0].events) != 2 {
		t.Fatalf("wrong first deadline flush: %+v", got)
	}
	if got := b.nextFlushDeadline(); !got.Equal(now.Add(time.Millisecond + maxBatchDelay)) {
		t.Fatalf("second batch lost its own deadline: %v", got)
	}
	b.flushAll()
	if !b.nextFlushDeadline().IsZero() {
		t.Fatal("drained batcher scheduled work")
	}
}

func TestPublishLoopFlushesIsolatedEventAndDrainsOnClose(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "deadline"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			m := newReplicationTestManager(t, newReplicationTestStore(t, "publish-deadline", 1))
			s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
			m.ctx, m.cancel = context.WithCancel(context.Background())
			done := make(chan struct{})
			m.wg.Add(1)
			t.Cleanup(func() { m.cancel(); <-done })
			m.publishCh <- batcherTestMessageEvent(testNodeID(1), 1, 1, 16)
			if shutdown {
				m.publishCh <- batcherTestMessageEvent(testNodeID(1), 2, 2, 16)
				m.cancel()
			}
			go func() { m.publishLoop(); close(done) }()
			if shutdown {
				select {
				case <-done:
					if len(m.publishCh) != 0 || len(m.replicationBatches.buckets) != 0 {
						t.Fatal("shutdown retained queued events or batches")
					}
				case <-time.After(time.Second):
					t.Fatal("shutdown did not drain the publish loop")
				}
				return
			}
			select {
			case envelope := <-s.send:
				batch := envelope.GetEventBatch()
				if batch == nil || len(batch.Events) != 1 {
					t.Fatalf("incomplete publish: %+v", envelope)
				}
			case <-time.After(time.Second):
				t.Fatal("isolated event was not flushed without further input")
			}
		})
	}
}

func TestSnapshotDeadlineTracksRequestsAndDigestBlockers(t *testing.T) {
	m := newReplicationTestManager(t, newReplicationTestStore(t, "snapshot-deadline", 1))
	s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
	now := time.Now()
	m.clearSnapshotDigestDirty(s, m.peers[s.peerID].snapshotDigestGeneration)
	if got := m.nextSnapshotWorkDeadline(now); !got.IsZero() {
		t.Fatalf("idle peer scheduled a scan: %v", got)
	}
	m.markSnapshotDigestDirty(s.peerID, false)
	m.mu.Lock()
	m.peers[s.peerID].lastSnapshotDigestAt = now
	m.mu.Unlock()
	if got := m.nextSnapshotWorkDeadline(now); !got.Equal(now.Add(snapshotDigestMinInterval)) {
		t.Fatalf("digest throttle changed: %v", got)
	}
	pull, _ := s.beginPendingPull(testNodeID(2), 0)
	if got := m.nextSnapshotWorkDeadline(now); !got.IsZero() {
		t.Fatalf("digest blocked by pull kept polling: %v", got)
	}
	kind := internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS
	token, _ := s.beginSnapshotRequestAt(store.SnapshotUsersPartition, kind, now)
	if got := m.nextSnapshotWorkDeadline(now); !got.Equal(now.Add(snapshotRequestTimeout)) {
		t.Fatalf("request timeout not scheduled while digest blocked: %v", got)
	}
	s.failSnapshotRequest(store.SnapshotUsersPartition, token, now)
	if got := m.nextSnapshotWorkDeadline(now); !got.Equal(now.Add(snapshotRetryBase)) {
		t.Fatalf("earlier failure retry not scheduled: %v", got)
	}
	s.completeSnapshotRequest(store.SnapshotUsersPartition)
	if got := m.nextSnapshotWorkDeadline(now); !got.IsZero() {
		t.Fatalf("completed request retained deadline: %v", got)
	}
	s.cancelPendingPull(testNodeID(2), pull)
	if got := m.nextSnapshotWorkDeadline(now); !got.Equal(now.Add(snapshotDigestMinInterval)) {
		t.Fatalf("released pull did not restore digest deadline: %v", got)
	}
	s.close()
	if got := m.nextSnapshotWorkDeadline(now); !got.IsZero() {
		t.Fatalf("closed session retained deadline: %v", got)
	}
}

func TestPublishLoopBurstKeepsOrderAndBatchLimit(t *testing.T) {
	m := newReplicationTestManager(t, newReplicationTestStore(t, "publish-burst", 1))
	s := readySnapshotTestSession(m, testNodeID(3), store.DefaultMessageWindowSize)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	for i := 0; i < 2*maxBatchEvents; i++ {
		m.publishCh <- batcherTestMessageEvent(testNodeID(1), int64(i+1), int64(i+1), 16)
	}
	done := make(chan struct{})
	m.wg.Add(1)
	go func() { m.publishLoop(); close(done) }()
	t.Cleanup(func() { m.cancel(); <-done })
	for batch := 0; batch < 2; batch++ {
		select {
		case envelope := <-s.send:
			events := envelope.GetEventBatch().GetEvents()
			if len(events) != maxBatchEvents {
				t.Fatalf("burst batch size = %d, want %d", len(events), maxBatchEvents)
			}
			for i, event := range events {
				if event.EventId != int64(batch*maxBatchEvents+i+1) {
					t.Fatalf("burst reordered event: %+v", event)
				}
			}
		case <-time.After(time.Second):
			t.Fatal("burst did not flush without another publication")
		}
	}
}

func TestSnapshotNewDirtySurvivesEarlierSendCompletion(t *testing.T) {
	m := newReplicationTestManager(t, newReplicationTestStore(t, "snapshot-dirty", 1))
	s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
	m.markSnapshotDigestDirty(s.peerID, false)
	oldGeneration := m.peers[s.peerID].snapshotDigestGeneration
	// 模拟摘要构建/发送期间完成了修复；旧发送不能确认这次新标记。
	m.markSnapshotDigestDirty(s.peerID, true)
	m.clearSnapshotDigestDirty(s, oldGeneration)
	if dirty, immediate := snapshotDigestState(m, s.peerID); !dirty || !immediate {
		t.Fatal("old send cleared newer repair notification")
	}
	m.clearSnapshotDigestDirty(s, m.peers[s.peerID].snapshotDigestGeneration)
	if dirty, immediate := snapshotDigestState(m, s.peerID); dirty || immediate {
		t.Fatal("current send did not clear its own notification")
	}
	m.markSnapshotDigestDirty(s.peerID, false)
	oldGeneration = m.peers[s.peerID].snapshotDigestGeneration
	replacement := m.newSession(testLegacyTransportConn{}, m.cfg.NodeID < s.peerID, nil)
	replacement.peerID = s.peerID
	replacement.remoteSnapshotVersion = internalproto.SnapshotVersion
	replacement.replicationReady = true
	if !m.activateSession(replacement) {
		t.Fatal("replacement session was not activated")
	}
	m.clearSnapshotDigestDirty(s, oldGeneration)
	if dirty, _ := snapshotDigestState(m, s.peerID); !dirty {
		t.Fatal("old session cleared replacement session work")
	}
}

func TestSnapshotLoopSleepsWhileClockBlockedAndWakesOnRecovery(t *testing.T) {
	m := newReplicationTestManager(t, newReplicationTestStore(t, "snapshot-clock-wake", 1))
	s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
	m.markSnapshotDigestDirty(s.peerID, false)
	m.mu.Lock()
	m.setPeerClockStateLocked(s, m.peers[s.peerID], clockStateRejected, "test rejected")
	m.mu.Unlock()
	if got := m.nextSnapshotWorkDeadline(time.Now()); !got.IsZero() {
		t.Fatalf("clock-blocked work kept polling: %v", got)
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	done := make(chan struct{})
	m.wg.Add(1)
	go func() { m.snapshotDigestLoop(); close(done) }()
	t.Cleanup(func() { m.cancel(); <-done })
	select {
	case sent := <-s.send:
		t.Fatalf("digest bypassed clock protection: %+v", sent)
	case <-time.After(10 * time.Millisecond):
	}
	m.mu.Lock()
	m.setPeerClockStateLocked(s, m.peers[s.peerID], clockStateTrusted, "test recovered")
	m.mu.Unlock()
	select {
	case sent := <-s.send:
		if sent.GetSnapshotDigest() == nil {
			t.Fatalf("expected recovered digest: %+v", sent)
		}
	case <-time.After(time.Second):
		t.Fatal("clock recovery did not wake digest loop")
	}
}

func TestSnapshotLoopWakesAfterPullCompletion(t *testing.T) {
	m := newReplicationTestManager(t, newReplicationTestStore(t, "snapshot-wake", 1))
	s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
	pull, _ := s.beginPendingPull(testNodeID(2), 0)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	done := make(chan struct{})
	m.wg.Add(1)
	go func() { m.snapshotDigestLoop(); close(done) }()
	t.Cleanup(func() { m.cancel(); <-done })
	m.markSnapshotDigestDirty(s.peerID, false)
	select {
	case sent := <-s.send:
		t.Fatalf("digest bypassed pending pull: %+v", sent)
	case <-time.After(10 * time.Millisecond):
	}
	if !s.completePendingPull(testNodeID(2), pull) {
		t.Fatal("pull did not complete")
	}
	select {
	case sent := <-s.send:
		if sent.GetSnapshotDigest() == nil {
			t.Fatalf("expected digest: %+v", sent)
		}
	case <-time.After(time.Second):
		t.Fatal("pull completion did not wake sleeping digest loop")
	}
}

func TestSnapshotLoopRetriesRequestWithoutDirtyDigest(t *testing.T) {
	m := newReplicationTestManager(t, newReplicationTestStore(t, "snapshot-retry-wake", 1))
	s := readySnapshotTestSession(m, testNodeID(2), store.DefaultMessageWindowSize)
	m.clearSnapshotDigestDirty(s, m.peers[s.peerID].snapshotDigestGeneration)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	done := make(chan struct{})
	m.wg.Add(1)
	go func() { m.snapshotDigestLoop(); close(done) }()
	t.Cleanup(func() { m.cancel(); <-done })
	token, _ := s.beginSnapshotRequestAt(store.SnapshotUsersPartition, internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS, time.Now())
	// 提前到即将到期的退避点，验证新请求能唤醒原本没有 timer 的循环。
	s.failSnapshotRequest(store.SnapshotUsersPartition, token, time.Now().Add(-snapshotRetryBase+10*time.Millisecond))
	select {
	case sent := <-s.send:
		if chunk := sent.GetSnapshotChunk(); chunk == nil || !chunk.Request {
			t.Fatalf("expected retry request: %+v", sent)
		}
	case <-time.After(time.Second):
		t.Fatal("clean peer lost request retry deadline")
	}
}
