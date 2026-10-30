package cluster

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestReplicationPrepareManagerFanout(t *testing.T) {
	mgr, sessions := replicationPrepareTestManager(3)
	events := []store.Event{
		batcherTestMessageEvent(1, 11, 101, 128),
		batcherTestMessageEvent(9, 12, 201, 256),
		batcherTestMessageEvent(1, 13, 102, 512),
	}
	var firstQueuedAt time.Time
	for i, event := range events {
		mgr.queuePublishedEvent(event)
		var shared *internalproto.ReplicatedEvent
		for _, sess := range sessions {
			batch := mgr.replicationBatches.buckets[replicationBatchKey{sess.peerID, event.OriginNodeID}]
			entry := batch.entries[len(batch.entries)-1]
			if shared == nil {
				shared = entry.replicated
			}
			if entry.replicated != shared {
				t.Fatal("peer buckets did not share the prepared protobuf")
			}
			want := store.ToReplicatedEvent(event)
			if !proto.Equal(entry.replicated, want) || entry.size != proto.Size(want) || entry.sentAtHLC != event.HLC.String() {
				t.Fatalf("prepared event changed wire data, size or HLC: %+v", entry)
			}
			if event.OriginNodeID == 1 {
				if firstQueuedAt.IsZero() {
					firstQueuedAt = batch.firstQueuedAt
				}
				if batch.firstQueuedAt != firstQueuedAt {
					t.Fatalf("peer fanout or later enqueue changed firstQueuedAt at event %d", i)
				}
			}
			wantBytes := 0
			for _, queued := range batch.entries {
				wantBytes += proto.Size(store.ToReplicatedEvent(queued.event))
			}
			if batch.totalBytes != wantBytes {
				t.Fatalf("peer %d: got %d charged bytes, want %d", sess.peerID, batch.totalBytes, wantBytes)
			}
		}
	}

	mgr.flushReplicationBatches()
	for _, sess := range sessions {
		if len(sess.send) != 2 {
			t.Fatalf("peer %d: expected one batch per origin, got %d", sess.peerID, len(sess.send))
		}
		for range 2 {
			envelope := <-sess.send
			batch := envelope.GetEventBatch()
			wantEvents := []store.Event{events[0], events[2]}
			if batch.OriginNodeId == 9 {
				wantEvents = []store.Event{events[1]}
			} else if batch.OriginNodeId != 1 {
				t.Fatalf("unexpected origin: %d", batch.OriginNodeId)
			}
			last := wantEvents[len(wantEvents)-1]
			if envelope.Sequence != uint64(last.Sequence) || envelope.SentAtHlc != last.HLC.String() || len(batch.Events) != len(wantEvents) {
				t.Fatalf("unexpected batch metadata: %v", envelope)
			}
			for i, want := range wantEvents {
				if !proto.Equal(batch.Events[i], store.ToReplicatedEvent(want)) {
					t.Fatalf("peer %d: event %d changed or reordered", sess.peerID, i)
				}
			}
		}
	}
	if len(mgr.replicationBatches.buckets) != 0 {
		t.Fatal("flush left pending buckets")
	}
}

func TestReplicationPrepareFlushBoundaries(t *testing.T) {
	now := time.Unix(123, 0).UTC()
	t.Run("exact protobuf bytes", func(t *testing.T) {
		for _, target := range []int{maxBatchBytes - 1, maxBatchBytes, maxBatchBytes + 1} {
			batcher := newReplicationBatcher()
			event := batcherTestMessageEvent(1, 11, 101, target)
			body := event.Body.(*internalproto.MessageCreatedEvent)
			body.Body = make([]byte, target-(proto.Size(store.ToReplicatedEvent(event))-target))
			entry := prepareReplicationEvent(event)
			if entry.size != target {
				t.Fatalf("fixture size: got %d want %d", entry.size, target)
			}
			flushes := batcher.enqueuePrepared(2, entry, now)
			if target < maxBatchBytes {
				if len(flushes) != 0 {
					t.Fatal("flushed before byte threshold")
				}
				key := replicationBatchKey{2, 1}
				if batcher.buckets[key].totalBytes != target {
					t.Fatal("byte accounting includes envelope/framing overhead")
				}
				flushes = batcher.enqueuePrepared(2, prepareReplicationEvent(batcherTestMessageEvent(1, 12, 102, 1)), now)
				if len(flushes) != 1 || len(flushes[0].events) != 2 {
					t.Fatalf("expected both events when crossing byte threshold: %+v", flushes)
				}
			} else if len(flushes) != 1 || len(flushes[0].events) != 1 {
				t.Fatalf("size %d did not immediately flush: %+v", target, flushes)
			}
		}
	})
	t.Run("count", func(t *testing.T) {
		batcher := newReplicationBatcher()
		for i := 1; i <= maxBatchEvents; i++ {
			entry := prepareReplicationEvent(batcherTestMessageEvent(1, int64(i+10), int64(i), 1))
			flushes := batcher.enqueuePrepared(2, entry, now)
			if i < maxBatchEvents && len(flushes) != 0 {
				t.Fatalf("early count flush at %d", i)
			}
			if i == maxBatchEvents {
				if len(flushes) != 1 || len(flushes[0].events) != i || flushes[0].sequence != uint64(i+10) || flushes[0].sentAtHLC != entry.sentAtHLC {
					t.Fatalf("wrong count threshold flush: %+v", flushes)
				}
			}
		}
	})
	t.Run("two millisecond deadline is not extended", func(t *testing.T) {
		if maxBatchDelay != 2*time.Millisecond {
			t.Fatalf("unexpected batching delay: %v", maxBatchDelay)
		}
		batcher := newReplicationBatcher()
		first := prepareReplicationEvent(batcherTestMessageEvent(1, 11, 101, 1))
		last := prepareReplicationEvent(batcherTestMessageEvent(1, 12, 102, 1))
		batcher.enqueuePrepared(2, first, now)
		batcher.enqueuePrepared(3, first, now.Add(time.Millisecond))
		batcher.enqueuePrepared(2, last, now.Add(time.Millisecond))
		if flushes := batcher.flushDue(now.Add(maxBatchDelay - time.Nanosecond)); len(flushes) != 0 {
			t.Fatal("flushed before 2ms")
		}
		flushes := batcher.flushDue(now.Add(maxBatchDelay))
		if len(flushes) != 1 || flushes[0].peerID != 2 || len(flushes[0].events) != 2 || flushes[0].sequence != 12 || flushes[0].sentAtHLC != last.sentAtHLC {
			t.Fatalf("wrong flush at original deadline: %+v", flushes)
		}
		flushes = batcher.flushDue(now.Add(maxBatchDelay + time.Millisecond))
		if len(flushes) != 1 || flushes[0].peerID != 3 || len(flushes[0].events) != 1 {
			t.Fatalf("peer deadline was not independent: %+v", flushes)
		}
	})
}

// 嵌入接口以满足 EventBody，但保持一个 SetTypedBody 不支持的动态类型。
type replicationPrepareUnsupportedBody struct{ internalproto.EventBody }

func TestReplicationPrepareConversionFailure(t *testing.T) {
	batcher := newReplicationBatcher()
	now := time.Unix(123, 0)
	valid := prepareReplicationEvent(batcherTestMessageEvent(1, 1, 1, 32))
	batcher.enqueuePrepared(2, valid, now)
	for i := 2; i <= maxBatchEvents; i++ {
		event := batcherTestMessageEvent(1, int64(i+100), int64(i), 0)
		event.Body = replicationPrepareUnsupportedBody{}
		entry := prepareReplicationEvent(event)
		if entry.replicated != nil || entry.size != 0 || entry.sentAtHLC != event.HLC.String() {
			t.Fatalf("wrong failed-conversion preparation: %+v", entry)
		}
		flushes := batcher.enqueuePrepared(2, entry, now)
		if i < maxBatchEvents {
			if len(flushes) != 0 || batcher.buckets[replicationBatchKey{2, 1}].totalBytes != valid.size {
				t.Fatal("failed conversion changed count/byte accounting")
			}
		} else if len(flushes) != 1 || len(flushes[0].events) != 1 || flushes[0].events[0] != valid.replicated || flushes[0].sequence != uint64(event.Sequence) || flushes[0].sentAtHLC != event.HLC.String() {
			t.Fatalf("failed conversion lost original count or last-event metadata: %+v", flushes)
		}
	}
}

func TestReplicationPrepareDrainAndUnavailablePeers(t *testing.T) {
	mgr, sessions := replicationPrepareTestManager(3)
	mgr.publishCh = make(chan store.Event, 2)
	mgr.publishCh <- batcherTestMessageEvent(1, 11, 101, 32)
	mgr.publishCh <- batcherTestMessageEvent(1, 12, 102, 32)
	mgr.drainPublishedEvents()
	// 关闭刷新沿用原有规则：退出的 peer 跳过，关闭通道由 enqueue 处理。
	delete(mgr.peers, sessions[0].peerID)
	sessions[1].close()
	mgr.flushReplicationBatches()
	if len(mgr.publishCh) != 0 || len(mgr.replicationBatches.buckets) != 0 {
		t.Fatal("shutdown helpers left events buffered")
	}
	if len(sessions[0].send) != 0 || len(sessions[2].send) != 1 {
		t.Fatal("unavailable peers affected healthy peer dispatch")
	}
	envelope := <-sessions[2].send
	if envelope.Sequence != 12 || len(envelope.GetEventBatch().Events) != 2 {
		t.Fatalf("shutdown flush lost events: %v", envelope)
	}
	mgr.flushReplicationBatches()
	if len(sessions[2].send) != 0 {
		t.Fatal("shutdown flush duplicated events")
	}
}

func TestReplicationPrepareConcurrentEncoding(t *testing.T) {
	const peers = 12
	mgr, sessions := replicationPrepareTestManager(peers)
	mgr.cfg.ClusterSecret = "replication-prepare-test"
	mgr.queuePublishedEvent(batcherTestMessageEvent(1, 11, 101, 4096))
	mgr.queuePublishedEvent(batcherTestMessageEvent(1, 12, 102, 128))
	mgr.flushReplicationBatches()
	envelopes := make([]*internalproto.Envelope, peers)
	for i, sess := range sessions {
		envelopes[i] = <-sess.send
	}
	shared := envelopes[0].GetEventBatch().Events
	snapshots := make([]*internalproto.ReplicatedEvent, len(shared))
	for i, event := range shared {
		snapshots[i] = proto.Clone(event).(*internalproto.ReplicatedEvent)
	}
	for _, envelope := range envelopes {
		for i, event := range envelope.GetEventBatch().Events {
			if event != shared[i] {
				t.Fatal("concurrency test requires shared protobuf event pointers")
			}
		}
	}

	// 同步起跑，让各 peer 并发读取相同外壳和嵌套 body，包括 protobuf 大小缓存。
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, envelope := range envelopes {
		wg.Add(1)
		go func(envelope *internalproto.Envelope) {
			defer wg.Done()
			meshEnvelope := &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ReplicationBatch{
				ReplicationBatch: &mesh.ReplicationBatch{
					OriginNodeId: mgr.cfg.NodeID, Sequence: envelope.Sequence,
					SentAtHlc: envelope.SentAtHlc, EventBatch: envelope.GetEventBatch(),
				},
			}}
			<-start
			var firstWire, firstMesh, firstSigned []byte
			for i := range 32 {
				wire, err := marshalOptions.Marshal(envelope)
				if err != nil || len(wire) != proto.Size(envelope) {
					t.Errorf("concurrent envelope encoding: %v", err)
					return
				}
				meshWire, err := meshMarshalOptions.Marshal(meshEnvelope)
				if err != nil || len(meshWire) != proto.Size(meshEnvelope) {
					t.Errorf("concurrent mesh encoding: %v", err)
					return
				}
				signed, err := mgr.marshalSignedEnvelope(envelope)
				if err != nil {
					t.Errorf("concurrent signed encoding: %v", err)
					return
				}
				if i == 0 {
					firstWire, firstMesh, firstSigned = wire, meshWire, signed
					var decoded internalproto.Envelope
					if err := proto.Unmarshal(signed, &decoded); err != nil {
						t.Errorf("decode signed envelope: %v", err)
						return
					}
					if err := mgr.verifyEnvelope(&decoded); err != nil || !proto.Equal(decoded.GetEventBatch(), envelope.GetEventBatch()) {
						t.Errorf("signed encoding changed batch or signature: %v", err)
						return
					}
				} else if !bytes.Equal(wire, firstWire) || !bytes.Equal(meshWire, firstMesh) || !bytes.Equal(signed, firstSigned) {
					t.Error("concurrent encoding produced unstable bytes")
					return
				}
			}
		}(envelope)
	}
	close(start)
	wg.Wait()
	for i, event := range shared {
		if !proto.Equal(event, snapshots[i]) {
			t.Fatal("encoding mutated shared event")
		}
	}
}

func replicationPrepareTestManager(peers int) (*Manager, []*session) {
	mgr := &Manager{
		cfg:                Config{NodeID: 1},
		peers:              make(map[int64]*peerState, peers),
		replicationBatches: newReplicationBatcher(),
	}
	sessions := make([]*session, peers)
	for i := range sessions {
		sess := &session{peerID: int64(i + 2), send: make(chan *internalproto.Envelope, maxBatchEvents)}
		sessions[i] = sess
		mgr.peers[sess.peerID] = &peerState{active: sess}
	}
	return mgr, sessions
}
