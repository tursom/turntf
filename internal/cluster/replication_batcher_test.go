package cluster

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestReplicationBatcherFlushesSameOriginInOrder(t *testing.T) {
	t.Parallel()

	batcher := newReplicationBatcher()
	now := time.Now().UTC()

	flushes := batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), 1, 11, 128), now)
	if len(flushes) != 0 {
		t.Fatalf("expected first enqueue to stay buffered, got %d flushes", len(flushes))
	}
	flushes = batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), 2, 12, 128), now)
	if len(flushes) != 0 {
		t.Fatalf("expected second enqueue to stay buffered, got %d flushes", len(flushes))
	}

	flushes = batcher.flushDue(now.Add(maxBatchDelay))
	if len(flushes) != 1 {
		t.Fatalf("expected one due flush, got %d", len(flushes))
	}
	flushed := flushes[0]
	if flushed.peerID != testNodeID(2) || flushed.originNodeID != testNodeID(1) {
		t.Fatalf("unexpected flush target: %+v", flushed)
	}
	if flushed.sequence != 2 || flushed.sentAtHLC == "" {
		t.Fatalf("unexpected envelope metadata: %+v", flushed)
	}
	if len(flushed.events) != 2 || flushed.events[0].GetEventId() != 11 || flushed.events[1].GetEventId() != 12 {
		t.Fatalf("unexpected flushed event order: %+v", flushed.events)
	}
}

func TestReplicationBatcherDoesNotMixOrigins(t *testing.T) {
	t.Parallel()

	batcher := newReplicationBatcher()
	now := time.Now().UTC()
	batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), 1, 11, 64), now)
	batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(9), 2, 21, 64), now)

	flushes := batcher.flushAll()
	if len(flushes) != 2 {
		t.Fatalf("expected two origin-specific flushes, got %d", len(flushes))
	}
	sort.Slice(flushes, func(i, j int) bool {
		return flushes[i].originNodeID < flushes[j].originNodeID
	})
	if flushes[0].originNodeID != testNodeID(1) || len(flushes[0].events) != 1 {
		t.Fatalf("unexpected first flush: %+v", flushes[0])
	}
	if flushes[1].originNodeID != testNodeID(9) || len(flushes[1].events) != 1 {
		t.Fatalf("unexpected second flush: %+v", flushes[1])
	}
}

func TestReplicationBatcherFlushThresholds(t *testing.T) {
	t.Parallel()

	t.Run("count", func(t *testing.T) {
		batcher := newReplicationBatcher()
		now := time.Now().UTC()
		var flushes []*flushedReplicationBatch
		for i := 0; i < maxBatchEvents; i++ {
			flushes = batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), int64(i+1), int64(i+1), 64), now)
		}
		if len(flushes) != 1 || len(flushes[0].events) != maxBatchEvents {
			t.Fatalf("expected count threshold flush, got %+v", flushes)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		batcher := newReplicationBatcher()
		now := time.Now().UTC()
		flushes := batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), 1, 1, 40<<10), now)
		if len(flushes) != 0 {
			t.Fatalf("expected first large event to stay buffered")
		}
		flushes = batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), 2, 2, 40<<10), now)
		if len(flushes) != 1 || len(flushes[0].events) != 2 {
			t.Fatalf("expected byte threshold flush, got %+v", flushes)
		}
	})

	t.Run("delay", func(t *testing.T) {
		batcher := newReplicationBatcher()
		now := time.Now().UTC()
		batcher.enqueue(testNodeID(2), batcherTestMessageEvent(testNodeID(1), 1, 1, 64), now)
		if flushes := batcher.flushDue(now.Add(maxBatchDelay - time.Nanosecond)); len(flushes) != 0 {
			t.Fatalf("expected delay threshold to wait, got %+v", flushes)
		}
		if flushes := batcher.flushDue(now.Add(maxBatchDelay)); len(flushes) != 1 {
			t.Fatalf("expected delay threshold flush, got %+v", flushes)
		}
	})
}

func TestHandleEventBatchAcksLastEventForBatchedApply(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	firstUser, firstEvent, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "batched-user-1",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create first source user: %v", err)
	}
	secondUser, secondEvent, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "batched-user-2",
		PasswordHash: "hash-2",
	})
	if err != nil {
		t.Fatalf("create second source user: %v", err)
	}

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId:    testNodeID(1),
		Sequence:  uint64(secondEvent.Sequence),
		SentAtHlc: secondEvent.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				OriginNodeId: testNodeID(1),
				Events: []*internalproto.ReplicatedEvent{
					store.ToReplicatedEvent(firstEvent),
					store.ToReplicatedEvent(secondEvent),
				},
			},
		},
	}

	if err := mgr.handleEventBatch(sess, envelope); err != nil {
		t.Fatalf("handle event batch: %v", err)
	}

	if _, err := targetStore.GetUser(context.Background(), firstUser.Key()); err != nil {
		t.Fatalf("expected first replicated user: %v", err)
	}
	if _, err := targetStore.GetUser(context.Background(), secondUser.Key()); err != nil {
		t.Fatalf("expected second replicated user: %v", err)
	}

	select {
	case ackEnvelope := <-sess.send:
		ack := ackEnvelope.GetAck()
		if ack == nil {
			t.Fatalf("expected ack envelope, got %+v", ackEnvelope)
		}
		if ack.AckedEventId != uint64(secondEvent.EventID) {
			t.Fatalf("unexpected batched acked event id: got=%d want=%d", ack.AckedEventId, secondEvent.EventID)
		}
	default:
		t.Fatalf("expected a single batched ack")
	}
}

func TestHandleEventBatchPushDoesNotMarkSnapshotDigestDirty(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	_, event, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "steady-push",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	envelope := &internalproto.Envelope{
		NodeId:    testNodeID(1),
		Sequence:  uint64(event.Sequence),
		SentAtHlc: event.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				OriginNodeId: testNodeID(1),
				Events:       []*internalproto.ReplicatedEvent{store.ToReplicatedEvent(event)},
			},
		},
	}

	if err := mgr.handleEventBatch(sess, envelope); err != nil {
		t.Fatalf("handle push event batch: %v", err)
	}
	if dirty, immediate := snapshotDigestState(mgr, testNodeID(1)); dirty || immediate {
		t.Fatalf("expected steady-state push to skip digest dirty mark, got dirty=%v immediate=%v", dirty, immediate)
	}
}

func TestHandleEventBatchEmptyPullMarksSnapshotDigestDirty(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	requestID, ok := sess.beginPendingPull(testNodeID(1), 0)
	if !ok {
		t.Fatalf("expected pending pull to start")
	}

	envelope := &internalproto.Envelope{
		NodeId: testNodeID(1),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				PullRequestId: requestID,
				OriginNodeId:  testNodeID(1),
			},
		},
	}

	if err := mgr.handleEventBatch(sess, envelope); err != nil {
		t.Fatalf("handle empty pull response: %v", err)
	}
	if dirty, immediate := snapshotDigestState(mgr, testNodeID(1)); !dirty || immediate {
		t.Fatalf("expected empty pull to mark non-immediate digest dirty, got dirty=%v immediate=%v", dirty, immediate)
	}

	select {
	case extra := <-sess.send:
		t.Fatalf("did not expect immediate snapshot digest, got %+v", extra)
	default:
	}

	mgr.flushSnapshotDigestsDue(time.Now().UTC())
	select {
	case sent := <-sess.send:
		if sent.GetSnapshotDigest() == nil {
			t.Fatalf("expected scheduled snapshot digest, got %+v", sent)
		}
	default:
		t.Fatalf("expected scheduled snapshot digest after sweep")
	}
}

func TestHandleSnapshotChunkMarksImmediateSnapshotDigestDirty(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	if _, _, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "snapshot-dirty-user",
		PasswordHash: "hash-1",
	}); err != nil {
		t.Fatalf("create source user: %v", err)
	}
	chunk, err := sourceStore.BuildSnapshotChunk(context.Background(), store.SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users chunk: %v", err)
	}
	chunk.SnapshotVersion = internalproto.SnapshotVersion

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	if !sess.beginSnapshotRequest(store.SnapshotUsersPartition) {
		t.Fatalf("expected snapshot request to start")
	}

	now := time.Now().UTC()
	mgr.mu.Lock()
	if peer := mgr.peers[testNodeID(1)]; peer != nil {
		peer.lastSnapshotDigestAt = now
	}
	mgr.mu.Unlock()

	if err := mgr.handleSnapshotChunk(sess, &internalproto.Envelope{
		NodeId: testNodeID(1),
		Body: &internalproto.Envelope_SnapshotChunk{
			SnapshotChunk: chunk,
		},
	}); err != nil {
		t.Fatalf("handle snapshot chunk: %v", err)
	}
	if dirty, immediate := snapshotDigestState(mgr, testNodeID(1)); !dirty || !immediate {
		t.Fatalf("expected repaired snapshot chunk to mark immediate digest dirty, got dirty=%v immediate=%v", dirty, immediate)
	}

	mgr.flushSnapshotDigestsDue(now)
	select {
	case sent := <-sess.send:
		if sent.GetSnapshotDigest() == nil {
			t.Fatalf("expected immediate snapshot digest, got %+v", sent)
		}
	default:
		t.Fatalf("expected immediate snapshot digest after repair sweep")
	}
}

func batcherTestMessageEvent(originNodeID, sequence, eventID int64, payloadSize int) store.Event {
	hlcClock := clock.NewClock(originNodeID)
	body := make([]byte, payloadSize)
	return store.Event{
		Sequence:     sequence,
		EventID:      eventID,
		EventType:    store.EventTypeMessageCreated,
		Aggregate:    "message",
		HLC:          hlcClock.Now(),
		OriginNodeID: originNodeID,
		Body: &internalproto.MessageCreatedEvent{
			Recipient:    &internalproto.ClusterUserRef{NodeId: originNodeID, UserId: 1},
			NodeId:       originNodeID,
			Seq:          sequence,
			Sender:       &internalproto.ClusterUserRef{NodeId: originNodeID, UserId: 2},
			Body:         body,
			CreatedAtHlc: hlcClock.Now().String(),
		},
	}
}

func snapshotDigestState(mgr *Manager, peerID int64) (bool, bool) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	peer := mgr.peers[peerID]
	if peer == nil {
		return false, false
	}
	return peer.snapshotDigestDirty, peer.snapshotDigestImmediate
}
