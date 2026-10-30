package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func snapshotRequestInFlightForTest(sess *session, partition string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	state := sess.pendingSnapshotParts[partition]
	return state != nil && state.inFlight
}

func TestSnapshotRequestRetryLifecycle(t *testing.T) {
	sess := &session{send: make(chan *internalproto.Envelope, 1)}
	now := time.Now()
	kind := internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS
	token, ok := sess.beginSnapshotRequestAt(store.SnapshotUsersPartition, kind, now)
	if !ok || sess.pendingSnapshotCount() != 1 {
		t.Fatal("request did not become in flight")
	}
	sess.failSnapshotRequest(store.SnapshotUsersPartition, token, now)
	if snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) || sess.pendingSnapshotCount() != 1 {
		t.Fatal("failed attempt must enter backoff while retaining partition deduplication")
	}
	if _, ok := sess.beginSnapshotRequestAt(store.SnapshotUsersPartition, kind, now); ok {
		t.Fatal("another digest bypassed retry backoff")
	}
	if due, expired := sess.takeSnapshotRetries(now.Add(snapshotRetryBase - time.Nanosecond)); len(due) != 0 || expired != 0 {
		t.Fatal("request retried before backoff elapsed")
	}
	due, expired := sess.takeSnapshotRetries(now.Add(snapshotRetryBase))
	if expired != 0 || len(due) != 1 || due[0].kind != kind || due[0].token == token {
		t.Fatalf("unexpected retry: %+v, expired=%d", due, expired)
	}
	// A late transport failure must not cancel the next attempt.
	sess.failSnapshotRequest(store.SnapshotUsersPartition, token, now)
	if !snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) {
		t.Fatal("late failure cancelled retry")
	}
	sess.completeSnapshotRequest(store.SnapshotUsersPartition)
	freshToken, ok := sess.beginSnapshotRequestAt(store.SnapshotUsersPartition, kind, now)
	if !ok || freshToken == due[0].token {
		t.Fatal("new request reused an attempt token")
	}
	sess.failSnapshotRequest(store.SnapshotUsersPartition, due[0].token, now)
	if !snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) {
		t.Fatal("failure from a completed request cancelled fresh request")
	}
	sess.close()
	if sess.hasSnapshotRequests() {
		t.Fatal("closed session retained requests")
	}
	if _, ok := sess.beginSnapshotRequestAt(store.SnapshotUsersPartition, kind, now); ok {
		t.Fatal("closed session accepted request")
	}
	for attempt := uint64(1); attempt <= 100; attempt++ {
		delay := snapshotRetryDelay(attempt)
		if delay < snapshotRetryBase || delay > snapshotRetryMax || (attempt >= 6 && delay != snapshotRetryMax) {
			t.Fatalf("invalid backoff for attempt %d: %s", attempt, delay)
		}
	}
}

func TestSnapshotLostResponseRetriesWithoutDirtyDigest(t *testing.T) {
	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	mgr.requestSnapshotPartition(sess, &internalproto.SnapshotPartitionDigest{
		Partition: store.SnapshotUsersPartition,
		Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS,
	})
	assertSnapshotRequest(t, sess, store.SnapshotUsersPartition)
	// Leave the digest clean. The production sweep must still find this request.
	mgr.clearSnapshotDigestDirty(sess, mgr.peers[sess.peerID].snapshotDigestGeneration)
	timeout := time.Now().Add(snapshotRequestTimeout)
	mgr.flushSnapshotDigestsDue(timeout)
	if snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) || sess.pendingSnapshotCount() != 1 || len(sess.send) != 0 {
		t.Fatal("timeout did not enter retry backoff")
	}
	mgr.flushSnapshotDigestsDue(timeout.Add(snapshotRetryBase))
	assertSnapshotRequest(t, sess, store.SnapshotUsersPartition)
	chunk, err := st.BuildSnapshotChunk(context.Background(), store.SnapshotUsersPartition)
	if err != nil {
		t.Fatal(err)
	}
	chunk.SnapshotVersion = internalproto.SnapshotVersion
	if err := mgr.handleSnapshotChunk(sess, &internalproto.Envelope{NodeId: sess.peerID, Body: &internalproto.Envelope_SnapshotChunk{SnapshotChunk: chunk}}); err != nil {
		t.Fatal(err)
	}
	if sess.hasSnapshotRequests() {
		t.Fatal("valid response did not complete request")
	}
	mgr.retrySnapshotPartitions(sess, timeout.Add(10*time.Minute))
	if len(sess.send) != 0 {
		t.Fatal("completed request was retried")
	}
}

func TestSnapshotRejectedResponseRetries(t *testing.T) {
	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	mgr.requestSnapshotPartition(sess, &internalproto.SnapshotPartitionDigest{
		Partition: store.SnapshotUsersPartition,
		Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS,
	})
	assertSnapshotRequest(t, sess, store.SnapshotUsersPartition)
	// The timestamp pass succeeds, but the transaction rejects a message row
	// inside a users partition. Application errors must not lose the repair.
	err := mgr.handleSnapshotChunk(sess, &internalproto.Envelope{NodeId: sess.peerID, Body: &internalproto.Envelope_SnapshotChunk{SnapshotChunk: &internalproto.SnapshotChunk{
		Partition:       store.SnapshotUsersPartition,
		SnapshotVersion: internalproto.SnapshotVersion,
		Kind:            internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS,
		Rows:            []*internalproto.SnapshotRow{{Body: &internalproto.SnapshotRow_Message{Message: &internalproto.SnapshotMessageRow{CreatedAtHlc: mgr.clock.Now().String()}}}},
	}}})
	if err == nil {
		t.Fatal("invalid users snapshot was accepted")
	}
	if snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) || sess.pendingSnapshotCount() != 1 {
		t.Fatal("rejected response lost request or left it in flight")
	}
	mgr.flushSnapshotDigestsDue(time.Now().Add(snapshotRetryBase))
	assertSnapshotRequest(t, sess, store.SnapshotUsersPartition)
}

func TestSnapshotMeshSendFailureRetries(t *testing.T) {
	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)
	runtime, err := mesh.NewRuntime(mesh.RuntimeOptions{
		LocalNodeID:   mgr.cfg.NodeID,
		Adapters:      []mesh.TransportAdapter{newRecordingMeshAdapter(mesh.TransportWebSocket)},
		LocalPolicy:   mgr.cfg.MeshForwardingPolicy(),
		TopologyStore: mesh.NewMemoryTopologyStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	mgr.meshRuntime = &MeshRuntimeBinding{runtime: runtime}
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	sess.conn = nil
	mgr.requestSnapshotPartition(sess, &internalproto.SnapshotPartitionDigest{
		Partition: store.SnapshotUsersPartition,
		Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS,
	})
	if snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) || sess.pendingSnapshotCount() != 1 {
		t.Fatal("unroutable mesh send did not enter backoff")
	}
	firstToken := sess.snapshotRequestToken(store.SnapshotUsersPartition)
	mgr.flushSnapshotDigestsDue(time.Now().Add(snapshotRetryBase))
	if sess.snapshotRequestToken(store.SnapshotUsersPartition) == firstToken || snapshotRequestInFlightForTest(sess, store.SnapshotUsersPartition) {
		t.Fatal("mesh send failure was not retried with a new attempt")
	}
}
