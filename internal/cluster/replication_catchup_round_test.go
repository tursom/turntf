package cluster

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestPeriodicCatchupSharesSnapshotAcrossPeers(t *testing.T) {
	for _, engine := range []string{"sqlite", "pebble"} {
		t.Run(engine, func(t *testing.T) {
			var st *store.Store
			if engine == "pebble" {
				st = newPebbleReplicationTestStore(t, "catchup", 2)
			} else {
				st = newReplicationTestStore(t, "catchup", 2)
			}
			mgr := newReplicationTestManager(t, st)
			origin := testNodeID(1)
			if err := st.RecordOriginApplied(context.Background(), origin, 5); err != nil {
				t.Fatal(err)
			}
			first := readySnapshotTestSession(mgr, testNodeID(3), store.DefaultMessageWindowSize)
			second := readySnapshotTestSession(mgr, testNodeID(4), store.DefaultMessageWindowSize)
			first.noteRemoteOriginEvent(origin, 10)
			second.noteRemoteOriginEvent(origin, 10)
			first.send = make(chan *internalproto.Envelope)
			// 阻塞第二个 peer 的进度读取，确保在两次请求之间推进本地游标。
			second.mu.Lock()
			locked := true
			defer func() {
				if locked {
					second.mu.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- mgr.requestPeriodicCatchup([]*session{first, second}) }()
			select {
			case envelope := <-first.send:
				if got := envelope.GetPullEvents().GetAfterEventId(); got != 5 {
					t.Fatalf("first cursor = %d, want 5", got)
				}
			case <-time.After(time.Second):
				t.Fatal("first peer did not request catchup")
			}
			if err := st.RecordOriginApplied(context.Background(), origin, 8); err != nil {
				t.Fatal(err)
			}
			second.mu.Unlock()
			locked = false
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("catchup round did not finish")
			}
			pull := (<-second.send).GetPullEvents()
			if pull.GetAfterEventId() != 5 {
				t.Fatalf("second peer did not share round snapshot: %v", pull)
			}
			second.completePendingPull(origin, pull.GetRequestId())
			if _, err := mgr.requestCatchupIfNeeded(second); err != nil {
				t.Fatal(err)
			}
			if got := (<-second.send).GetPullEvents().GetAfterEventId(); got != 8 {
				t.Fatalf("immediate catchup cursor = %d, want latest 8", got)
			}
		})
	}
}

func TestPeriodicCatchupMissingCursorOrderingAndPending(t *testing.T) {
	mgr := newReplicationTestManager(t, newReplicationTestStore(t, "catchup", 2))
	sess := readySnapshotTestSession(mgr, testNodeID(3), store.DefaultMessageWindowSize)
	for _, slot := range []uint16{5, 1, 4} {
		sess.noteRemoteOriginEvent(testNodeID(slot), 10)
	}
	if err := mgr.store.RecordOriginApplied(context.Background(), testNodeID(4), 10); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		if err := mgr.requestPeriodicCatchup([]*session{nil, {peerID: 0}, sess}); err != nil {
			t.Fatal(err)
		}
	}
	for i, slot := range []uint16{1, 5} {
		pull := (<-sess.send).GetPullEvents()
		if pull.GetOriginNodeId() != testNodeID(slot) || pull.GetAfterEventId() != 0 || pull.GetRequestId() != uint64(i+1) || pull.GetLimit() != pullBatchSize {
			t.Fatalf("unexpected pull: %v", pull)
		}
	}
	if len(sess.send) != 0 {
		t.Fatal("pending or up-to-date origins were requested again")
	}
	if requested, err := mgr.requestCatchupIfNeeded(sess); err != nil || !requested {
		t.Fatalf("pending pull must still count as requested: %v, %v", requested, err)
	}
}

func TestPeriodicCatchupSnapshotReadFailure(t *testing.T) {
	st := newReplicationTestStore(t, "catchup", 2)
	mgr := newReplicationTestManager(t, st)
	sess := readySnapshotTestSession(mgr, testNodeID(3), store.DefaultMessageWindowSize)
	sess.noteRemoteOriginEvent(testNodeID(1), 10)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mgr.requestPeriodicCatchup(nil); err != nil {
		t.Fatalf("empty round read the closed store: %v", err)
	}
	if err := mgr.requestPeriodicCatchup([]*session{sess}); err == nil {
		t.Fatal("expected snapshot read error")
	}
	if sess.hasPendingPulls() || len(sess.send) != 0 {
		t.Fatal("snapshot read failure started a pull")
	}
}

func TestPeriodicCatchupContinuesAfterPeerFailure(t *testing.T) {
	mgr := newReplicationTestManager(t, newReplicationTestStore(t, "catchup", 2))
	// 非空 binding 让无连接 session 走 mesh 分支；未附加 runtime 的发送立即失败。
	mgr.meshRuntime = &MeshRuntimeBinding{}
	failed := &session{manager: mgr, peerID: testNodeID(3)}
	failed.noteRemoteOriginEvent(testNodeID(1), 10)
	otherPending, ok := failed.beginPendingPull(testNodeID(5), 3)
	if !ok {
		t.Fatal("could not start unrelated pending pull")
	}
	healthy := readySnapshotTestSession(mgr, testNodeID(4), store.DefaultMessageWindowSize)
	healthy.noteRemoteOriginEvent(testNodeID(1), 10)
	if err := mgr.requestPeriodicCatchup([]*session{failed, healthy}); err != nil {
		t.Fatal(err)
	}
	if failed.hasPendingPull(testNodeID(1)) || !failed.hasPendingPull(testNodeID(5)) {
		t.Fatal("failed send did not cancel precisely its own pending pull")
	}
	if len(healthy.send) != 1 {
		t.Fatal("failed peer stopped catchup to the next peer")
	}
	// 失败的请求可即时重试，错误仍向原单 session 调用者返回。
	if requested, err := mgr.requestCatchupIfNeeded(failed); requested || err == nil {
		t.Fatalf("immediate failed retry = %t, %v", requested, err)
	}
	if failed.hasPendingPull(testNodeID(1)) || failed.nextPullRequestID != otherPending+2 {
		t.Fatal("failed retry retained pending pull or reused request ID")
	}
}
