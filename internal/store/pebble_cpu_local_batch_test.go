package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestPebbleCPULocalBatchFinalStatesAndEvents(t *testing.T) {
	for _, userCount := range []int{1, 8, pebbleLocalMessageBatchMaxOps} {
		t.Run(fmt.Sprintf("users-%d", userCount), func(t *testing.T) {
			ctx := context.Background()
			st := openPebbleTestStore(t, "cpu-batch-state", 1, 256)
			backend := requirePebbleBackend(t, st)
			keys := make([]UserKey, userCount)
			for i := range keys {
				keys[i] = pebbleCPUCreateUser(t, st, fmt.Sprintf("user-%d", i), RoleUser).Key()
			}
			last, err := st.LastEventSequence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// 先写一轮，再写另一同步模式的批次，验证从持久状态继续累加。
			counts := make(map[UserKey]int64)
			for _, mode := range []PebbleMessageSyncMode{PebbleMessageSyncModeNoSync, PebbleMessageSyncModeForceSync} {
				requests := pebbleCPURequests(keys, pebbleLocalMessageBatchMaxOps, mode)
				backend.processLocalMessageBatch(requests)
				for _, request := range requests {
					result := <-request.response
					counts[request.params.UserKey]++
					last++
					if result.err != nil || result.message.Seq != counts[request.params.UserKey] || result.event.Sequence != last {
						t.Fatalf("unexpected batch result: %+v count=%d eventSequence=%d", result, counts[request.params.UserKey], last)
					}
					// 收到成功响应时，消息、事件和序号均已在 DB 中可见。
					stored, err := backend.messageProjectionRepo.messageByRef(pebbleMessageRefFromMessage(result.message))
					if err != nil || messageIdentity(stored) != messageIdentity(result.message) {
						t.Fatalf("response before message persisted: %+v err=%v", stored, err)
					}
					event, err := backend.eventLog.eventBySequence(result.event.Sequence)
					if err != nil || event.EventID != result.event.EventID {
						t.Fatalf("response before event persisted: %+v err=%v", event, err)
					}
				}
				for _, key := range keys {
					state := readPebbleMessageUserStateForTest(t, st, key)
					if state.StoredCount != counts[key] || state.MaxSeq != counts[key] || state.TrimNeeded {
						t.Fatalf("final state for %+v: %+v want count=%d", key, state, counts[key])
					}
					next, ok, err := backend.messageSequences.readStoredNextSequenceLocked(pebbleMessageSequenceKey(key, st.NodeID()))
					if err != nil || !ok || next != counts[key]+1 {
						t.Fatalf("persisted sequence=%d ok=%v err=%v", next, ok, err)
					}
					inbox, err := backend.messageProjectionRepo.listInboxMessagesByUser(ctx, key, 1000)
					if err != nil || len(inbox) != 0 {
						t.Fatalf("local batch unexpectedly populated inbox: count=%d err=%v", len(inbox), err)
					}
				}
			}
			stats := backend.localMessageStats.snapshot()
			if stats.NoSyncBatches != 1 || stats.ForceSyncBatches != 1 {
				t.Fatalf("unexpected commit modes: %+v", stats)
			}
		})
	}
}

func TestPebbleCPULocalBatchTrimBoundary(t *testing.T) {
	st := openPebbleTestStore(t, "cpu-trim-boundary", 1, 4)
	backend := requirePebbleBackend(t, st)
	// 禁用后台 worker，使软阈值和硬阈值的边界可重复观察。
	if err := backend.messageProjectionRepo.close(); err != nil {
		t.Fatal(err)
	}
	key := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	for _, count := range []int{128, 4, 1} {
		requests := pebbleCPURequests([]UserKey{key}, count, PebbleMessageSyncModeNoSync)
		backend.processLocalMessageBatch(requests)
		var last int64
		for _, request := range requests {
			result := <-request.response
			if result.err != nil {
				t.Fatal(result.err)
			}
			last = result.message.Seq
		}
		state := readPebbleMessageUserStateForTest(t, st, key)
		wantCount, wantTrim := last, true
		if last == 133 {
			wantCount, wantTrim = 4, false
		}
		if state.StoredCount != wantCount || state.MaxSeq != last || state.TrimNeeded != wantTrim {
			t.Fatalf("trim boundary at %d: state=%+v wantCount=%d wantTrim=%v", last, state, wantCount, wantTrim)
		}
	}
	messages, err := backend.messageProjectionRepo.listStoredMessagesByUser(context.Background(), key, nil)
	if err != nil || len(messages) != 4 || messages[0].Seq != 133 || messages[3].Seq != 130 {
		t.Fatalf("hard trim messages=%+v err=%v", messages, err)
	}
}

func TestPebbleCPULocalBatchFailuresDoNotPublishStateOrSequences(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "cpu-batch-failure", 1, 256)
	backend := requirePebbleBackend(t, st)
	key := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	base, err := st.LastEventSequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertUnchanged := func() {
		t.Helper()
		state, ok, err := backend.messageProjectionRepo.readMessageUserStateLocked(key)
		if err != nil || ok || state != (pebbleMessageUserState{}) {
			t.Fatalf("failed batch published state: %+v ok=%v err=%v", state, ok, err)
		}
		last, err := st.LastEventSequence(ctx)
		if err != nil || last != base {
			t.Fatalf("failed batch published event sequence: %d err=%v", last, err)
		}
		_, _, next, err := backend.messageSequences.LoadNextSequence(ctx, key, st.NodeID())
		if err != nil || next != 1 {
			t.Fatalf("failed batch advanced sequence: %d err=%v", next, err)
		}
		messages, err := backend.messageProjectionRepo.listStoredMessagesByUser(ctx, key, nil)
		if err != nil || len(messages) != 0 {
			t.Fatalf("failed batch published messages: %+v err=%v", messages, err)
		}
		if stats := backend.localMessageStats.snapshot(); stats != (pebbleLocalMessageBatchStatsSnapshot{}) {
			t.Fatalf("failed batch recorded as committed: %+v", stats)
		}
	}
	requests := pebbleCPURequests([]UserKey{key}, 3, PebbleMessageSyncModeNoSync)
	requests[2].params.PebbleMessageSyncMode = PebbleMessageSyncModeForceSync
	backend.processLocalMessageBatch(requests)
	for _, request := range requests {
		if result := <-request.response; !errors.Is(result.err, ErrInvalidInput) {
			t.Fatalf("expected mixed-mode preparation failure: %+v", result)
		}
	}
	assertUnchanged()

	// 仅将创建 batch 的 DB 换成只读 DB，保留所有读取依赖，以确保失败发生在 Commit。
	path := filepath.Join(t.TempDir(), "readonly.pebble")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	originalDB := backend.db
	backend.db = ro
	requests = pebbleCPURequests([]UserKey{key}, 3, PebbleMessageSyncModeNoSync)
	backend.processLocalMessageBatch(requests)
	backend.db = originalDB
	for _, request := range requests {
		if result := <-request.response; !errors.Is(result.err, pebble.ErrReadOnly) {
			t.Fatalf("expected commit failure: %+v", result)
		}
	}
	assertUnchanged()

	requests = pebbleCPURequests([]UserKey{key}, 3, PebbleMessageSyncModeForceSync)
	backend.processLocalMessageBatch(requests)
	for i, request := range requests {
		result := <-request.response
		if result.err != nil || result.message.Seq != int64(i+1) || result.event.Sequence != base+int64(i+1) {
			t.Fatalf("recovery result %d: %+v", i, result)
		}
	}
	if state := readPebbleMessageUserStateForTest(t, st, key); state.StoredCount != 3 || state.MaxSeq != 3 {
		t.Fatalf("recovered state: %+v", state)
	}
}

func TestPebbleCPULocalBatchStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := openPersistentPebbleTestStore(t, dir, "cpu-restart", 1, 256)
	defer func() {
		if st != nil {
			if err := st.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	keys := []UserKey{
		pebbleCPUCreateUser(t, st, "first", RoleUser).Key(),
		pebbleCPUCreateUser(t, st, "second", RoleUser).Key(),
	}
	backend := requirePebbleBackend(t, st)
	requests := pebbleCPURequests(keys, 16, PebbleMessageSyncModeForceSync)
	backend.processLocalMessageBatch(requests)
	for _, request := range requests {
		if result := <-request.response; result.err != nil {
			t.Fatal(result.err)
		}
	}
	last, err := st.LastEventSequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st = nil
	st = openPersistentPebbleTestStore(t, dir, "cpu-restart", 1, 256)
	backend = requirePebbleBackend(t, st)
	for _, key := range keys {
		state := readPebbleMessageUserStateForTest(t, st, key)
		if state.StoredCount != 8 || state.MaxSeq != 8 || state.TrimNeeded {
			t.Fatalf("reopened state: %+v", state)
		}
	}
	requests = pebbleCPURequests(keys, 4, PebbleMessageSyncModeNoSync)
	backend.processLocalMessageBatch(requests)
	for i, request := range requests {
		result := <-request.response
		if result.err != nil || result.message.Seq != int64(9+i/len(keys)) || result.event.Sequence != last+int64(i+1) {
			t.Fatalf("reopened result: %+v", result)
		}
	}
	for _, key := range keys {
		state := readPebbleMessageUserStateForTest(t, st, key)
		if state.StoredCount != 10 || state.MaxSeq != 10 || state.TrimNeeded {
			t.Fatalf("continued state: %+v", state)
		}
	}
}
