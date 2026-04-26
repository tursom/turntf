package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestPebbleWriteCoordinatorSeparatesRelaxedAndForceSyncPaths(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "stats", 1, DefaultMessageWindowSize)
	backend := requirePebbleBackend(t, st)

	before := backend.writes.statsSnapshot()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "force-sync-user",
		PasswordHash: "hash-1",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	afterUser := backend.writes.statsSnapshot()
	if afterUser.ForceSyncBatches <= before.ForceSyncBatches {
		t.Fatalf("expected user event to use force sync path: before=%+v after=%+v", before, afterUser)
	}
	if afterUser.RelaxedBatches != before.RelaxedBatches {
		t.Fatalf("expected user event to avoid relaxed path: before=%+v after=%+v", before, afterUser)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  user.Key(),
		Body:    []byte("relaxed-message"),
	}); err != nil {
		t.Fatalf("create message: %v", err)
	}
	afterMessage := backend.writes.statsSnapshot()
	if afterMessage != afterUser {
		t.Fatalf("expected local message path to bypass write coordinator: user=%+v message=%+v", afterUser, afterMessage)
	}
}

func TestPebbleMetadataWritesUseRelaxedCoordinatorPath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "metadata-relaxed", 1, DefaultMessageWindowSize)
	backend := requirePebbleBackend(t, st)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "pending-user",
		PasswordHash: "hash-pending",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, event, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  user.Key(),
		Body:    []byte("pending"),
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}

	before := backend.writes.statsSnapshot()
	if err := st.RecordPeerAck(ctx, testNodeID(2), testNodeID(3), 7); err != nil {
		t.Fatalf("record peer ack: %v", err)
	}
	if err := st.RecordOriginApplied(ctx, testNodeID(3), 11); err != nil {
		t.Fatalf("record origin applied: %v", err)
	}
	if err := st.recordPendingProjection(ctx, event, errors.New("projection failed")); err != nil {
		t.Fatalf("record pending projection: %v", err)
	}
	if err := st.clearPendingProjection(ctx, event.OriginNodeID, event.EventID); err != nil {
		t.Fatalf("clear pending projection: %v", err)
	}

	after := backend.writes.statsSnapshot()
	if after.RelaxedBatches <= before.RelaxedBatches {
		t.Fatalf("expected metadata writes to use relaxed coordinator path: before=%+v after=%+v", before, after)
	}
	if after.ForceSyncBatches != before.ForceSyncBatches {
		t.Fatalf("expected metadata writes to avoid force-sync coordinator path: before=%+v after=%+v", before, after)
	}
}

func TestPebbleWriteCoordinatorFlushesBySizeAndDelay(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := pebble.Open(filepath.Join(dir, "coordinator.pebble"), &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble db: %v", err)
	}
	coordinator := newPebbleWriteCoordinator(db)
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil {
			t.Fatalf("close coordinator: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close pebble db: %v", err)
		}
	})

	for i := 0; i < groupCommitMaxOps; i++ {
		batch := db.NewBatch()
		if err := batch.Set([]byte{byte(i), 1}, []byte("v"), nil); err != nil {
			t.Fatalf("set size batch %d: %v", i, err)
		}
		if err := coordinator.Apply(batch, false); err != nil {
			t.Fatalf("apply size batch %d: %v", i, err)
		}
	}
	if stats := coordinator.statsSnapshot(); stats.FlushesBySize == 0 {
		t.Fatalf("expected size-based flush, got %+v", stats)
	}

	batch := db.NewBatch()
	if err := batch.Set([]byte("delay-key"), []byte("delay-value"), nil); err != nil {
		t.Fatalf("set delay batch: %v", err)
	}
	if err := coordinator.Apply(batch, false); err != nil {
		t.Fatalf("apply delay batch: %v", err)
	}
	waitForCoordinatorStat(t, time.Second, func(stats pebbleWriteCoordinatorStats) bool {
		return stats.FlushesByDelay > 0
	}, coordinator)
}

func TestPebbleApplyReplicatedMessageRemainsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	source := openPebbleTestStore(t, "source", 1, DefaultMessageWindowSize)
	target := openPebbleTestStore(t, "target", 2, DefaultMessageWindowSize)

	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "idempotent-user",
		PasswordHash: "hash-1",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(userEvent)); err != nil {
		t.Fatalf("apply user event: %v", err)
	}

	message, messageEvent, err := source.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  user.Key(),
		Body:    []byte("idempotent-message"),
	})
	if err != nil {
		t.Fatalf("create source message: %v", err)
	}
	replicated := ToReplicatedEvent(messageEvent)
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated message first time: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated message second time: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages: %v", err)
	}
	if len(messages) != 1 || messages[0].NodeID != message.NodeID || messages[0].Seq != message.Seq {
		t.Fatalf("expected one replicated message after duplicate apply, got %+v", messages)
	}

	events, err := target.ListEventsByOrigin(ctx, source.NodeID(), 0, 10)
	if err != nil {
		t.Fatalf("list target events by origin: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected one user event and one message event after duplicate apply, got %+v", events)
	}
}

func waitForCoordinatorStat(t *testing.T, timeout time.Duration, check func(pebbleWriteCoordinatorStats) bool, coordinator *pebbleWriteCoordinator) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check(coordinator.statsSnapshot()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("coordinator stat condition not satisfied: %+v", coordinator.statsSnapshot())
}

func requirePebbleBackend(tb testing.TB, st *Store) *pebbleStoreBackend {
	tb.Helper()

	backend, ok := st.backend.(*pebbleStoreBackend)
	if !ok {
		tb.Fatalf("expected pebble backend, got %T", st.backend)
	}
	return backend
}
