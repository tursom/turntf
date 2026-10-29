package store

import (
	"bytes"
	"context"
	"testing"
)

func assertUsersSnapshotEqual(t *testing.T, source, target *Store) {
	t.Helper()
	a, err := source.BuildSnapshotChunk(context.Background(), SnapshotUsersPartition)
	if err != nil {
		t.Fatal(err)
	}
	b, err := target.BuildSnapshotChunk(context.Background(), SnapshotUsersPartition)
	if err != nil {
		t.Fatal(err)
	}
	ha, err := hashSnapshotRows(a.Rows)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := hashSnapshotRows(b.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Rows) != len(b.Rows) || !bytes.Equal(ha, hb) {
		t.Fatalf("users snapshot did not converge: source rows=%d target rows=%d", len(a.Rows), len(b.Rows))
	}
}

func TestSnapshotDeletedBeforeJoinConverges(t *testing.T) {
	for _, tombstoneFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "tombstone-first"}[tombstoneFirst], func(t *testing.T) {
			ctx := context.Background()
			source := openNamedTestStore(t, "source", 1)
			defer source.Close()
			target := openNamedTestStore(t, "target", 2)
			defer target.Close()
			user, create, err := source.CreateUser(ctx, CreateUserParams{Username: "deleted-before-join", PasswordHash: "test"})
			if err != nil {
				t.Fatal(err)
			}
			deleted, err := source.DeleteUser(ctx, user.Key())
			if err != nil {
				t.Fatal(err)
			}
			if tombstoneFirst {
				if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(deleted)); err != nil {
					t.Fatal(err)
				}
			}
			chunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
					t.Fatal(err)
				}
				assertUsersSnapshotEqual(t, source, target)
			}
			if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(create)); err != nil {
				t.Fatal(err)
			}
			if _, err := target.GetUser(ctx, user.Key()); err != ErrNotFound {
				t.Fatalf("deleted user became visible: %v", err)
			}
			assertUsersSnapshotEqual(t, source, target)
		})
	}
}

func TestSnapshotDeletedUserKeepsRemoteDeleteOrigin(t *testing.T) {
	ctx := context.Background()
	creator := openNamedTestStore(t, "creator", 1)
	defer creator.Close()
	deleter := openNamedTestStore(t, "deleter", 2)
	defer deleter.Close()
	target := openNamedTestStore(t, "target", 3)
	defer target.Close()
	user, create, err := creator.CreateUser(ctx, CreateUserParams{Username: "remote-delete", PasswordHash: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := deleter.ApplyReplicatedEvent(ctx, ToReplicatedEvent(create)); err != nil {
		t.Fatal(err)
	}
	if _, err := deleter.DeleteUser(ctx, user.Key()); err != nil {
		t.Fatal(err)
	}
	chunk, err := deleter.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatal(err)
	}
	assertUsersSnapshotEqual(t, deleter, target)
	var origin int64
	if err := target.db.QueryRowContext(ctx, "SELECT origin_node_id FROM tombstones WHERE entity_type='user' AND entity_node_id=? AND entity_id=?", user.NodeID, user.ID).Scan(&origin); err != nil {
		t.Fatal(err)
	}
	if origin != deleter.nodeID {
		t.Fatalf("delete origin=%d want=%d", origin, deleter.nodeID)
	}
}

func TestSnapshotDeletedHistoryMergesMissingUpdates(t *testing.T) {
	ctx := context.Background()
	source := openNamedTestStore(t, "source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "target", 2)
	defer target.Close()
	user, create, err := source.CreateUser(ctx, CreateUserParams{Username: "before-update", PasswordHash: "old"})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(create)); err != nil {
		t.Fatal(err)
	}
	name, profile := "updated-before-delete", `{"changed":true}`
	if _, _, err := source.UpdateUser(ctx, UpdateUserParams{Key: user.Key(), Username: &name, Profile: &profile}); err != nil {
		t.Fatal(err)
	}
	deleted, err := source.DeleteUser(ctx, user.Key())
	if err != nil {
		t.Fatal(err)
	}
	// Target missed the update but already received the delete. It must merge
	// the historical fields without making the deleted account visible again.
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(deleted)); err != nil {
		t.Fatal(err)
	}
	chunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatal(err)
	}
	assertUsersSnapshotEqual(t, source, target)
	if _, err := target.GetUser(ctx, user.Key()); err != ErrNotFound {
		t.Fatalf("deleted user became visible: %v", err)
	}
}

func TestSnapshotDeletedHistoryPreservesNewerTombstone(t *testing.T) {
	ctx := context.Background()
	source := openNamedTestStore(t, "source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "target", 2)
	defer target.Close()
	user, _, err := source.CreateUser(ctx, CreateUserParams{Username: "old-deleted-snapshot", PasswordHash: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.DeleteUser(ctx, user.Key()); err != nil {
		t.Fatal(err)
	}
	chunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatal(err)
	}
	newer := target.clock.Now()
	tx, err := target.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.applyUserDeleteTx(ctx, tx, user.Key(), newer, target.nodeID, false); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatal(err)
	}
	var version string
	if err := target.db.QueryRowContext(ctx, "SELECT version_deleted FROM users WHERE node_id=? AND user_id=?", user.NodeID, user.ID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != newer.String() {
		t.Fatalf("old snapshot replaced newer deletion: %s", version)
	}
	if _, err := target.GetUser(ctx, user.Key()); err != ErrNotFound {
		t.Fatalf("deleted user became visible: %v", err)
	}
}
