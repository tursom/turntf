package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/tursom/turntf/internal/auth"
)

func TestUserLoginNameLifecycleAndAuthentication(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	passwordHash, err := auth.HashPassword("alice-password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}

	user, events, err := st.CreateUserWithEvents(ctx, CreateUserParams{
		Username:     "alice",
		LoginName:    "alice.login",
		PasswordHash: passwordHash,
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user with login name: %v", err)
	}
	if len(events) != 2 || events[0].EventType != EventTypeUserCreated || events[1].EventType != EventTypeUserLoginNameUpserted {
		t.Fatalf("unexpected create events: %+v", events)
	}

	loginName, err := st.GetUserLoginName(ctx, user.Key())
	if err != nil {
		t.Fatalf("get user login name: %v", err)
	}
	if loginName != "alice.login" {
		t.Fatalf("unexpected login name: %q", loginName)
	}

	resolvedKey, err := st.ResolveLoginName(ctx, "alice.login")
	if err != nil {
		t.Fatalf("resolve login name: %v", err)
	}
	if resolvedKey != user.Key() {
		t.Fatalf("unexpected resolved key: %+v", resolvedKey)
	}

	authed, err := st.AuthenticateUserByLoginName(ctx, "alice.login", "alice-password")
	if err != nil {
		t.Fatalf("authenticate by login name: %v", err)
	}
	if authed.Key() != user.Key() {
		t.Fatalf("unexpected authenticated user: %+v", authed)
	}

	renamed := "alice.renamed"
	updated, renameEvents, err := st.UpdateUserWithEvents(ctx, UpdateUserParams{
		Key:       user.Key(),
		LoginName: &renamed,
	})
	if err != nil {
		t.Fatalf("rename login name: %v", err)
	}
	if updated.Key() != user.Key() {
		t.Fatalf("unexpected updated user: %+v", updated)
	}
	if len(renameEvents) != 2 || renameEvents[0].EventType != EventTypeUserLoginNameDeleted || renameEvents[1].EventType != EventTypeUserLoginNameUpserted {
		t.Fatalf("unexpected rename events: %+v", renameEvents)
	}
	if _, err := st.ResolveLoginName(ctx, "alice.login"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected old login name to disappear, got %v", err)
	}
	if got, err := st.GetUserLoginName(ctx, user.Key()); err != nil || got != renamed {
		t.Fatalf("expected renamed login name, got %q err=%v", got, err)
	}

	cleared := ""
	clearEventsUser, clearEvents, err := func() (User, []Event, error) {
		return st.UpdateUserWithEvents(ctx, UpdateUserParams{
			Key:       user.Key(),
			LoginName: &cleared,
		})
	}()
	if err != nil {
		t.Fatalf("clear login name: %v", err)
	}
	if clearEventsUser.Key() != user.Key() {
		t.Fatalf("unexpected cleared user: %+v", clearEventsUser)
	}
	if len(clearEvents) != 1 || clearEvents[0].EventType != EventTypeUserLoginNameDeleted {
		t.Fatalf("unexpected clear events: %+v", clearEvents)
	}
	if got, err := st.GetUserLoginName(ctx, user.Key()); err != nil || got != "" {
		t.Fatalf("expected empty login name after clear, got %q err=%v", got, err)
	}
	if _, err := st.AuthenticateUserByLoginName(ctx, renamed, "alice-password"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected cleared login name auth to fail, got %v", err)
	}
}

func TestUserLoginNameConflictAndRoleDowngrade(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	firstHash, err := auth.HashPassword("first-password")
	if err != nil {
		t.Fatalf("hash first password: %v", err)
	}
	secondHash, err := auth.HashPassword("second-password")
	if err != nil {
		t.Fatalf("hash second password: %v", err)
	}

	first, _, err := st.CreateUserWithEvents(ctx, CreateUserParams{
		Username:     "first",
		LoginName:    "shared.login",
		PasswordHash: firstHash,
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create first user: %v", err)
	}
	second, _, err := st.CreateUserWithEvents(ctx, CreateUserParams{
		Username:     "second",
		PasswordHash: secondHash,
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create second user: %v", err)
	}

	conflict := "shared.login"
	if _, _, err := st.UpdateUserWithEvents(ctx, UpdateUserParams{
		Key:       second.Key(),
		LoginName: &conflict,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected login name conflict, got %v", err)
	}

	channelRole := RoleChannel
	updated, downgradeEvents, err := st.UpdateUserWithEvents(ctx, UpdateUserParams{
		Key:  first.Key(),
		Role: &channelRole,
	})
	if err != nil {
		t.Fatalf("downgrade role: %v", err)
	}
	if updated.Role != RoleChannel {
		t.Fatalf("unexpected downgraded role: %s", updated.Role)
	}
	if len(downgradeEvents) != 2 || downgradeEvents[0].EventType != EventTypeUserUpdated || downgradeEvents[1].EventType != EventTypeUserLoginNameDeleted {
		t.Fatalf("unexpected downgrade events: %+v", downgradeEvents)
	}
	if _, err := st.ResolveLoginName(ctx, "shared.login"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected login name to be removed after role downgrade, got %v", err)
	}
}

func TestReplicatedUserLoginNameEvents(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "target", 2)
	defer target.Close()

	ctx := context.Background()
	passwordHash, err := auth.HashPassword("alice-password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}

	user, createEvents, err := source.CreateUserWithEvents(ctx, CreateUserParams{
		Username:     "alice",
		LoginName:    "alice.login",
		PasswordHash: passwordHash,
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	applyReplicatedEventsForTest(t, target, createEvents)

	if got, err := target.GetUserLoginName(ctx, user.Key()); err != nil || got != "alice.login" {
		t.Fatalf("expected replicated login name, got %q err=%v", got, err)
	}

	renamed := "alice.replica"
	_, renameEvents, err := source.UpdateUserWithEvents(ctx, UpdateUserParams{
		Key:       user.Key(),
		LoginName: &renamed,
	})
	if err != nil {
		t.Fatalf("rename source login name: %v", err)
	}
	applyReplicatedEventsForTest(t, target, renameEvents)

	if _, err := target.ResolveLoginName(ctx, "alice.login"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected old replicated login name to disappear, got %v", err)
	}
	if got, err := target.GetUserLoginName(ctx, user.Key()); err != nil || got != renamed {
		t.Fatalf("expected replicated renamed login name, got %q err=%v", got, err)
	}
}

func TestSnapshotIncludesLoginNames(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "target", 2)
	defer target.Close()

	ctx := context.Background()
	passwordHash, err := auth.HashPassword("snapshot-password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	user, _, err := source.CreateUserWithEvents(ctx, CreateUserParams{
		Username:     "snapshot-user",
		LoginName:    "snapshot.login",
		PasswordHash: passwordHash,
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create snapshot user: %v", err)
	}

	digest, err := source.BuildSnapshotDigest(ctx, nil)
	if err != nil {
		t.Fatalf("build snapshot digest: %v", err)
	}
	foundLoginNames := false
	for _, part := range digest.Partitions {
		if part.Partition == SnapshotLoginNamesPartition {
			foundLoginNames = true
			if part.Kind != snapshotPartitionKindLoginNames || part.RowCount != 1 {
				t.Fatalf("unexpected login name digest partition: %+v", part)
			}
		}
	}
	if !foundLoginNames {
		t.Fatalf("expected login names partition in snapshot digest")
	}

	usersChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, usersChunk); err != nil {
		t.Fatalf("apply users chunk: %v", err)
	}
	loginNamesChunk, err := source.BuildSnapshotChunk(ctx, SnapshotLoginNamesPartition)
	if err != nil {
		t.Fatalf("build login names chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, loginNamesChunk); err != nil {
		t.Fatalf("apply login names chunk: %v", err)
	}

	if got, err := target.GetUserLoginName(ctx, user.Key()); err != nil || got != "snapshot.login" {
		t.Fatalf("expected snapshot login name, got %q err=%v", got, err)
	}
}

func TestSchemaVersion16MigratesLoginNamesTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "migrate.db")
	st, err := Open(dbPath, Options{NodeID: testNodeID(1)})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(ctx); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `DROP TABLE user_login_names`); err != nil {
		t.Fatalf("drop login names table: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, previousSchemaVersion); err != nil {
		t.Fatalf("downgrade schema version: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(dbPath, Options{NodeID: testNodeID(1)})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	if err := reopened.Init(ctx); err != nil {
		t.Fatalf("reinit migrated store: %v", err)
	}

	passwordHash, err := auth.HashPassword("migrate-password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	user, _, err := reopened.CreateUserWithEvents(ctx, CreateUserParams{
		Username:     "migrated",
		LoginName:    "migrated.login",
		PasswordHash: passwordHash,
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user after migration: %v", err)
	}
	if got, err := reopened.GetUserLoginName(ctx, user.Key()); err != nil || got != "migrated.login" {
		t.Fatalf("expected migrated login name support, got %q err=%v", got, err)
	}
}

func applyReplicatedEventsForTest(t *testing.T, target *Store, events []Event) {
	t.Helper()

	for _, event := range events {
		replicated := ToReplicatedEvent(event)
		if replicated == nil {
			t.Fatalf("marshal replicated event: %+v", event)
		}
		if err := target.ApplyReplicatedEvent(context.Background(), replicated); err != nil {
			t.Fatalf("apply replicated event %s: %v", event.EventType, err)
		}
	}
}
