package store

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	"notifier/internal/clock"
)

func TestLocalUserCRUDAndEventLog(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, createEvent, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Alice"}`,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if createEvent.Kind != "user.created" {
		t.Fatalf("unexpected create event kind: %s", createEvent.Kind)
	}

	loaded, err := st.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if loaded.Username != "alice" {
		t.Fatalf("unexpected username: %s", loaded.Username)
	}

	newUsername := "alice-updated"
	newProfile := `{"display_name":"Alice Updated"}`
	newPasswordHash := "hash-2"
	updated, updateEvent, err := st.UpdateUser(ctx, UpdateUserParams{
		UserID:       user.ID,
		Username:     &newUsername,
		Profile:      &newProfile,
		PasswordHash: &newPasswordHash,
	})
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	if updateEvent.Kind != "user.updated" {
		t.Fatalf("unexpected update event kind: %s", updateEvent.Kind)
	}
	if updated.Username != newUsername || updated.Profile != newProfile || updated.PasswordHash != newPasswordHash {
		t.Fatalf("unexpected updated user: %+v", updated)
	}

	users, err := st.ListUsers(ctx)
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 active user, got %d", len(users))
	}

	deleteEvent, err := st.DeleteUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if deleteEvent.Kind != "user.deleted" {
		t.Fatalf("unexpected delete event kind: %s", deleteEvent.Kind)
	}

	if _, err := st.GetUser(ctx, user.ID); err != ErrNotFound {
		t.Fatalf("expected not found after delete, got %v", err)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Sequence >= events[1].Sequence || events[1].Sequence >= events[2].Sequence {
		t.Fatalf("events not ordered by sequence: %+v", events)
	}
}

func TestEnsureBootstrapAdminCreatesAndProtectsReservedUser(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: "hash-root",
	}); err != nil {
		t.Fatalf("ensure bootstrap admin: %v", err)
	}

	user, err := st.GetUser(ctx, BootstrapAdminUserID)
	if err != nil {
		t.Fatalf("get bootstrap admin: %v", err)
	}
	if user.Username != "root" || user.Role != RoleSuperAdmin || !user.SystemReserved {
		t.Fatalf("unexpected bootstrap admin: %+v", user)
	}

	newPasswordHash := "hash-root-2"
	updated, _, err := st.UpdateUser(ctx, UpdateUserParams{
		UserID:       BootstrapAdminUserID,
		PasswordHash: &newPasswordHash,
	})
	if err != nil {
		t.Fatalf("update bootstrap admin password: %v", err)
	}
	if updated.PasswordHash != newPasswordHash {
		t.Fatalf("expected updated password hash, got %+v", updated)
	}

	newUsername := "renamed-root"
	if _, _, err := st.UpdateUser(ctx, UpdateUserParams{
		UserID:   BootstrapAdminUserID,
		Username: &newUsername,
	}); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected rename bootstrap admin to fail with forbidden, got %v", err)
	}

	newRole := RoleAdmin
	if _, _, err := st.UpdateUser(ctx, UpdateUserParams{
		UserID: BootstrapAdminUserID,
		Role:   &newRole,
	}); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected downgrade bootstrap admin to fail with forbidden, got %v", err)
	}

	if _, err := st.DeleteUser(ctx, BootstrapAdminUserID); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected delete bootstrap admin to fail with forbidden, got %v", err)
	}
}

func TestEnsureBootstrapAdminRepairsReservedUserWithoutOverwritingPassword(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: "hash-root",
	}); err != nil {
		t.Fatalf("ensure bootstrap admin: %v", err)
	}

	now := st.clock.Now()
	if _, err := st.db.ExecContext(ctx, `
UPDATE users
SET username = ?, role = ?, system_reserved = 0, deleted_at_hlc = ?, version_deleted = ?, updated_at_hlc = ?, password_hash = ?
WHERE user_id = ?
`, "broken-root", RoleAdmin, now.String(), now.String(), now.String(), "preserve-hash", BootstrapAdminUserID); err != nil {
		t.Fatalf("corrupt bootstrap admin: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO tombstones(entity_type, entity_id, deleted_at_hlc, expires_at_hlc, origin_node_id)
VALUES('user', ?, ?, NULL, ?)
`, BootstrapAdminUserID, now.String(), "node-a"); err != nil {
		t.Fatalf("insert bootstrap tombstone: %v", err)
	}

	if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{
		Username:     "root-fixed",
		PasswordHash: "ignored-hash",
	}); err != nil {
		t.Fatalf("repair bootstrap admin: %v", err)
	}

	user, err := st.GetUser(ctx, BootstrapAdminUserID)
	if err != nil {
		t.Fatalf("get repaired bootstrap admin: %v", err)
	}
	if user.Username != "root-fixed" || user.Role != RoleSuperAdmin || !user.SystemReserved {
		t.Fatalf("unexpected repaired bootstrap admin: %+v", user)
	}
	if user.PasswordHash != "preserve-hash" {
		t.Fatalf("expected password hash to be preserved, got %+v", user)
	}

	var tombstones int
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM tombstones
WHERE entity_type = 'user' AND entity_id = ?
`, BootstrapAdminUserID).Scan(&tombstones); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if tombstones != 0 {
		t.Fatalf("expected bootstrap tombstone to be cleared, got %d", tombstones)
	}
}

func TestLocalMessageWriteAndQuery(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	first, firstEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserID:   user.ID,
		Sender:   "orders",
		Body:     "first message",
		Metadata: `{"kind":"first"}`,
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if firstEvent.Kind != "message.created" {
		t.Fatalf("unexpected event kind: %s", firstEvent.Kind)
	}

	second, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "second message",
	})
	if err != nil {
		t.Fatalf("create second message: %v", err)
	}

	messages, err := st.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if first.NodeID != "node-a" || first.Seq != 1 || second.NodeID != "node-a" || second.Seq != 2 {
		t.Fatalf("unexpected message identities: first=%+v second=%+v", first, second)
	}
	if messages[0].Seq != second.Seq || messages[1].Seq != first.Seq {
		t.Fatalf("messages not ordered newest-first: %+v", messages)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events (user + 2 message), got %d", len(events))
	}
}

func TestLocalMessagesTrimToConfiguredWindow(t *testing.T) {
	t.Parallel()

	st := openNamedTestStoreWithWindow(t, "node-a", 1, 2)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "trim-local",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserID: user.ID,
			Sender: "orders",
			Body:   "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	messages, err := st.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after trim, got %d", len(messages))
	}
	if messages[0].Body != "message-3" || messages[1].Body != "message-2" {
		t.Fatalf("unexpected trimmed messages: %+v", messages)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events retained in log, got %d", len(events))
	}

	stats, err := st.OperationsStats(ctx, nil)
	if err != nil {
		t.Fatalf("operations stats: %v", err)
	}
	if stats.MessageTrim.TrimmedTotal != 1 || stats.MessageTrim.LastTrimmedAt == nil {
		t.Fatalf("unexpected message trim stats: %+v", stats.MessageTrim)
	}
}

func TestDuplicateActiveUsernameAllowed(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	if _, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "carol",
		PasswordHash: "hash-1",
	}); err != nil {
		t.Fatalf("create user: %v", err)
	}

	second, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "carol",
		PasswordHash: "hash-2",
	})
	if err != nil {
		t.Fatalf("create duplicate username: %v", err)
	}
	if second.ID == 0 {
		t.Fatalf("expected second user id")
	}

	users, err := st.ListUsers(ctx)
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
}

func TestRenameUserToDuplicateUsernameAllowed(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	first, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "carol",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create first user: %v", err)
	}
	second, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "dave",
		PasswordHash: "hash-2",
	})
	if err != nil {
		t.Fatalf("create second user: %v", err)
	}

	duplicate := first.Username
	updated, _, err := st.UpdateUser(ctx, UpdateUserParams{
		UserID:   second.ID,
		Username: &duplicate,
	})
	if err != nil {
		t.Fatalf("rename to duplicate username: %v", err)
	}
	if updated.Username != first.Username {
		t.Fatalf("unexpected updated username: %+v", updated)
	}
}

func TestPeerCursorSchemaAndMonotonicUpdates(t *testing.T) {
	t.Parallel()

	st := openNamedTestStore(t, "node-b", 2)
	defer st.Close()

	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx, `
SELECT acked_sequence, applied_sequence
FROM peer_cursors
`)
	if err != nil {
		t.Fatalf("peer cursor schema query: %v", err)
	}
	rows.Close()

	if err := st.RecordPeerAck(ctx, "node-a", 5); err != nil {
		t.Fatalf("record peer ack: %v", err)
	}
	if err := st.RecordPeerAck(ctx, "node-a", 3); err != nil {
		t.Fatalf("record stale peer ack: %v", err)
	}
	if err := st.RecordPeerApplied(ctx, "node-a", 7); err != nil {
		t.Fatalf("record peer applied: %v", err)
	}
	if err := st.RecordPeerApplied(ctx, "node-a", 4); err != nil {
		t.Fatalf("record stale peer applied: %v", err)
	}

	cursor, err := st.GetPeerCursor(ctx, "node-a")
	if err != nil {
		t.Fatalf("get peer cursor: %v", err)
	}
	if cursor.AckedSequence != 5 {
		t.Fatalf("unexpected acked sequence: got=%d want=5", cursor.AckedSequence)
	}
	if cursor.AppliedSequence != 7 {
		t.Fatalf("unexpected applied sequence: got=%d want=7", cursor.AppliedSequence)
	}
}

func TestOperationsStatsIncludesPeerCursorsConflictsAndTrimStats(t *testing.T) {
	t.Parallel()

	st := openNamedTestStoreWithWindow(t, "node-b", 2, 1)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "ops-stats-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserID: user.ID,
			Sender: "orders",
			Body:   "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	if err := st.RecordPeerAck(ctx, "node-a", 1); err != nil {
		t.Fatalf("record peer ack: %v", err)
	}
	if err := st.RecordPeerApplied(ctx, "node-a", 2); err != nil {
		t.Fatalf("record peer applied: %v", err)
	}
	now := st.clock.Now().String()
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO user_conflicts(loser_user_id, winner_user_id, username, detected_at_hlc)
VALUES(?, ?, ?, ?)
`, int64(10), int64(11), "conflicted", now); err != nil {
		t.Fatalf("insert user conflict: %v", err)
	}

	stats, err := st.OperationsStats(ctx, []string{"node-a", "node-c"})
	if err != nil {
		t.Fatalf("operations stats: %v", err)
	}
	if stats.NodeID != "node-b" || stats.MessageWindowSize != 1 || stats.LastEventSequence != 3 {
		t.Fatalf("unexpected top-level stats: %+v", stats)
	}
	if stats.UserConflictsTotal != 1 {
		t.Fatalf("unexpected conflict count: %+v", stats)
	}
	if stats.MessageTrim.TrimmedTotal != 1 || stats.MessageTrim.LastTrimmedAt == nil {
		t.Fatalf("unexpected trim stats: %+v", stats.MessageTrim)
	}
	if len(stats.PeerCursors) != 2 {
		t.Fatalf("expected configured peer stats, got %+v", stats.PeerCursors)
	}
	if stats.PeerCursors[0].PeerNodeID != "node-a" ||
		stats.PeerCursors[0].AckedSequence != 1 ||
		stats.PeerCursors[0].AppliedSequence != 2 ||
		stats.PeerCursors[0].UnconfirmedEvents != 2 ||
		stats.PeerCursors[0].UpdatedAt == nil {
		t.Fatalf("unexpected node-a cursor stats: %+v", stats.PeerCursors[0])
	}
	if stats.PeerCursors[1].PeerNodeID != "node-c" ||
		stats.PeerCursors[1].UnconfirmedEvents != 3 ||
		stats.PeerCursors[1].UpdatedAt != nil {
		t.Fatalf("unexpected node-c cursor stats: %+v", stats.PeerCursors[1])
	}
}

func TestApplyReplicatedEventIsIdempotent(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, event, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-user",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Replicated"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	replicated := ToReplicatedEvent(event)
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated event second time: %v", err)
	}

	loaded, err := target.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get replicated user: %v", err)
	}
	if loaded.Username != "replicated-user" {
		t.Fatalf("unexpected replicated user: %+v", loaded)
	}

	events, err := target.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list target events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 replicated event, got %d", len(events))
	}
}

func TestReplicatedDuplicateUsernameUsersRemainDistinctByID(t *testing.T) {
	t.Parallel()

	sourceA := openNamedTestStore(t, "node-a", 1)
	defer sourceA.Close()

	sourceB := openNamedTestStore(t, "node-b", 2)
	defer sourceB.Close()

	observerAB := openNamedTestStore(t, "node-c", 3)
	defer observerAB.Close()

	observerBA := openNamedTestStore(t, "node-d", 4)
	defer observerBA.Close()

	ctx := context.Background()
	userA, eventA, err := sourceA.CreateUser(ctx, CreateUserParams{
		Username:     "shared-name",
		PasswordHash: "hash-a",
	})
	if err != nil {
		t.Fatalf("create source A user: %v", err)
	}
	userB, eventB, err := sourceB.CreateUser(ctx, CreateUserParams{
		Username:     "shared-name",
		PasswordHash: "hash-b",
	})
	if err != nil {
		t.Fatalf("create source B user: %v", err)
	}
	if userA.ID == userB.ID {
		t.Fatalf("expected distinct replicated user ids, both were %d", userA.ID)
	}

	if err := observerAB.ApplyReplicatedEvent(ctx, ToReplicatedEvent(eventA)); err != nil {
		t.Fatalf("apply source A event to observer AB: %v", err)
	}
	if err := observerAB.ApplyReplicatedEvent(ctx, ToReplicatedEvent(eventB)); err != nil {
		t.Fatalf("apply source B event to observer AB: %v", err)
	}
	if err := observerBA.ApplyReplicatedEvent(ctx, ToReplicatedEvent(eventB)); err != nil {
		t.Fatalf("apply source B event to observer BA: %v", err)
	}
	if err := observerBA.ApplyReplicatedEvent(ctx, ToReplicatedEvent(eventA)); err != nil {
		t.Fatalf("apply source A event to observer BA: %v", err)
	}

	assertUsersDistinctByID := func(t *testing.T, st *Store) {
		t.Helper()

		users, err := st.ListUsers(ctx)
		if err != nil {
			t.Fatalf("list replicated users: %v", err)
		}
		if len(users) != 2 {
			t.Fatalf("expected 2 users with duplicate username, got %d: %+v", len(users), users)
		}

		byID := make(map[int64]User, len(users))
		for _, user := range users {
			byID[user.ID] = user
		}
		if byID[userA.ID].Username != "shared-name" || byID[userA.ID].PasswordHash != "hash-a" {
			t.Fatalf("missing or changed source A user: %+v", byID[userA.ID])
		}
		if byID[userB.ID].Username != "shared-name" || byID[userB.ID].PasswordHash != "hash-b" {
			t.Fatalf("missing or changed source B user: %+v", byID[userB.ID])
		}
	}

	assertUsersDistinctByID(t, observerAB)
	assertUsersDistinctByID(t, observerBA)
}

func TestApplyReplicatedMessageIsIdempotent(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-message-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(userEvent)); err != nil {
		t.Fatalf("apply user event: %v", err)
	}

	message, messageEvent, err := source.CreateMessage(ctx, CreateMessageParams{
		UserID:   user.ID,
		Sender:   "orders",
		Body:     "hello cluster",
		Metadata: `{"kind":"replicated"}`,
	})
	if err != nil {
		t.Fatalf("create source message: %v", err)
	}

	replicated := ToReplicatedEvent(messageEvent)
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply message event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply message event second time: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list replicated messages: %v", err)
	}
	if len(messages) != 1 || messages[0].NodeID != message.NodeID || messages[0].Seq != message.Seq {
		t.Fatalf("unexpected replicated messages: %+v", messages)
	}
}

func TestReplicatedMessagesTrimToConfiguredWindow(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStoreWithWindow(t, "node-b", 2, 2)
	defer target.Close()

	ctx := context.Background()
	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-trim-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(userEvent)); err != nil {
		t.Fatalf("apply user event: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, messageEvent, err := source.CreateMessage(ctx, CreateMessageParams{
			UserID: user.ID,
			Sender: "orders",
			Body:   "message-" + strconv.Itoa(i),
		})
		if err != nil {
			t.Fatalf("create source message %d: %v", i, err)
		}
		if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(messageEvent)); err != nil {
			t.Fatalf("apply message event %d: %v", i, err)
		}
	}

	messages, err := target.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list replicated messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after replicated trim, got %d", len(messages))
	}
	if messages[0].Body != "message-3" || messages[1].Body != "message-2" {
		t.Fatalf("unexpected trimmed replicated messages: %+v", messages)
	}
}

func TestReplicatedMessageTrimUsesNodeAndSeqForSameTimestamp(t *testing.T) {
	t.Parallel()

	target := openNamedTestStoreWithWindow(t, "node-b", 2, 1)
	defer target.Close()

	ctx := context.Background()
	user, _, err := target.CreateUser(ctx, CreateUserParams{
		Username:     "same-ts-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	sharedHLC := clock.Timestamp{WallTimeMs: 1700000000000, Logical: 7, NodeID: 1}
	firstEventID := target.ids.Next()
	secondEventID := target.ids.Next()

	firstPayload, err := marshalPayload(replicatedMessageEnvelope{
		Message: replicatedMessagePayload{
			UserID:    user.ID,
			NodeID:    "node-a",
			Seq:       1,
			Sender:    "orders",
			Body:      "older-seq",
			CreatedAt: sharedHLC.String(),
		},
	})
	if err != nil {
		t.Fatalf("marshal first payload: %v", err)
	}
	secondPayload, err := marshalPayload(replicatedMessageEnvelope{
		Message: replicatedMessagePayload{
			UserID:    user.ID,
			NodeID:    "node-a",
			Seq:       2,
			Sender:    "orders",
			Body:      "newer-seq",
			CreatedAt: sharedHLC.String(),
		},
	})
	if err != nil {
		t.Fatalf("marshal second payload: %v", err)
	}

	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(Event{
		EventID:      firstEventID,
		Kind:         "message.created",
		Aggregate:    "message",
		AggregateID:  1,
		HLC:          sharedHLC,
		OriginNodeID: "node-a",
		Payload:      firstPayload,
	})); err != nil {
		t.Fatalf("apply first replicated event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(Event{
		EventID:      secondEventID,
		Kind:         "message.created",
		Aggregate:    "message",
		AggregateID:  2,
		HLC:          sharedHLC,
		OriginNodeID: "node-a",
		Payload:      secondPayload,
	})); err != nil {
		t.Fatalf("apply second replicated event: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message after trim, got %d", len(messages))
	}
	if messages[0].NodeID != "node-a" || messages[0].Seq != 2 || messages[0].Body != "newer-seq" {
		t.Fatalf("expected higher seq to win tie, got %+v", messages[0])
	}
}

func TestReplicatedUserUpdatesMergeDifferentFields(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "merge-user",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Alice"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
		t.Fatalf("replicate create event: %v", err)
	}

	nextUsername := "merge-user-renamed"
	sourceUpdated, updateUsernameEvent, err := source.UpdateUser(ctx, UpdateUserParams{
		UserID:   user.ID,
		Username: &nextUsername,
	})
	if err != nil {
		t.Fatalf("update source username: %v", err)
	}

	nextProfile := `{"display_name":"Merged On Target"}`
	targetUpdated, updateProfileEvent, err := target.UpdateUser(ctx, UpdateUserParams{
		UserID:  user.ID,
		Profile: &nextProfile,
	})
	if err != nil {
		t.Fatalf("update target profile: %v", err)
	}

	if err := source.ApplyReplicatedEvent(ctx, ToReplicatedEvent(updateProfileEvent)); err != nil {
		t.Fatalf("apply target profile update to source: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(updateUsernameEvent)); err != nil {
		t.Fatalf("apply source username update to target: %v", err)
	}

	sourceMerged, err := source.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get merged source user: %v", err)
	}
	targetMerged, err := target.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get merged target user: %v", err)
	}

	if sourceMerged.Username != sourceUpdated.Username || targetMerged.Username != sourceUpdated.Username {
		t.Fatalf("expected merged username %q, source=%+v target=%+v", sourceUpdated.Username, sourceMerged, targetMerged)
	}
	if sourceMerged.Profile != targetUpdated.Profile || targetMerged.Profile != targetUpdated.Profile {
		t.Fatalf("expected merged profile %q, source=%+v target=%+v", targetUpdated.Profile, sourceMerged, targetMerged)
	}
}

func TestReplicatedUserUpdatesLatestFieldWins(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "winner-base",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
		t.Fatalf("replicate create event: %v", err)
	}

	firstName := "winner-from-source"
	_, firstUpdateEvent, err := source.UpdateUser(ctx, UpdateUserParams{
		UserID:   user.ID,
		Username: &firstName,
	})
	if err != nil {
		t.Fatalf("update source username: %v", err)
	}

	target.clock.Observe(clock.Timestamp{
		WallTimeMs: source.clock.Now().WallTimeMs + 100,
		Logical:    0,
		NodeID:     1,
	})

	secondName := "winner-from-target"
	targetUpdated, secondUpdateEvent, err := target.UpdateUser(ctx, UpdateUserParams{
		UserID:   user.ID,
		Username: &secondName,
	})
	if err != nil {
		t.Fatalf("update target username: %v", err)
	}

	if err := source.ApplyReplicatedEvent(ctx, ToReplicatedEvent(secondUpdateEvent)); err != nil {
		t.Fatalf("apply later target update to source: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(firstUpdateEvent)); err != nil {
		t.Fatalf("apply earlier source update to target: %v", err)
	}

	sourceMerged, err := source.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get merged source user: %v", err)
	}
	targetMerged, err := target.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get merged target user: %v", err)
	}

	if sourceMerged.Username != targetUpdated.Username || targetMerged.Username != targetUpdated.Username {
		t.Fatalf("expected latest username %q, source=%+v target=%+v", targetUpdated.Username, sourceMerged, targetMerged)
	}
}

func TestReplicatedDeletePreventsResurrection(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	replica := openNamedTestStore(t, "node-b", 2)
	defer replica.Close()

	observer := openNamedTestStore(t, "node-c", 3)
	defer observer.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "tombstone-user",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Before Delete"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	for _, st := range []*Store{replica, observer} {
		if err := st.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
			t.Fatalf("replicate create event: %v", err)
		}
	}

	newProfile := `{"display_name":"Stale Update"}`
	_, staleUpdateEvent, err := replica.UpdateUser(ctx, UpdateUserParams{
		UserID:  user.ID,
		Profile: &newProfile,
	})
	if err != nil {
		t.Fatalf("create stale update event: %v", err)
	}

	deleteEvent, err := source.DeleteUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("delete source user: %v", err)
	}

	if err := observer.ApplyReplicatedEvent(ctx, ToReplicatedEvent(deleteEvent)); err != nil {
		t.Fatalf("apply delete event: %v", err)
	}
	if err := observer.ApplyReplicatedEvent(ctx, ToReplicatedEvent(staleUpdateEvent)); err != nil {
		t.Fatalf("apply stale update event after delete: %v", err)
	}

	if _, err := observer.GetUser(ctx, user.ID); err != ErrNotFound {
		t.Fatalf("expected deleted user to stay hidden, got %v", err)
	}
}

func TestReplicatedUpdateUpsertsWithoutRegressingOnOlderCreate(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "upsert-base",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Initial"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	updatedName := "upsert-updated"
	updatedProfile := `{"display_name":"Updated"}`
	updatedUser, updateEvent, err := source.UpdateUser(ctx, UpdateUserParams{
		UserID:   user.ID,
		Username: &updatedName,
		Profile:  &updatedProfile,
	})
	if err != nil {
		t.Fatalf("update source user: %v", err)
	}

	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(updateEvent)); err != nil {
		t.Fatalf("apply update before create: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
		t.Fatalf("apply older create after update: %v", err)
	}

	loaded, err := target.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get upserted user: %v", err)
	}
	if loaded.Username != updatedUser.Username || loaded.Profile != updatedUser.Profile {
		t.Fatalf("expected updated user to remain after older create, got %+v", loaded)
	}
}

func TestSnapshotUsersChunkRepairsDeletedUser(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-deleted",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
		t.Fatalf("apply create to target: %v", err)
	}
	if _, err := source.DeleteUser(ctx, user.ID); err != nil {
		t.Fatalf("delete source user: %v", err)
	}

	chunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}

	if _, err := target.GetUser(ctx, user.ID); err != ErrNotFound {
		t.Fatalf("expected snapshot tombstone to hide user, got %v", err)
	}
}

func TestSnapshotMessagesChunkIsIdempotentAndTrimsToLocalWindow(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStoreWithWindow(t, "node-b", 2, 2)
	defer target.Close()

	ctx := context.Background()
	user, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-message-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, _, err := source.CreateMessage(ctx, CreateMessageParams{
			UserID: user.ID,
			Sender: "orders",
			Body:   "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create source message %d: %v", i, err)
		}
	}

	userChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, userChunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}

	messageChunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition("node-a"))
	if err != nil {
		t.Fatalf("build messages snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, messageChunk); err != nil {
		t.Fatalf("apply messages snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, messageChunk); err != nil {
		t.Fatalf("apply messages snapshot chunk second time: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list target messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after snapshot trim, got %d", len(messages))
	}
	if messages[0].Body != "message-3" || messages[1].Body != "message-2" {
		t.Fatalf("unexpected snapshot messages: %+v", messages)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()

	return openNamedTestStore(t, "node-a", 1)
}

func openNamedTestStore(t *testing.T, nodeID string, nodeSlot uint16) *Store {
	t.Helper()
	return openNamedTestStoreWithWindow(t, nodeID, nodeSlot, DefaultMessageWindowSize)
}

func openNamedTestStoreWithWindow(t *testing.T, nodeID string, nodeSlot uint16, messageWindowSize int) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "distributed.db")
	st, err := Open(dbPath, Options{
		NodeID:            nodeID,
		NodeSlot:          nodeSlot,
		MessageWindowSize: messageWindowSize,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return st
}
