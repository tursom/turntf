package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	proto "github.com/tursom/turntf/internal/proto"
)

func testNodeID(slot uint16) int64 {
	return int64(slot) << 12
}

func bootstrapKey(st *Store) UserKey {
	return UserKey{NodeID: st.NodeID(), UserID: BootstrapAdminUserID}
}

func testSenderKey(slot uint16, userID int64) UserKey {
	return UserKey{NodeID: testNodeID(slot), UserID: userID}
}

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
	if createEvent.EventType != EventTypeUserCreated {
		t.Fatalf("unexpected create event type: %s", createEvent.EventType)
	}

	loaded, err := st.GetUser(ctx, user.Key())
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
		Key:          user.Key(),
		Username:     &newUsername,
		Profile:      &newProfile,
		PasswordHash: &newPasswordHash,
	})
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	if updateEvent.EventType != EventTypeUserUpdated {
		t.Fatalf("unexpected update event type: %s", updateEvent.EventType)
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

	deleteEvent, err := st.DeleteUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if deleteEvent.EventType != EventTypeUserDeleted {
		t.Fatalf("unexpected delete event type: %s", deleteEvent.EventType)
	}

	if _, err := st.GetUser(ctx, user.Key()); err != ErrNotFound {
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

func TestListEventsIncludesMessageCreated(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	newUsername := "alice-updated"
	if _, _, err := st.UpdateUser(ctx, UpdateUserParams{
		Key:      user.Key(),
		Username: &newUsername,
	}); err != nil {
		t.Fatalf("update user: %v", err)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("package shipped"),
	}); err != nil {
		t.Fatalf("create message: %v", err)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[2].EventType != EventTypeMessageCreated {
		t.Fatalf("unexpected message event type: %+v", events[2])
	}
}

func TestEventLogStoresReplicatedEventBlob(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, event, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "blob-user",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Blob User"}`,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	var (
		eventID      int64
		originNodeID int64
		value        []byte
	)
	if err := st.db.QueryRowContext(ctx, `
SELECT event_id, origin_node_id, value
FROM event_log
WHERE sequence = ?
`, event.Sequence).Scan(&eventID, &originNodeID, &value); err != nil {
		t.Fatalf("read event log row: %v", err)
	}
	if eventID != event.EventID || originNodeID != event.OriginNodeID {
		t.Fatalf("unexpected event log indexes: event_id=%d origin_node_id=%d", eventID, originNodeID)
	}
	if json.Valid(value) {
		t.Fatalf("expected protobuf blob, got json-looking payload: %q", string(value))
	}

	var replicated proto.ReplicatedEvent
	if err := gproto.Unmarshal(value, &replicated); err != nil {
		t.Fatalf("unmarshal blob value: %v", err)
	}
	if !gproto.Equal(&replicated, ToReplicatedEvent(event)) {
		t.Fatalf("unexpected replicated event blob: got=%+v want=%+v", &replicated, ToReplicatedEvent(event))
	}

	body, ok := replicated.GetTypedBody().(*proto.UserCreatedEvent)
	if !ok {
		t.Fatalf("expected user_created body, got %T", replicated.GetTypedBody())
	}
	if body.GetNodeId() != user.NodeID || body.GetUserId() != user.ID || body.GetUsername() != user.Username {
		t.Fatalf("unexpected user_created body: %+v", body)
	}
}

func TestApplyReplicatedEventRoundTripsStoredBlob(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	_, sourceEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-user",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Replicated"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	replicated := ToReplicatedEvent(sourceEvent)
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated event: %v", err)
	}

	events, err := target.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list target events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 replicated event, got %d", len(events))
	}
	if !gproto.Equal(ToReplicatedEvent(events[0]), replicated) {
		t.Fatalf("unexpected round-tripped event: got=%+v want=%+v", ToReplicatedEvent(events[0]), replicated)
	}

	var value []byte
	if err := target.db.QueryRowContext(ctx, `
SELECT value
FROM event_log
WHERE sequence = ?
`, events[0].Sequence).Scan(&value); err != nil {
		t.Fatalf("read replicated event log row: %v", err)
	}

	var stored proto.ReplicatedEvent
	if err := gproto.Unmarshal(value, &stored); err != nil {
		t.Fatalf("unmarshal stored blob: %v", err)
	}
	if !gproto.Equal(&stored, replicated) {
		t.Fatalf("stored blob mismatch: got=%+v want=%+v", &stored, replicated)
	}
}

func TestPruneEventLogOnceSQLiteKeepsLatestEventsPerOrigin(t *testing.T) {
	t.Parallel()

	st := openNamedTestStoreWithRetention(t, "node-a", 1, DefaultMessageWindowSize, 2)
	defer st.Close()

	ctx := context.Background()
	user, userEvent, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "sqlite-prune",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, firstMessageEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("first"),
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	_, secondMessageEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("second"),
	})
	if err != nil {
		t.Fatalf("create second message: %v", err)
	}
	_, thirdMessageEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("third"),
	})
	if err != nil {
		t.Fatalf("create third message: %v", err)
	}

	result, err := st.PruneEventLogOnce(ctx)
	if err != nil {
		t.Fatalf("prune event log: %v", err)
	}
	if result.TrimmedEvents != 2 || result.OriginsAffected != 1 {
		t.Fatalf("unexpected prune result: %+v", result)
	}

	retained, err := st.ListEventsByOrigin(ctx, st.NodeID(), 0, 10)
	if err != nil {
		t.Fatalf("list retained events: %v", err)
	}
	if len(retained) != 2 {
		t.Fatalf("expected 2 retained events, got %d", len(retained))
	}
	if retained[0].EventID != secondMessageEvent.EventID || retained[1].EventID != thirdMessageEvent.EventID {
		t.Fatalf("unexpected retained events: %+v", retained)
	}

	truncatedBefore, err := st.EventLogTruncatedBefore(ctx, st.NodeID())
	if err != nil {
		t.Fatalf("read truncated boundary: %v", err)
	}
	if truncatedBefore != firstMessageEvent.EventID {
		t.Fatalf("unexpected truncated boundary: got=%d want=%d", truncatedBefore, firstMessageEvent.EventID)
	}

	stats, err := st.eventLogTrimStats(ctx)
	if err != nil {
		t.Fatalf("event log trim stats: %v", err)
	}
	if stats.TrimmedTotal != 2 || stats.LastTrimmedAt == nil {
		t.Fatalf("unexpected event log trim stats: %+v", stats)
	}

	afterRetained, err := st.ListEventsByOrigin(ctx, st.NodeID(), userEvent.EventID, 10)
	if err != nil {
		t.Fatalf("list retained suffix: %v", err)
	}
	if len(afterRetained) != 2 || afterRetained[0].EventID != secondMessageEvent.EventID {
		t.Fatalf("unexpected retained suffix after truncation: %+v", afterRetained)
	}
}

func TestApplyReplicatedEventDefersFailedMessageProjectionForReplay(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
		t.Fatalf("apply replicated user create: %v", err)
	}

	_, messageEvent, err := source.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("package shipped"),
	})
	if err != nil {
		t.Fatalf("create source message: %v", err)
	}

	originalProjection := messageProjectionForTest(t, target)
	setMessageProjectionForTest(t, target, failingMessageProjectionRepository{
		delegate: originalProjection,
		failType: EventTypeMessageCreated,
		err:      errors.New("projection unavailable"),
	})
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(messageEvent)); err != nil {
		t.Fatalf("apply replicated message with deferred projection: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages before replay: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected no projected messages before replay: %+v", messages)
	}

	events, err := target.ListEventsByOrigin(ctx, messageEvent.OriginNodeID, createEvent.EventID, 10)
	if err != nil {
		t.Fatalf("list replicated events: %v", err)
	}
	if len(events) != 1 || events[0].EventType != EventTypeMessageCreated {
		t.Fatalf("unexpected replicated events after deferred projection: %+v", events)
	}

	stats, err := target.projectionStats(ctx)
	if err != nil {
		t.Fatalf("projection stats before replay: %v", err)
	}
	if stats.PendingTotal != 1 {
		t.Fatalf("expected one pending projection before replay: %+v", stats)
	}

	setMessageProjectionForTest(t, target, originalProjection)
	if err := target.ReplayPendingEvents(ctx, 10); err != nil {
		t.Fatalf("replay replicated pending events: %v", err)
	}

	messages, err = target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages after replay: %v", err)
	}
	if len(messages) != 1 || string(messages[0].Body) != "package shipped" {
		t.Fatalf("unexpected messages after replay: %+v", messages)
	}
}

func TestInitGeneratesAndPersistsNodeID(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "node-id.db")
	first, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	if err := first.Init(context.Background()); err != nil {
		t.Fatalf("init first store: %v", err)
	}
	nodeID := first.NodeID()
	if nodeID <= 0 {
		t.Fatalf("expected generated node id, got %d", nodeID)
	}

	var stored string
	if err := first.db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'node_id'`).Scan(&stored); err != nil {
		t.Fatalf("read stored node id: %v", err)
	}
	if stored != strconv.FormatInt(nodeID, 10) {
		t.Fatalf("unexpected stored node id: got=%q want=%d", stored, nodeID)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	second, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	defer second.Close()
	if err := second.Init(context.Background()); err != nil {
		t.Fatalf("init second store: %v", err)
	}
	if second.NodeID() != nodeID {
		t.Fatalf("expected persisted node identity, got id=%d want id=%d", second.NodeID(), nodeID)
	}
}

func TestOpenConfiguresSQLitePragmasAndPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sqlite-config.db"), Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if stats := st.db.Stats(); stats.MaxOpenConnections != sqliteMaxOpenConns {
		t.Fatalf("unexpected max open connections: got=%d want=%d", stats.MaxOpenConnections, sqliteMaxOpenConns)
	}

	conn1, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatalf("open sqlite conn1: %v", err)
	}
	defer conn1.Close()

	conn2, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatalf("open sqlite conn2: %v", err)
	}
	defer conn2.Close()

	assertSQLitePragmas(t, ctx, conn1)
	assertSQLitePragmas(t, ctx, conn2)
}

func TestStoreCloseSucceedsForSQLiteAndPebbleBackends(t *testing.T) {
	t.Parallel()

	sqliteStore := openTestStore(t)
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	pebbleStore := openPersistentPebbleTestStore(t, t.TempDir(), "close", 1, DefaultMessageWindowSize)
	if err := pebbleStore.Close(); err != nil {
		t.Fatalf("close pebble store: %v", err)
	}
}

func TestInitRejectsInvalidStoredNodeID(t *testing.T) {
	t.Parallel()

	st, err := Open(filepath.Join(t.TempDir(), "bad-node-id.db"), Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if _, err := st.db.Exec(`
CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES('node_id', 'not-an-int');
`); err != nil {
		t.Fatalf("seed invalid node id: %v", err)
	}

	if err := st.Init(context.Background()); err == nil {
		t.Fatalf("expected invalid stored node id to fail")
	}
}

func TestInitRejectsUnsupportedSchemaVersion(t *testing.T) {
	t.Parallel()

	st, err := Open(filepath.Join(t.TempDir(), "old-schema.db"), Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if _, err := st.db.Exec(`
CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES('schema_version', '5');
INSERT INTO schema_meta(key, value) VALUES('node_id', '4096');
`); err != nil {
		t.Fatalf("seed old schema version: %v", err)
	}

	if err := st.Init(context.Background()); err == nil {
		t.Fatalf("expected unsupported schema version to fail")
	}
}

func TestInitCreatesMessageBodyBlobWithoutMetadata(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	rows, err := st.db.Query(`PRAGMA table_info(messages)`)
	if err != nil {
		t.Fatalf("read messages schema: %v", err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var (
			cid      int
			name     string
			colType  string
			notNull  int
			defaultV any
			primaryK int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryK); err != nil {
			t.Fatalf("scan messages schema: %v", err)
		}
		columns[name] = colType
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate messages schema: %v", err)
	}
	if columns["body"] != "BLOB" {
		t.Fatalf("expected messages.body to be BLOB, got %q", columns["body"])
	}
	if _, ok := columns["metadata"]; ok {
		t.Fatalf("messages.metadata should not exist")
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

	user, err := st.GetUser(ctx, bootstrapKey(st))
	if err != nil {
		t.Fatalf("get bootstrap admin: %v", err)
	}
	if user.Username != "root" || user.Role != RoleSuperAdmin || !user.SystemReserved {
		t.Fatalf("unexpected bootstrap admin: %+v", user)
	}
	broadcast, err := st.GetUser(ctx, UserKey{NodeID: st.NodeID(), UserID: BroadcastUserID})
	if err != nil {
		t.Fatalf("get broadcast user: %v", err)
	}
	if broadcast.Username != "broadcast" || broadcast.Role != RoleBroadcast || !broadcast.SystemReserved {
		t.Fatalf("unexpected broadcast user: %+v", broadcast)
	}
	if _, err := st.AuthenticateUser(ctx, broadcast.Key(), "anything"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected broadcast login to fail with not found, got %v", err)
	}
	nodeIngress, err := st.GetUser(ctx, UserKey{NodeID: st.NodeID(), UserID: NodeIngressUserID})
	if err != nil {
		t.Fatalf("get node ingress user: %v", err)
	}
	if nodeIngress.Username != "node" || nodeIngress.Role != RoleNode || !nodeIngress.SystemReserved {
		t.Fatalf("unexpected node ingress user: %+v", nodeIngress)
	}
	if _, err := st.AuthenticateUser(ctx, nodeIngress.Key(), "anything"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected node ingress login to fail with not found, got %v", err)
	}

	newPasswordHash := "hash-root-2"
	updated, _, err := st.UpdateUser(ctx, UpdateUserParams{
		Key:          bootstrapKey(st),
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
		Key:      bootstrapKey(st),
		Username: &newUsername,
	}); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected rename bootstrap admin to fail with forbidden, got %v", err)
	}

	newRole := RoleAdmin
	if _, _, err := st.UpdateUser(ctx, UpdateUserParams{
		Key:  bootstrapKey(st),
		Role: &newRole,
	}); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected downgrade bootstrap admin to fail with forbidden, got %v", err)
	}

	if _, err := st.DeleteUser(ctx, bootstrapKey(st)); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected delete bootstrap admin to fail with forbidden, got %v", err)
	}
	if _, _, err := st.UpdateUser(ctx, UpdateUserParams{
		Key:      nodeIngress.Key(),
		Username: &newUsername,
	}); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected rename node ingress user to fail with forbidden, got %v", err)
	}
	if _, err := st.DeleteUser(ctx, nodeIngress.Key()); err == nil || !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected delete node ingress user to fail with forbidden, got %v", err)
	}

	normal, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
	})
	if err != nil {
		t.Fatalf("create normal user: %v", err)
	}
	if normal.ID != ReservedUserIDMax+1 {
		t.Fatalf("expected normal user id to start at %d, got %+v", ReservedUserIDMax+1, normal)
	}
}

func TestBootstrapAdminProtectionFollowsSmallestStoredNodeID(t *testing.T) {
	t.Parallel()

	nodeA := openNamedTestStore(t, "node-a", 1)
	defer nodeA.Close()
	nodeB := openNamedTestStore(t, "node-b", 2)
	defer nodeB.Close()

	ctx := context.Background()
	for _, st := range []*Store{nodeA, nodeB} {
		if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{
			Username:     "root",
			PasswordHash: "hash-root",
		}); err != nil {
			t.Fatalf("ensure bootstrap admin: %v", err)
		}
	}

	eventsA, err := nodeA.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list node A events: %v", err)
	}
	eventsB, err := nodeB.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list node B events: %v", err)
	}
	for _, event := range eventsA {
		if err := nodeB.ApplyReplicatedEvent(ctx, ToReplicatedEvent(event)); err != nil {
			t.Fatalf("replicate node A root to node B: %v", err)
		}
	}
	for _, event := range eventsB {
		if err := nodeA.ApplyReplicatedEvent(ctx, ToReplicatedEvent(event)); err != nil {
			t.Fatalf("replicate node B root to node A: %v", err)
		}
	}

	for _, st := range []*Store{nodeA, nodeB} {
		protected, err := st.GetUser(ctx, UserKey{NodeID: nodeA.NodeID(), UserID: BootstrapAdminUserID})
		if err != nil {
			t.Fatalf("get protected root on node %d: %v", st.NodeID(), err)
		}
		if protected.Role != RoleSuperAdmin || !protected.SystemReserved {
			t.Fatalf("expected smallest node root to be protected on node %d: %+v", st.NodeID(), protected)
		}

		demoted, err := st.GetUser(ctx, UserKey{NodeID: nodeB.NodeID(), UserID: BootstrapAdminUserID})
		if err != nil {
			t.Fatalf("get demoted root on node %d: %v", st.NodeID(), err)
		}
		if demoted.Role == RoleSuperAdmin || demoted.SystemReserved {
			t.Fatalf("expected larger node root to be demoted on node %d: %+v", st.NodeID(), demoted)
		}
	}

	chunkA, err := nodeA.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build node A users snapshot: %v", err)
	}
	chunkB, err := nodeB.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build node B users snapshot: %v", err)
	}
	hashA, err := hashSnapshotRows(chunkA.Rows)
	if err != nil {
		t.Fatalf("hash node A users snapshot: %v", err)
	}
	hashB, err := hashSnapshotRows(chunkB.Rows)
	if err != nil {
		t.Fatalf("hash node B users snapshot: %v", err)
	}
	if !bytes.Equal(hashA, hashB) {
		t.Fatalf("expected converged users snapshot hashes, got nodeA=%x nodeB=%x", hashA, hashB)
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
WHERE node_id = ? AND user_id = ?
`, "broken-root", RoleAdmin, now.String(), now.String(), now.String(), "preserve-hash", st.NodeID(), BootstrapAdminUserID); err != nil {
		t.Fatalf("corrupt bootstrap admin: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO tombstones(entity_type, entity_node_id, entity_id, deleted_at_hlc, expires_at_hlc, origin_node_id)
VALUES('user', ?, ?, ?, NULL, ?)
`, st.NodeID(), BootstrapAdminUserID, now.String(), testNodeID(1)); err != nil {
		t.Fatalf("insert bootstrap tombstone: %v", err)
	}

	if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{
		Username:     "root-fixed",
		PasswordHash: "ignored-hash",
	}); err != nil {
		t.Fatalf("repair bootstrap admin: %v", err)
	}

	user, err := st.GetUser(ctx, bootstrapKey(st))
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
WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?
`, st.NodeID(), BootstrapAdminUserID).Scan(&tombstones); err != nil {
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
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("first message"),
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if firstEvent.EventType != EventTypeMessageCreated {
		t.Fatalf("unexpected event type: %s", firstEvent.EventType)
	}

	second, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("second message"),
	})
	if err != nil {
		t.Fatalf("create second message: %v", err)
	}

	messages, err := st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if first.NodeID != testNodeID(1) || first.Seq != 1 || second.NodeID != testNodeID(1) || second.Seq != 2 {
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

func TestSQLiteCreateMessageUsesStoredCounterAfterFirstWrite(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "sqlite-seq",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	first, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("first"),
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if first.Seq != 1 {
		t.Fatalf("unexpected first sequence: %+v", first)
	}

	if _, err := st.db.ExecContext(ctx, `
DELETE FROM messages
WHERE user_node_id = ? AND user_id = ?
`, user.NodeID, user.ID); err != nil {
		t.Fatalf("delete projected messages: %v", err)
	}

	second, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("second"),
	})
	if err != nil {
		t.Fatalf("create second message: %v", err)
	}
	if second.Seq != 2 {
		t.Fatalf("expected stored counter to continue at seq 2, got %+v", second)
	}

	var nextSeq int64
	if err := st.db.QueryRowContext(ctx, `
SELECT next_seq
FROM message_sequence_counters
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, user.NodeID, user.ID, st.NodeID()).Scan(&nextSeq); err != nil {
		t.Fatalf("read next message sequence: %v", err)
	}
	if nextSeq != 3 {
		t.Fatalf("unexpected next message sequence: got=%d want=3", nextSeq)
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
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte("message-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	messages, err := st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after trim, got %d", len(messages))
	}
	if string(messages[0].Body) != "message-3" || string(messages[1].Body) != "message-2" {
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

func TestCreateMessageDefersProjectionWhenApplyFails(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	originalProjection := messageProjectionForTest(t, st)
	setMessageProjectionForTest(t, st, failingMessageProjectionRepository{
		delegate: originalProjection,
		failType: EventTypeMessageCreated,
		err:      errors.New("projection unavailable"),
	})

	message, event, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("package shipped"),
	})
	if !errors.Is(err, ErrProjectionDeferred) {
		t.Fatalf("expected projection deferred error, got %v", err)
	}
	if message.Seq != 1 || event.EventType != EventTypeMessageCreated {
		t.Fatalf("unexpected deferred write result: message=%+v event=%+v", message, event)
	}

	messages, err := st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages after deferred projection: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected no projected messages, got %+v", messages)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events after deferred projection: %v", err)
	}
	if len(events) != 2 || events[1].EventType != EventTypeMessageCreated {
		t.Fatalf("unexpected event log after deferred projection: %+v", events)
	}

	stats, err := st.projectionStats(ctx)
	if err != nil {
		t.Fatalf("projection stats: %v", err)
	}
	if stats.PendingTotal != 1 || stats.LastFailedAt == nil {
		t.Fatalf("unexpected projection stats: %+v", stats)
	}
}

func TestReplayPendingEventsProjectsDeferredMessages(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	originalProjection := messageProjectionForTest(t, st)
	setMessageProjectionForTest(t, st, failingMessageProjectionRepository{
		delegate: originalProjection,
		failType: EventTypeMessageCreated,
		err:      errors.New("projection unavailable"),
	})
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("package shipped"),
	}); !errors.Is(err, ErrProjectionDeferred) {
		t.Fatalf("expected deferred projection error, got %v", err)
	}

	setMessageProjectionForTest(t, st, originalProjection)
	if err := st.ReplayPendingEvents(ctx, 10); err != nil {
		t.Fatalf("replay pending events: %v", err)
	}
	if err := st.ReplayPendingEvents(ctx, 10); err != nil {
		t.Fatalf("replay pending events idempotent run: %v", err)
	}

	messages, err := st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages after replay: %v", err)
	}
	if len(messages) != 1 || string(messages[0].Body) != "package shipped" {
		t.Fatalf("unexpected replayed messages: %+v", messages)
	}

	stats, err := st.projectionStats(ctx)
	if err != nil {
		t.Fatalf("projection stats after replay: %v", err)
	}
	if stats.PendingTotal != 0 {
		t.Fatalf("expected no pending projections after replay: %+v", stats)
	}
}

func TestChannelSubscriptionAndBroadcastMessageVisibility(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: "hash-root",
	}); err != nil {
		t.Fatalf("ensure reserved users: %v", err)
	}
	alice, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-bob",
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := st.AuthenticateUser(ctx, channel.Key(), "anything"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected channel login to fail with not found, got %v", err)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("before subscription"),
	}); err != nil {
		t.Fatalf("create pre-subscription channel message: %v", err)
	}
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: alice.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe channel: %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("after subscription"),
	}); err != nil {
		t.Fatalf("create channel message: %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: UserKey{NodeID: st.NodeID(), UserID: BroadcastUserID},
		Sender:  testSenderKey(9, 1),
		Body:    []byte("broadcast message"),
	}); err != nil {
		t.Fatalf("create broadcast message: %v", err)
	}

	aliceMessages, err := st.ListMessagesByUser(ctx, alice.Key(), 10)
	if err != nil {
		t.Fatalf("list alice messages: %v", err)
	}
	if !messagesContainBody(aliceMessages, "after subscription") || !messagesContainBody(aliceMessages, "broadcast message") {
		t.Fatalf("expected alice to see channel and broadcast messages: %+v", aliceMessages)
	}
	if messagesContainBody(aliceMessages, "before subscription") {
		t.Fatalf("alice should not see channel history before subscription: %+v", aliceMessages)
	}

	bobMessages, err := st.ListMessagesByUser(ctx, bob.Key(), 10)
	if err != nil {
		t.Fatalf("list bob messages: %v", err)
	}
	if !messagesContainBody(bobMessages, "broadcast message") || messagesContainBody(bobMessages, "after subscription") {
		t.Fatalf("unexpected bob messages: %+v", bobMessages)
	}

	charlie, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "charlie",
		PasswordHash: "hash-charlie",
	})
	if err != nil {
		t.Fatalf("create charlie: %v", err)
	}
	charlieMessages, err := st.ListMessagesByUser(ctx, charlie.Key(), 10)
	if err != nil {
		t.Fatalf("list charlie messages: %v", err)
	}
	if !messagesContainBody(charlieMessages, "broadcast message") {
		t.Fatalf("future user should see retained broadcast messages: %+v", charlieMessages)
	}

	if _, _, err := st.UnsubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: alice.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("unsubscribe channel: %v", err)
	}
	aliceMessages, err = st.ListMessagesByUser(ctx, alice.Key(), 10)
	if err != nil {
		t.Fatalf("list alice messages after unsubscribe: %v", err)
	}
	if messagesContainBody(aliceMessages, "after subscription") {
		t.Fatalf("alice should not see channel messages after unsubscribe: %+v", aliceMessages)
	}
}

func TestBlacklistLifecycleAndRoleValidation(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	alice, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-bob",
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	entry, event, err := st.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	})
	if err != nil {
		t.Fatalf("block bob: %v", err)
	}
	if event.EventType != EventTypeUserBlocked || entry.Owner != alice.Key() || entry.Blocked != bob.Key() {
		t.Fatalf("unexpected blacklist block result: entry=%+v event=%+v", entry, event)
	}

	duplicate, duplicateEvent, err := st.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	})
	if err != nil {
		t.Fatalf("duplicate block should be idempotent: %v", err)
	}
	if duplicateEvent.EventID != 0 || duplicate.BlockedAt != entry.BlockedAt {
		t.Fatalf("unexpected duplicate block result: entry=%+v event=%+v", duplicate, duplicateEvent)
	}

	entries, err := st.ListBlockedUsers(ctx, alice.Key())
	if err != nil {
		t.Fatalf("list blocked users: %v", err)
	}
	if len(entries) != 1 || entries[0].Blocked != bob.Key() {
		t.Fatalf("unexpected blocked users: %+v", entries)
	}

	unblocked, unblockEvent, err := st.UnblockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	})
	if err != nil {
		t.Fatalf("unblock bob: %v", err)
	}
	if unblockEvent.EventType != EventTypeUserUnblocked || unblocked.DeletedAt == nil {
		t.Fatalf("unexpected unblock result: entry=%+v event=%+v", unblocked, unblockEvent)
	}
	if _, _, err := st.UnblockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected repeated unblock to return not found, got %v", err)
	}
	if _, _, err := st.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: alice.Key(),
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected self block to fail, got %v", err)
	}
	if _, _, err := st.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: channel.Key(),
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected channel block to fail, got %v", err)
	}
}

func TestBlacklistRejectsDirectMessagesButKeepsHistoryAndChannelVisibility(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	alice, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-bob",
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: alice.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe alice channel: %v", err)
	}
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: bob.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe bob channel: %v", err)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: alice.Key(),
		Sender:  bob.Key(),
		Body:    []byte("before blacklist"),
	}); err != nil {
		t.Fatalf("create message before blacklist: %v", err)
	}
	if _, _, err := st.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	}); err != nil {
		t.Fatalf("block bob: %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: alice.Key(),
		Sender:  bob.Key(),
		Body:    []byte("after blacklist"),
	}); !errors.Is(err, ErrBlockedByBlacklist) {
		t.Fatalf("expected direct message after blacklist to fail, got %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  bob.Key(),
		Body:    []byte("channel survives blacklist"),
	}); err != nil {
		t.Fatalf("expected channel message to ignore blacklist, got %v", err)
	}

	aliceMessages, err := st.ListMessagesByUser(ctx, alice.Key(), 20)
	if err != nil {
		t.Fatalf("list alice messages: %v", err)
	}
	if !messagesContainBody(aliceMessages, "before blacklist") || !messagesContainBody(aliceMessages, "channel survives blacklist") {
		t.Fatalf("expected history and channel message to remain visible, got %+v", aliceMessages)
	}
	if messagesContainBody(aliceMessages, "after blacklist") {
		t.Fatalf("unexpected blocked direct message in alice inbox: %+v", aliceMessages)
	}

	if _, _, err := st.UnblockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	}); err != nil {
		t.Fatalf("unblock bob: %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: alice.Key(),
		Sender:  bob.Key(),
		Body:    []byte("after unblock"),
	}); err != nil {
		t.Fatalf("create message after unblock: %v", err)
	}
	aliceMessages, err = st.ListMessagesByUser(ctx, alice.Key(), 20)
	if err != nil {
		t.Fatalf("list alice messages after unblock: %v", err)
	}
	if !messagesContainBody(aliceMessages, "after unblock") {
		t.Fatalf("expected direct message after unblock to be visible, got %+v", aliceMessages)
	}
}

func messagesContainBody(messages []Message, body string) bool {
	for _, message := range messages {
		if string(message.Body) == body {
			return true
		}
	}
	return false
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
		Key:      second.Key(),
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
SELECT acked_event_id
FROM peer_ack_cursors
`)
	if err != nil {
		t.Fatalf("peer ack cursor schema query: %v", err)
	}
	rows.Close()

	rows, err = st.db.QueryContext(ctx, `
SELECT applied_event_id
FROM origin_cursors
`)
	if err != nil {
		t.Fatalf("origin cursor schema query: %v", err)
	}
	rows.Close()

	if err := st.RecordPeerAck(ctx, testNodeID(1), testNodeID(2), 5); err != nil {
		t.Fatalf("record peer ack: %v", err)
	}
	if err := st.RecordPeerAck(ctx, testNodeID(1), testNodeID(2), 3); err != nil {
		t.Fatalf("record stale peer ack: %v", err)
	}
	if err := st.RecordOriginApplied(ctx, testNodeID(2), 7); err != nil {
		t.Fatalf("record origin applied: %v", err)
	}
	if err := st.RecordOriginApplied(ctx, testNodeID(2), 4); err != nil {
		t.Fatalf("record stale origin applied: %v", err)
	}

	ackCursor, err := st.GetPeerAckCursor(ctx, testNodeID(1), testNodeID(2))
	if err != nil {
		t.Fatalf("get peer ack cursor: %v", err)
	}
	if ackCursor.AckedEventID != 5 {
		t.Fatalf("unexpected acked event id: got=%d want=5", ackCursor.AckedEventID)
	}
	originCursor, err := st.GetOriginCursor(ctx, testNodeID(2))
	if err != nil {
		t.Fatalf("get origin cursor: %v", err)
	}
	if originCursor.AppliedEventID != 7 {
		t.Fatalf("unexpected applied event id: got=%d want=7", originCursor.AppliedEventID)
	}
}

func TestOperationsStatsIncludesPeerCursorsConflictsAndTrimStats(t *testing.T) {
	t.Parallel()

	st := openNamedTestStoreWithWindow(t, "node-b", 2, 1)
	defer st.Close()

	ctx := context.Background()
	user, userEvent, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "ops-stats-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	var messageEvents []Event
	for i := 1; i <= 2; i++ {
		_, event, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte("message-" + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
		messageEvents = append(messageEvents, event)
	}

	if err := st.RecordPeerAck(ctx, testNodeID(1), testNodeID(2), userEvent.EventID); err != nil {
		t.Fatalf("record peer ack: %v", err)
	}
	if err := st.RecordOriginApplied(ctx, testNodeID(2), messageEvents[0].EventID); err != nil {
		t.Fatalf("record origin applied: %v", err)
	}
	now := st.clock.Now().String()
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO user_conflicts(loser_node_id, loser_user_id, winner_node_id, winner_user_id, username, detected_at_hlc)
VALUES(?, ?, ?, ?, ?, ?)
`, testNodeID(1), int64(10), testNodeID(2), int64(11), "conflicted", now); err != nil {
		t.Fatalf("insert user conflict: %v", err)
	}

	stats, err := st.OperationsStats(ctx, []int64{testNodeID(1), testNodeID(3)})
	if err != nil {
		t.Fatalf("operations stats: %v", err)
	}
	if stats.NodeID != testNodeID(2) || stats.MessageWindowSize != 1 || stats.LastEventSequence != 3 {
		t.Fatalf("unexpected top-level stats: %+v", stats)
	}
	if stats.UserConflictsTotal != 1 {
		t.Fatalf("unexpected conflict count: %+v", stats)
	}
	if stats.MessageTrim.TrimmedTotal != 1 || stats.MessageTrim.LastTrimmedAt == nil {
		t.Fatalf("unexpected trim stats: %+v", stats.MessageTrim)
	}
	if len(stats.Peers) != 2 {
		t.Fatalf("expected configured peer stats, got %+v", stats.Peers)
	}
	if stats.Peers[0].PeerNodeID != testNodeID(1) || len(stats.Peers[0].Origins) != 1 {
		t.Fatalf("unexpected node-a peer stats: %+v", stats.Peers[0])
	}
	originStats := stats.Peers[0].Origins[0]
	if originStats.OriginNodeID != testNodeID(2) ||
		originStats.AckedEventID != userEvent.EventID ||
		originStats.AppliedEventID != messageEvents[len(messageEvents)-1].EventID ||
		originStats.UnconfirmedEvents != 2 ||
		originStats.UpdatedAt == nil {
		t.Fatalf("unexpected node-a origin stats: %+v", originStats)
	}
	if stats.Peers[1].PeerNodeID != testNodeID(3) || len(stats.Peers[1].Origins) != 1 {
		t.Fatalf("unexpected node-c peer stats: %+v", stats.Peers[1])
	}
	if stats.Peers[1].Origins[0].OriginNodeID != testNodeID(2) ||
		stats.Peers[1].Origins[0].UnconfirmedEvents != 3 ||
		stats.Peers[1].Origins[0].UpdatedAt == nil {
		t.Fatalf("unexpected node-c origin stats: %+v", stats.Peers[1].Origins[0])
	}
}

func TestApplyReplicatedEventDuplicateDeliveryIsIdempotent(t *testing.T) {
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

	loaded, err := target.GetUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("get replicated user: %v", err)
	}
	if loaded.Username != "replicated-user" {
		t.Fatalf("expected duplicate replicated event to converge to one user state, got %+v", loaded)
	}

	events, err := target.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list target events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 replicated event, got %d", len(events))
	}
}

func TestApplyReplicatedEventsKeepsDistinctOriginsSeparate(t *testing.T) {
	t.Parallel()

	sourceA := openNamedTestStore(t, "node-a", 1)
	defer sourceA.Close()

	sourceB := openNamedTestStore(t, "node-b", 2)
	defer sourceB.Close()

	target := openNamedTestStore(t, "node-c", 3)
	defer target.Close()

	ctx := context.Background()
	userA, eventA, err := sourceA.CreateUser(ctx, CreateUserParams{
		Username:     "from-a",
		PasswordHash: "hash-a",
	})
	if err != nil {
		t.Fatalf("create source A user: %v", err)
	}
	userB, eventB, err := sourceB.CreateUser(ctx, CreateUserParams{
		Username:     "from-b",
		PasswordHash: "hash-b",
	})
	if err != nil {
		t.Fatalf("create source B user: %v", err)
	}

	eventA.EventID = 7
	eventB.EventID = 7

	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(eventA)); err != nil {
		t.Fatalf("apply source A replicated event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(eventB)); err != nil {
		t.Fatalf("apply source B replicated event: %v", err)
	}

	if _, err := target.GetUser(ctx, userA.Key()); err != nil {
		t.Fatalf("get source A user: %v", err)
	}
	if _, err := target.GetUser(ctx, userB.Key()); err != nil {
		t.Fatalf("get source B user: %v", err)
	}

	events, err := target.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list replicated events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected same event_id from different origins to remain distinct, got %d events", len(events))
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
	if userA.Key() == userB.Key() {
		t.Fatalf("expected distinct replicated user keys, both were %+v", userA.Key())
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

	assertUsersDistinctByKey := func(t *testing.T, st *Store) {
		t.Helper()

		users, err := st.ListUsers(ctx)
		if err != nil {
			t.Fatalf("list replicated users: %v", err)
		}
		if len(users) != 2 {
			t.Fatalf("expected 2 users with duplicate username, got %d: %+v", len(users), users)
		}

		byKey := make(map[UserKey]User, len(users))
		for _, user := range users {
			byKey[user.Key()] = user
		}
		if byKey[userA.Key()].Username != "shared-name" || byKey[userA.Key()].PasswordHash != "hash-a" {
			t.Fatalf("missing or changed source A user: %+v", byKey[userA.Key()])
		}
		if byKey[userB.Key()].Username != "shared-name" || byKey[userB.Key()].PasswordHash != "hash-b" {
			t.Fatalf("missing or changed source B user: %+v", byKey[userB.Key()])
		}
	}

	assertUsersDistinctByKey(t, observerAB)
	assertUsersDistinctByKey(t, observerBA)
}

func TestApplyReplicatedMessageDuplicateDeliveryIsIdempotent(t *testing.T) {
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
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("hello cluster"),
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

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list replicated messages: %v", err)
	}
	if len(messages) != 1 || messages[0].NodeID != message.NodeID || messages[0].Seq != message.Seq {
		t.Fatalf("expected duplicate replicated message to be absorbed once, got %+v", messages)
	}
}

func TestReplicatedChannelSubscriptionRespectsSubscriptionVisibilityBoundary(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "subscriber",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	channel, channelEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username: "alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	for name, event := range map[string]Event{
		"user":    userEvent,
		"channel": channelEvent,
	} {
		if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(event)); err != nil {
			t.Fatalf("apply %s event: %v", name, err)
		}
	}

	_, subscriptionEvent, err := source.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: user.Key(),
		Channel:    channel.Key(),
	})
	if err != nil {
		t.Fatalf("subscribe channel: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(subscriptionEvent)); err != nil {
		t.Fatalf("apply subscription event: %v", err)
	}

	_, messageEvent, err := source.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("replicated channel message"),
	})
	if err != nil {
		t.Fatalf("create channel message: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(messageEvent)); err != nil {
		t.Fatalf("apply channel message event: %v", err)
	}
	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages: %v", err)
	}
	if !messagesContainBody(messages, "replicated channel message") {
		t.Fatalf("expected subscribed user to see channel message after replicated subscription, got %+v", messages)
	}
}

func TestReplicatedBlacklistHidesOnlyMessagesAfterBlockedAt(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	alice, aliceEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, bobEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-bob",
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	for name, event := range map[string]Event{
		"alice": aliceEvent,
		"bob":   bobEvent,
	} {
		if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(event)); err != nil {
			t.Fatalf("apply %s create event: %v", name, err)
		}
	}

	_, beforeEvent, err := source.CreateMessage(ctx, CreateMessageParams{
		UserKey: alice.Key(),
		Sender:  bob.Key(),
		Body:    []byte("before block"),
	})
	if err != nil {
		t.Fatalf("create before-block message: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(beforeEvent)); err != nil {
		t.Fatalf("apply before-block message: %v", err)
	}

	entry, blockEvent, err := source.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	})
	if err != nil {
		t.Fatalf("block bob: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(blockEvent)); err != nil {
		t.Fatalf("apply blacklist event: %v", err)
	}

	afterCreatedAt := nextDeterministicTimestamp(entry.BlockedAt)
	afterEvent := Event{
		EventID:         9001,
		EventType:       EventTypeMessageCreated,
		Aggregate:       "message",
		AggregateNodeID: testNodeID(9),
		AggregateID:     1,
		HLC:             afterCreatedAt,
		OriginNodeID:    testNodeID(9),
		Body: messageCreatedProtoFromMessage(Message{
			Recipient: alice.Key(),
			NodeID:    testNodeID(9),
			Seq:       1,
			Sender:    bob.Key(),
			Body:      []byte("after block"),
			CreatedAt: afterCreatedAt,
		}),
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(afterEvent)); err != nil {
		t.Fatalf("apply after-block message: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, alice.Key(), 20)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if !messagesContainBody(messages, "before block") || messagesContainBody(messages, "after block") {
		t.Fatalf("unexpected replicated blacklist visibility: %+v", messages)
	}
}

func TestReplicatedMessagesConvergeToConfiguredWindowBoundary(t *testing.T) {
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
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte("message-" + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatalf("create source message %d: %v", i, err)
		}
		if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(messageEvent)); err != nil {
			t.Fatalf("apply message event %d: %v", i, err)
		}
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list replicated messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after replicated trim, got %d", len(messages))
	}
	if string(messages[0].Body) != "message-3" || string(messages[1].Body) != "message-2" {
		t.Fatalf("unexpected trimmed replicated messages: %+v", messages)
	}
}

func TestReplicatedMessageWindowTrimUsesNodeAndSeqTieBreaker(t *testing.T) {
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

	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(Event{
		EventID:         firstEventID,
		EventType:       EventTypeMessageCreated,
		Aggregate:       "message",
		AggregateNodeID: testNodeID(1),
		AggregateID:     1,
		HLC:             sharedHLC,
		OriginNodeID:    testNodeID(1),
		Body: &proto.MessageCreatedEvent{
			Recipient:    &proto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
			NodeId:       testNodeID(1),
			Seq:          1,
			Sender:       &proto.ClusterUserRef{NodeId: testSenderKey(9, 1).NodeID, UserId: testSenderKey(9, 1).UserID},
			Body:         []byte("older-seq"),
			CreatedAtHlc: sharedHLC.String(),
		},
	})); err != nil {
		t.Fatalf("apply first replicated event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(Event{
		EventID:         secondEventID,
		EventType:       EventTypeMessageCreated,
		Aggregate:       "message",
		AggregateNodeID: testNodeID(1),
		AggregateID:     2,
		HLC:             sharedHLC,
		OriginNodeID:    testNodeID(1),
		Body: &proto.MessageCreatedEvent{
			Recipient:    &proto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
			NodeId:       testNodeID(1),
			Seq:          2,
			Sender:       &proto.ClusterUserRef{NodeId: testSenderKey(9, 1).NodeID, UserId: testSenderKey(9, 1).UserID},
			Body:         []byte("newer-seq"),
			CreatedAtHlc: sharedHLC.String(),
		},
	})); err != nil {
		t.Fatalf("apply second replicated event: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message after trim, got %d", len(messages))
	}
	if messages[0].NodeID != testNodeID(1) || messages[0].Seq != 2 || string(messages[0].Body) != "newer-seq" {
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
		Key:      user.Key(),
		Username: &nextUsername,
	})
	if err != nil {
		t.Fatalf("update source username: %v", err)
	}

	nextProfile := `{"display_name":"Merged On Target"}`
	targetUpdated, updateProfileEvent, err := target.UpdateUser(ctx, UpdateUserParams{
		Key:     user.Key(),
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

	sourceMerged, err := source.GetUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("get merged source user: %v", err)
	}
	targetMerged, err := target.GetUser(ctx, user.Key())
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
		Key:      user.Key(),
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
		Key:      user.Key(),
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

	sourceMerged, err := source.GetUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("get merged source user: %v", err)
	}
	targetMerged, err := target.GetUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("get merged target user: %v", err)
	}

	if sourceMerged.Username != targetUpdated.Username || targetMerged.Username != targetUpdated.Username {
		t.Fatalf("expected latest username %q, source=%+v target=%+v", targetUpdated.Username, sourceMerged, targetMerged)
	}
}

func TestReplicatedDeleteTombstonePreventsResurrection(t *testing.T) {
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
		Key:     user.Key(),
		Profile: &newProfile,
	})
	if err != nil {
		t.Fatalf("create stale update event: %v", err)
	}

	deleteEvent, err := source.DeleteUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("delete source user: %v", err)
	}

	if err := observer.ApplyReplicatedEvent(ctx, ToReplicatedEvent(deleteEvent)); err != nil {
		t.Fatalf("apply delete event: %v", err)
	}
	if err := observer.ApplyReplicatedEvent(ctx, ToReplicatedEvent(staleUpdateEvent)); err != nil {
		t.Fatalf("apply stale update event after delete: %v", err)
	}

	if _, err := observer.GetUser(ctx, user.Key()); err != ErrNotFound {
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
		Key:      user.Key(),
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

	loaded, err := target.GetUser(ctx, user.Key())
	if err != nil {
		t.Fatalf("get upserted user: %v", err)
	}
	if loaded.Username != updatedUser.Username || loaded.Profile != updatedUser.Profile {
		t.Fatalf("expected updated user to remain after older create, got %+v", loaded)
	}
}

func TestSnapshotUsersChunkConvergesDeletedUserViaTombstone(t *testing.T) {
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
	if _, err := source.DeleteUser(ctx, user.Key()); err != nil {
		t.Fatalf("delete source user: %v", err)
	}

	chunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}

	if _, err := target.GetUser(ctx, user.Key()); err != ErrNotFound {
		t.Fatalf("expected snapshot tombstone to keep deleted user hidden, got %v", err)
	}
}

func TestReplicatedReservedUsersKeepSystemReserved(t *testing.T) {
	t.Parallel()

	now := clock.NewClock(testNodeID(1)).Now()
	for _, userID := range []int64{BroadcastUserID, NodeIngressUserID} {
		user, err := userFromCreatedEvent(&proto.UserCreatedEvent{
			NodeId:              testNodeID(1),
			UserId:              userID,
			Username:            "reserved",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleUser,
			SystemReserved:      true,
			CreatedAtHlc:        now.String(),
			UpdatedAtHlc:        now.String(),
			VersionUsername:     now.String(),
			VersionPasswordHash: now.String(),
			VersionProfile:      now.String(),
			VersionRole:         now.String(),
			OriginNodeId:        testNodeID(1),
		}, testNodeID(1))
		if err != nil {
			t.Fatalf("userFromCreatedEvent(%d): %v", userID, err)
		}
		if !user.SystemReserved {
			t.Fatalf("expected replicated reserved user_id=%d to keep system_reserved", userID)
		}
	}
}

func TestSnapshotUsersChunkKeepsRemoteReservedUsers(t *testing.T) {
	t.Parallel()

	now := clock.NewClock(testNodeID(1)).Now()
	for _, userID := range []int64{BroadcastUserID, NodeIngressUserID} {
		user, err := userFromSnapshotRow(&proto.SnapshotUserRow{
			NodeId:              testNodeID(1),
			UserId:              userID,
			Username:            "reserved",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleUser,
			SystemReserved:      true,
			CreatedAtHlc:        now.String(),
			UpdatedAtHlc:        now.String(),
			VersionUsername:     now.String(),
			VersionPasswordHash: now.String(),
			VersionProfile:      now.String(),
			VersionRole:         now.String(),
			OriginNodeId:        testNodeID(1),
		})
		if err != nil {
			t.Fatalf("userFromSnapshotRow(%d): %v", userID, err)
		}
		if !user.SystemReserved {
			t.Fatalf("expected snapshot reserved user_id=%d to keep system_reserved", userID)
		}
	}
}

func TestSnapshotMessagesChunkIsIdempotentAndConvergesToLocalWindow(t *testing.T) {
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
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte("message-" + strconv.Itoa(i)),
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

	messageChunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition(testNodeID(1)))
	if err != nil {
		t.Fatalf("build messages snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, messageChunk); err != nil {
		t.Fatalf("apply messages snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, messageChunk); err != nil {
		t.Fatalf("apply messages snapshot chunk second time: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after snapshot trim, got %d", len(messages))
	}
	if string(messages[0].Body) != "message-3" || string(messages[1].Body) != "message-2" {
		t.Fatalf("expected snapshot repair to converge to local window, got %+v", messages)
	}
}

func TestApplySnapshotChunkAdvancesLocalClockPastChunkMaxTimestamp(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	if _, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-clock-user",
		PasswordHash: "hash-1",
	}); err != nil {
		t.Fatalf("create source user: %v", err)
	}
	chunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	maxTimestamp, err := MaxSnapshotChunkTimestamp(chunk)
	if err != nil {
		t.Fatalf("max snapshot chunk timestamp: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}

	next := target.clock.Now()
	if next.Compare(maxTimestamp) <= 0 {
		t.Fatalf("expected target clock to advance past snapshot max timestamp, next=%s max=%s", next, maxTimestamp)
	}
}

func TestSnapshotSubscriptionsChunkRepairsSubscriptionVisibilityBoundary(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-subscriber",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	channel, _, err := source.CreateUser(ctx, CreateUserParams{
		Username: "snapshot-alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	if _, _, err := source.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: user.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe source channel: %v", err)
	}
	if _, _, err := source.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("snapshot channel message"),
	}); err != nil {
		t.Fatalf("create source channel message: %v", err)
	}

	userChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, userChunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}
	subscriptionChunk, err := source.BuildSnapshotChunk(ctx, SnapshotSubscriptionsPartition)
	if err != nil {
		t.Fatalf("build subscriptions snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, subscriptionChunk); err != nil {
		t.Fatalf("apply subscriptions snapshot chunk: %v", err)
	}
	messageChunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition(testNodeID(1)))
	if err != nil {
		t.Fatalf("build messages snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, messageChunk); err != nil {
		t.Fatalf("apply messages snapshot chunk: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages: %v", err)
	}
	if !messagesContainBody(messages, "snapshot channel message") {
		t.Fatalf("expected snapshot-repaired subscription visibility to include channel message, got %+v", messages)
	}
}

func TestSnapshotBlacklistsChunkRepairsDirectVisibilityBoundary(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	alice, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-alice",
		PasswordHash: "hash-alice",
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-bob",
		PasswordHash: "hash-bob",
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	if _, _, err := source.CreateMessage(ctx, CreateMessageParams{
		UserKey: alice.Key(),
		Sender:  bob.Key(),
		Body:    []byte("snapshot before block"),
	}); err != nil {
		t.Fatalf("create before-block message: %v", err)
	}
	entry, _, err := source.BlockUser(ctx, BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	})
	if err != nil {
		t.Fatalf("block bob: %v", err)
	}
	afterCreatedAt := nextDeterministicTimestamp(entry.BlockedAt)
	afterEvent := Event{
		EventID:         9101,
		EventType:       EventTypeMessageCreated,
		Aggregate:       "message",
		AggregateNodeID: testNodeID(9),
		AggregateID:     1,
		HLC:             afterCreatedAt,
		OriginNodeID:    testNodeID(9),
		Body: messageCreatedProtoFromMessage(Message{
			Recipient: alice.Key(),
			NodeID:    testNodeID(9),
			Seq:       1,
			Sender:    bob.Key(),
			Body:      []byte("snapshot after block"),
			CreatedAt: afterCreatedAt,
		}),
	}
	if err := source.ApplyReplicatedEvent(ctx, ToReplicatedEvent(afterEvent)); err != nil {
		t.Fatalf("apply after-block replicated message to source: %v", err)
	}

	userChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, userChunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}
	blacklistChunk, err := source.BuildSnapshotChunk(ctx, SnapshotBlacklistsPartition)
	if err != nil {
		t.Fatalf("build blacklists snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, blacklistChunk); err != nil {
		t.Fatalf("apply blacklists snapshot chunk: %v", err)
	}
	firstMessageChunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition(testNodeID(1)))
	if err != nil {
		t.Fatalf("build local message snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, firstMessageChunk); err != nil {
		t.Fatalf("apply local message snapshot chunk: %v", err)
	}
	secondMessageChunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition(testNodeID(9)))
	if err != nil {
		t.Fatalf("build remote message snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, secondMessageChunk); err != nil {
		t.Fatalf("apply remote message snapshot chunk: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, alice.Key(), 20)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if !messagesContainBody(messages, "snapshot before block") || messagesContainBody(messages, "snapshot after block") {
		t.Fatalf("unexpected snapshot blacklist visibility: %+v", messages)
	}
}

func TestDiscoveredPeersPersistAndReload(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	connectedAt := st.Clock().Now()
	if err := st.UpsertDiscoveredPeer(ctx, DiscoveredPeer{
		NodeID:                     testNodeID(2),
		URL:                        "ws://127.0.0.1:9081/internal/cluster/ws",
		ZeroMQCurveServerPublicKey: "curve-server-public",
		SourcePeerNodeID:           testNodeID(1),
		State:                      "connected",
		LastConnectedAt:            &connectedAt,
		Generation:                 7,
	}); err != nil {
		t.Fatalf("upsert discovered peer: %v", err)
	}
	if err := st.UpsertDiscoveredPeer(ctx, DiscoveredPeer{
		NodeID:           testNodeID(2),
		URL:              "ws://127.0.0.1:9081/internal/cluster/ws",
		SourcePeerNodeID: testNodeID(3),
		State:            "failed",
		LastError:        "dial failed",
		Generation:       6,
	}); err != nil {
		t.Fatalf("update discovered peer: %v", err)
	}

	peers, err := st.ListDiscoveredPeers(ctx)
	if err != nil {
		t.Fatalf("list discovered peers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("unexpected discovered peer count: %d", len(peers))
	}
	peer := peers[0]
	if peer.NodeID != testNodeID(2) || peer.SourcePeerNodeID != testNodeID(3) || peer.State != "failed" || peer.LastError != "dial failed" || peer.Generation != 7 {
		t.Fatalf("unexpected discovered peer: %+v", peer)
	}
	if peer.ZeroMQCurveServerPublicKey != "curve-server-public" {
		t.Fatalf("expected curve server public key to be preserved, got %+v", peer)
	}
	if peer.LastConnectedAt == nil || peer.LastConnectedAt.Compare(connectedAt) != 0 {
		t.Fatalf("expected last connected timestamp to be preserved, got %+v", peer.LastConnectedAt)
	}

	if err := st.RecordDiscoveredPeerState(ctx, testNodeID(2), peer.URL, "connected", "", true); err != nil {
		t.Fatalf("record discovered peer state: %v", err)
	}
	peers, err = st.ListDiscoveredPeers(ctx)
	if err != nil {
		t.Fatalf("list updated discovered peers: %v", err)
	}
	if peers[0].State != "connected" || peers[0].LastConnectedAt == nil {
		t.Fatalf("unexpected updated discovered peer: %+v", peers[0])
	}
	if peers[0].ZeroMQCurveServerPublicKey != "curve-server-public" {
		t.Fatalf("expected curve server public key to survive state update, got %+v", peers[0])
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

func openNamedTestStoreWithRetention(t *testing.T, nodeID string, nodeSlot uint16, messageWindowSize int, maxEventsPerOrigin int) *Store {
	t.Helper()
	_ = nodeID

	dbPath := filepath.Join(t.TempDir(), "distributed.db")
	st, err := Open(dbPath, Options{
		NodeID:                     testNodeID(nodeSlot),
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return st
}

func openNamedTestStoreWithWindow(t *testing.T, nodeID string, nodeSlot uint16, messageWindowSize int) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "distributed.db")
	st, err := Open(dbPath, Options{
		NodeID:            testNodeID(nodeSlot),
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

func messageProjectionForTest(tb testing.TB, st *Store) MessageProjectionRepository {
	tb.Helper()

	switch backend := st.backend.(type) {
	case *sqliteStoreBackend:
		return backend.messageProjection
	case *pebbleStoreBackend:
		return backend.messageProjection
	default:
		tb.Fatalf("unexpected store backend %T", st.backend)
		return nil
	}
}

func setMessageProjectionForTest(tb testing.TB, st *Store, repo MessageProjectionRepository) {
	tb.Helper()

	switch backend := st.backend.(type) {
	case *sqliteStoreBackend:
		backend.messageProjection = repo
	case *pebbleStoreBackend:
		backend.messageProjection = repo
	default:
		tb.Fatalf("unexpected store backend %T", st.backend)
	}
}

type failingMessageProjectionRepository struct {
	delegate MessageProjectionRepository
	failType EventType
	err      error
}

func (r failingMessageProjectionRepository) ApplyMessageCreated(ctx context.Context, message Message) error {
	if r.failType == EventTypeMessageCreated {
		return r.err
	}
	return r.delegate.ApplyMessageCreated(ctx, message)
}

func (r failingMessageProjectionRepository) applyMessageCreatedTx(ctx context.Context, tx *sql.Tx, message Message) error {
	if r.failType == EventTypeMessageCreated {
		return r.err
	}
	delegate, ok := r.delegate.(txMessageProjectionRepository)
	if !ok {
		return r.delegate.ApplyMessageCreated(ctx, message)
	}
	return delegate.applyMessageCreatedTx(ctx, tx, message)
}

func (r failingMessageProjectionRepository) ListMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	return r.delegate.ListMessagesByUser(ctx, key, limit)
}

func (r failingMessageProjectionRepository) BuildMessageSnapshotRows(ctx context.Context, producer int64) ([]*proto.SnapshotRow, error) {
	return r.delegate.BuildMessageSnapshotRows(ctx, producer)
}

func (r failingMessageProjectionRepository) ApplyMessageSnapshotRows(ctx context.Context, producer int64, rows []*proto.SnapshotRow) error {
	return r.delegate.ApplyMessageSnapshotRows(ctx, producer, rows)
}

type sqlitePragmaQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func assertSQLitePragmas(tb testing.TB, ctx context.Context, querier sqlitePragmaQueryer) {
	tb.Helper()

	var journalMode string
	if err := querier.QueryRowContext(ctx, `PRAGMA journal_mode;`).Scan(&journalMode); err != nil {
		tb.Fatalf("read journal_mode pragma: %v", err)
	}
	if journalMode != "wal" {
		tb.Fatalf("unexpected journal_mode: got=%q want=%q", journalMode, "wal")
	}

	var synchronous int
	if err := querier.QueryRowContext(ctx, `PRAGMA synchronous;`).Scan(&synchronous); err != nil {
		tb.Fatalf("read synchronous pragma: %v", err)
	}
	if synchronous != 1 {
		tb.Fatalf("unexpected synchronous pragma: got=%d want=1", synchronous)
	}

	var busyTimeout int
	if err := querier.QueryRowContext(ctx, `PRAGMA busy_timeout;`).Scan(&busyTimeout); err != nil {
		tb.Fatalf("read busy_timeout pragma: %v", err)
	}
	if busyTimeout != 5000 {
		tb.Fatalf("unexpected busy_timeout pragma: got=%d want=5000", busyTimeout)
	}

	var foreignKeys int
	if err := querier.QueryRowContext(ctx, `PRAGMA foreign_keys;`).Scan(&foreignKeys); err != nil {
		tb.Fatalf("read foreign_keys pragma: %v", err)
	}
	if foreignKeys != 1 {
		tb.Fatalf("unexpected foreign_keys pragma: got=%d want=1", foreignKeys)
	}

	var tempStore int
	if err := querier.QueryRowContext(ctx, `PRAGMA temp_store;`).Scan(&tempStore); err != nil {
		tb.Fatalf("read temp_store pragma: %v", err)
	}
	if tempStore != 2 {
		tb.Fatalf("unexpected temp_store pragma: got=%d want=2", tempStore)
	}
}
