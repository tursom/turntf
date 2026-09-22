package store

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestListMessagesBySessionRejectsInvalidRequesterAcrossEngines(t *testing.T) {
	t.Parallel()

	forEachStoreEngine(t, "invalid-session-requester", func(t *testing.T, st *Store) {
		session := MessageSession(
			UserKey{NodeID: testNodeID(1), UserID: 1},
			UserKey{NodeID: testNodeID(1), UserID: 2},
		)
		if _, err := st.ListMessagesBySession(context.Background(), session, UserKey{}, 10); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid requester should be rejected, got %v", err)
		}
	})
}

func TestListMessagesBySessionReturnsConversationAcrossEngines(t *testing.T) {
	t.Parallel()

	forEachStoreEngine(t, "session-conversation", func(t *testing.T, st *Store) {
		ctx := context.Background()
		alice := createContractUser(t, st, "alice", RoleUser)
		bob := createContractUser(t, st, "bob", RoleUser)
		carol := createContractUser(t, st, "carol", RoleUser)

		first, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: bob.Key(),
			Sender:  alice.Key(),
			Body:    []byte("alice to bob"),
		})
		if err != nil {
			t.Fatalf("create first conversation message: %v", err)
		}
		second, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: alice.Key(),
			Sender:  bob.Key(),
			Body:    []byte("bob to alice"),
		})
		if err != nil {
			t.Fatalf("create second conversation message: %v", err)
		}
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: carol.Key(),
			Sender:  alice.Key(),
			Body:    []byte("unrelated"),
		}); err != nil {
			t.Fatalf("create unrelated message: %v", err)
		}

		messages, err := st.ListMessagesBySession(ctx, MessageSession(bob.Key(), alice.Key()), alice.Key(), 10)
		if err != nil {
			t.Fatalf("list conversation: %v", err)
		}
		if len(messages) != 2 || messages[0].CreatedAt != second.CreatedAt || messages[1].CreatedAt != first.CreatedAt {
			t.Fatalf("unexpected conversation order or membership: %+v", messages)
		}
		limited, err := st.ListMessagesBySession(ctx, MessageSession(alice.Key(), bob.Key()), bob.Key(), 1)
		if err != nil {
			t.Fatalf("list limited conversation: %v", err)
		}
		if len(limited) != 1 || string(limited[0].Body) != "bob to alice" {
			t.Fatalf("unexpected limited conversation: %+v", limited)
		}
	})
}

func TestChannelRelationshipQueriesTrackLifecycleAcrossEngines(t *testing.T) {
	t.Parallel()

	forEachStoreEngine(t, "channel-relationships", func(t *testing.T, st *Store) {
		ctx := context.Background()
		channel := createContractUser(t, st, "alerts", RoleChannel)
		alice := createContractUser(t, st, "alice", RoleUser)
		bob := createContractUser(t, st, "bob", RoleUser)

		manager, err := st.IsChannelManager(ctx, channel.Key(), alice.Key())
		if err != nil {
			t.Fatalf("check initial manager: %v", err)
		}
		writer, err := st.IsChannelWriter(ctx, channel.Key(), alice.Key())
		if err != nil {
			t.Fatalf("check initial writer: %v", err)
		}
		if manager || writer {
			t.Fatalf("new user should have no channel relationship: manager=%t writer=%t", manager, writer)
		}

		upsertContractAttachment(t, st, channel.Key(), alice.Key(), AttachmentTypeChannelManager)
		upsertContractAttachment(t, st, channel.Key(), alice.Key(), AttachmentTypeChannelWriter)
		manager, err = st.IsChannelManager(ctx, channel.Key(), alice.Key())
		if err != nil || !manager {
			t.Fatalf("active manager relationship not reported: manager=%t err=%v", manager, err)
		}
		writer, err = st.IsChannelWriter(ctx, channel.Key(), alice.Key())
		if err != nil || !writer {
			t.Fatalf("active writer relationship not reported: writer=%t err=%v", writer, err)
		}

		if _, _, err := st.DeleteAttachment(ctx, DeleteAttachmentParams{
			Owner: channel.Key(), Subject: alice.Key(), Type: AttachmentTypeChannelManager,
		}); !errors.Is(err, ErrForbidden) {
			t.Fatalf("last channel manager must be protected, got %v", err)
		}
		upsertContractAttachment(t, st, channel.Key(), bob.Key(), AttachmentTypeChannelManager)
		if _, _, err := st.DeleteAttachment(ctx, DeleteAttachmentParams{
			Owner: channel.Key(), Subject: alice.Key(), Type: AttachmentTypeChannelManager,
		}); err != nil {
			t.Fatalf("delete manager while replacement exists: %v", err)
		}
		if _, _, err := st.DeleteAttachment(ctx, DeleteAttachmentParams{
			Owner: channel.Key(), Subject: alice.Key(), Type: AttachmentTypeChannelWriter,
		}); err != nil {
			t.Fatalf("delete writer: %v", err)
		}
		manager, err = st.IsChannelManager(ctx, channel.Key(), alice.Key())
		if err != nil || manager {
			t.Fatalf("deleted manager relationship still active: manager=%t err=%v", manager, err)
		}
		writer, err = st.IsChannelWriter(ctx, channel.Key(), alice.Key())
		if err != nil || writer {
			t.Fatalf("deleted writer relationship still active: writer=%t err=%v", writer, err)
		}
	})
}

func TestBlacklistQueriesTrackCurrentAndMessageTimeStateAcrossEngines(t *testing.T) {
	t.Parallel()

	forEachStoreEngine(t, "blacklist-queries", func(t *testing.T, st *Store) {
		ctx := context.Background()
		recipient := createContractUser(t, st, "recipient", RoleUser)
		sender := createContractUser(t, st, "sender", RoleUser)
		beforeBlock, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: recipient.Key(), Sender: sender.Key(), Body: []byte("before block"),
		})
		if err != nil {
			t.Fatalf("create message before block: %v", err)
		}

		blocked, err := st.IsBlockedByRecipient(ctx, recipient.Key(), sender.Key())
		if err != nil || blocked {
			t.Fatalf("unexpected initial block state: blocked=%t err=%v", blocked, err)
		}
		entry, _, err := st.BlockUser(ctx, BlacklistParams{Owner: recipient.Key(), Blocked: sender.Key()})
		if err != nil {
			t.Fatalf("block sender: %v", err)
		}
		blocked, err = st.IsBlockedByRecipient(ctx, recipient.Key(), sender.Key())
		if err != nil || !blocked {
			t.Fatalf("active block not reported: blocked=%t err=%v", blocked, err)
		}
		hidden, err := st.IsMessageHiddenByBlacklist(ctx, recipient.Key(), sender.Key(), beforeBlock.CreatedAt)
		if err != nil || hidden {
			t.Fatalf("pre-block message should remain visible: hidden=%t err=%v", hidden, err)
		}
		hidden, err = st.IsMessageHiddenByBlacklist(ctx, recipient.Key(), sender.Key(), entry.BlockedAt)
		if err != nil || !hidden {
			t.Fatalf("message at block boundary should be hidden: hidden=%t err=%v", hidden, err)
		}

		if _, _, err := st.UnblockUser(ctx, BlacklistParams{Owner: recipient.Key(), Blocked: sender.Key()}); err != nil {
			t.Fatalf("unblock sender: %v", err)
		}
		blocked, err = st.IsBlockedByRecipient(ctx, recipient.Key(), sender.Key())
		if err != nil || blocked {
			t.Fatalf("deleted block still active: blocked=%t err=%v", blocked, err)
		}
	})
}

func TestMeshTopologyGenerationRoundTripsAcrossEngines(t *testing.T) {
	t.Parallel()

	forEachStoreEngine(t, "mesh-generation", func(t *testing.T, st *Store) {
		ctx := context.Background()
		generation, err := st.LoadMeshTopologyGeneration(ctx)
		if err != nil || generation != 0 {
			t.Fatalf("unexpected initial generation: generation=%d err=%v", generation, err)
		}
		if err := st.StoreMeshTopologyGeneration(ctx, 42); err != nil {
			t.Fatalf("store generation: %v", err)
		}
		generation, err = st.LoadMeshTopologyGeneration(ctx)
		if err != nil || generation != 42 {
			t.Fatalf("unexpected stored generation: generation=%d err=%v", generation, err)
		}
	})

	var nilStore *Store
	if generation, err := nilStore.LoadMeshTopologyGeneration(context.Background()); err != nil || generation != 0 {
		t.Fatalf("nil store load should be safe: generation=%d err=%v", generation, err)
	}
	if err := nilStore.StoreMeshTopologyGeneration(context.Background(), 42); err != nil {
		t.Fatalf("nil store write should be safe: %v", err)
	}
}

func TestMeshRuntimeEpochReservationIsMonotonicAcrossEngines(t *testing.T) {
	t.Parallel()

	forEachStoreEngine(t, "mesh-runtime-epoch", func(t *testing.T, st *Store) {
		ctx := context.Background()
		first, err := st.ReserveMeshRuntimeEpoch(ctx, 200)
		if err != nil || first != 200 {
			t.Fatalf("reserve first epoch: epoch=%d err=%v", first, err)
		}
		rolledBack, err := st.ReserveMeshRuntimeEpoch(ctx, 150)
		if err != nil || rolledBack != 201 {
			t.Fatalf("reserve after clock rollback: epoch=%d err=%v", rolledBack, err)
		}
		forward, err := st.ReserveMeshRuntimeEpoch(ctx, 300)
		if err != nil || forward != 300 {
			t.Fatalf("reserve forward epoch: epoch=%d err=%v", forward, err)
		}

		const workers = 8
		results := make(chan uint64, workers)
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				epoch, reserveErr := st.ReserveMeshRuntimeEpoch(ctx, 400)
				results <- epoch
				errs <- reserveErr
			}()
		}
		wg.Wait()
		close(results)
		close(errs)
		for reserveErr := range errs {
			if reserveErr != nil {
				t.Fatalf("concurrent reservation: %v", reserveErr)
			}
		}
		seen := make(map[uint64]struct{}, workers)
		for epoch := range results {
			seen[epoch] = struct{}{}
		}
		for epoch := uint64(400); epoch < 400+workers; epoch++ {
			if _, ok := seen[epoch]; !ok {
				t.Fatalf("concurrent reservations = %v, missing %d", seen, epoch)
			}
		}

		const maxEpoch = uint64(^uint64(0) >> 1)
		if _, err := st.ReserveMeshRuntimeEpoch(ctx, maxEpoch+1); err == nil {
			t.Fatal("out-of-range candidate was accepted")
		}
		if _, err := st.db.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = ?`,
			"9223372036854775807", schemaMetaMeshRuntimeEpochKey); err != nil {
			t.Fatalf("seed exhausted epoch: %v", err)
		}
		if _, err := st.ReserveMeshRuntimeEpoch(ctx, 1); err == nil {
			t.Fatal("exhausted persisted epoch was incremented")
		}
		var raw string
		if err := st.db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = ?`, schemaMetaMeshRuntimeEpochKey).Scan(&raw); err != nil || raw != "9223372036854775807" {
			t.Fatalf("exhausted epoch was mutated: value=%q err=%v", raw, err)
		}

		if _, err := st.db.ExecContext(ctx, `UPDATE schema_meta SET value = 'invalid' WHERE key = ?`, schemaMetaMeshRuntimeEpochKey); err != nil {
			t.Fatalf("seed invalid epoch: %v", err)
		}
		if _, err := st.ReserveMeshRuntimeEpoch(ctx, 1); err == nil {
			t.Fatal("invalid persisted epoch was accepted")
		}
		if err := st.db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = ?`, schemaMetaMeshRuntimeEpochKey).Scan(&raw); err != nil || raw != "invalid" {
			t.Fatalf("invalid epoch was mutated: value=%q err=%v", raw, err)
		}
	})

	var nilStore *Store
	if epoch, err := nilStore.ReserveMeshRuntimeEpoch(context.Background(), 42); err != nil || epoch != 42 {
		t.Fatalf("nil store reservation should use candidate: epoch=%d err=%v", epoch, err)
	}
}

func TestTransientDeliveryValueValidation(t *testing.T) {
	t.Parallel()

	if (SessionRef{}).Valid() {
		t.Fatal("empty session ref must be invalid")
	}
	if (SessionRef{ServingNodeID: testNodeID(1), SessionID: "   "}).Valid() {
		t.Fatal("blank session id must be invalid")
	}
	if !(SessionRef{ServingNodeID: testNodeID(1), SessionID: "session-1"}).Valid() {
		t.Fatal("complete session ref must be valid")
	}

	for _, tt := range []struct {
		raw     string
		want    DeliveryMode
		wantErr error
	}{
		{raw: "", want: DeliveryModeBestEffort},
		{raw: " best_effort ", want: DeliveryModeBestEffort},
		{raw: "route_retry", want: DeliveryModeRouteRetry},
		{raw: "durable", wantErr: ErrInvalidInput},
	} {
		got, err := NormalizeDeliveryMode(tt.raw)
		if got != tt.want || !errors.Is(err, tt.wantErr) {
			t.Fatalf("normalize delivery mode %q: got (%q, %v) want (%q, %v)", tt.raw, got, err, tt.want, tt.wantErr)
		}
	}
}

func createContractUser(t *testing.T, st *Store, username, role string) User {
	t.Helper()
	user, _, err := st.CreateUser(context.Background(), CreateUserParams{
		Username: username,
		Role:     role,
		PasswordHash: func() string {
			if role == RoleChannel {
				return ""
			}
			return "hash-" + username
		}(),
	})
	if err != nil {
		t.Fatalf("create %s user %q: %v", role, username, err)
	}
	return user
}

func upsertContractAttachment(t *testing.T, st *Store, owner, subject UserKey, attachmentType AttachmentType) {
	t.Helper()
	if _, _, err := st.UpsertAttachment(context.Background(), UpsertAttachmentParams{
		Owner: owner, Subject: subject, Type: attachmentType, ConfigJSON: "{}",
	}); err != nil {
		t.Fatalf("upsert %s attachment: %v", attachmentType, err)
	}
}

func forEachStoreEngine(t *testing.T, name string, test func(*testing.T, *Store)) {
	t.Helper()

	t.Run(EngineSQLite, func(t *testing.T) {
		st := openNamedTestStore(t, name+"-sqlite", 1)
		t.Cleanup(func() { _ = st.Close() })
		test(t, st)
	})
	t.Run(EnginePebble, func(t *testing.T) {
		st := openPebbleTestStore(t, name+"-pebble", 1, DefaultMessageWindowSize)
		test(t, st)
	})
}
