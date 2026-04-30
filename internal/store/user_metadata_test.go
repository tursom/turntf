package store

import (
	"context"
	"errors"
	"testing"
	"time"

	proto "github.com/tursom/turntf/internal/proto"
)

func TestUserMetadataCRUDAndScan(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "meta-alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "meta-channel",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	first, event, err := st.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner: user.Key(),
		Key:   "session:web:1",
		Value: []byte{0xff, 0x00, 'a'},
	})
	if err != nil {
		t.Fatalf("upsert first metadata: %v", err)
	}
	if event.EventType != EventTypeUserMetadataUpserted {
		t.Fatalf("unexpected upsert event type: %s", event.EventType)
	}
	if first.Key != "session:web:1" || string(first.Value) != string([]byte{0xff, 0x00, 'a'}) {
		t.Fatalf("unexpected first metadata: %+v", first)
	}

	expiresAt := time.Now().UTC().Add(2 * time.Hour)
	if _, _, err := st.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner:     user.Key(),
		Key:       "session:web:2",
		Value:     []byte("second"),
		ExpiresAt: &expiresAt,
	}); err != nil {
		t.Fatalf("upsert second metadata: %v", err)
	}
	if _, _, err := st.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner: user.Key(),
		Key:   "draft:chat:1",
		Value: []byte("draft"),
	}); err != nil {
		t.Fatalf("upsert draft metadata: %v", err)
	}

	replacement, _, err := st.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner: user.Key(),
		Key:   "session:web:1",
		Value: []byte("replacement"),
	})
	if err != nil {
		t.Fatalf("replace first metadata: %v", err)
	}
	if string(replacement.Value) != "replacement" {
		t.Fatalf("unexpected replacement metadata: %+v", replacement)
	}

	loaded, err := st.GetUserMetadata(ctx, user.Key(), "session:web:1")
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if string(loaded.Value) != "replacement" {
		t.Fatalf("unexpected loaded metadata: %+v", loaded)
	}

	firstPage, err := st.ScanUserMetadata(ctx, ScanUserMetadataParams{
		Owner:  user.Key(),
		Prefix: "session:",
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("scan first page: %v", err)
	}
	if len(firstPage.Items) != 1 || firstPage.Items[0].Key != "session:web:1" || firstPage.NextAfter != "session:web:1" {
		t.Fatalf("unexpected first scan page: %+v", firstPage)
	}

	secondPage, err := st.ScanUserMetadata(ctx, ScanUserMetadataParams{
		Owner:  user.Key(),
		Prefix: "session:",
		After:  firstPage.NextAfter,
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("scan second page: %v", err)
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].Key != "session:web:2" || secondPage.NextAfter != "" {
		t.Fatalf("unexpected second scan page: %+v", secondPage)
	}

	allItems, err := st.ScanUserMetadata(ctx, ScanUserMetadataParams{
		Owner: user.Key(),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("scan all metadata: %v", err)
	}
	if len(allItems.Items) != 3 {
		t.Fatalf("expected 3 visible metadata rows, got %+v", allItems)
	}

	if _, err := st.ScanUserMetadata(ctx, ScanUserMetadataParams{
		Owner:  user.Key(),
		Prefix: "session:",
		After:  "draft:chat:1",
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid input for mismatched prefix/after, got %v", err)
	}

	deleted, event, err := st.DeleteUserMetadata(ctx, DeleteUserMetadataParams{
		Owner: user.Key(),
		Key:   "session:web:1",
	})
	if err != nil {
		t.Fatalf("delete metadata: %v", err)
	}
	if event.EventType != EventTypeUserMetadataDeleted || deleted.DeletedAt == nil {
		t.Fatalf("unexpected delete metadata result: metadata=%+v event=%+v", deleted, event)
	}
	if _, err := st.GetUserMetadata(ctx, user.Key(), "session:web:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted metadata to be hidden, got %v", err)
	}

	expiredAt := currentUserMetadataWallTime(st.clock).Add(-time.Minute)
	expired, _, err := st.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner:     user.Key(),
		Key:       "session:web:expired",
		Value:     []byte("expired"),
		ExpiresAt: &expiredAt,
	})
	if err != nil {
		t.Fatalf("upsert expired metadata: %v", err)
	}
	if expired.ExpiresAt == nil {
		t.Fatalf("expected expired metadata to preserve expires_at")
	}
	if _, err := st.GetUserMetadata(ctx, user.Key(), "session:web:expired"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired metadata to be hidden, got %v", err)
	}

	if _, _, err := st.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner: channel.Key(),
		Key:   "session:web:channel",
		Value: []byte("forbidden"),
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid input for channel metadata owner, got %v", err)
	}
}

func TestUserMetadataReplicationUsesLWW(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "meta-source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "meta-target", 2)
	defer target.Close()

	ctx := context.Background()
	user, createEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-meta-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(createEvent)); err != nil {
		t.Fatalf("replicate user create: %v", err)
	}

	originNodeID := testNodeID(9)
	base := source.Clock().Now()
	older := nextDeterministicTimestamp(base)
	newer := nextDeterministicTimestamp(older)
	newest := nextDeterministicTimestamp(newer)

	oldUpsert := Event{
		EventID:         1001,
		EventType:       EventTypeUserMetadataUpserted,
		Aggregate:       "user_metadata",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             older,
		OriginNodeID:    originNodeID,
		Body: &proto.UserMetadataUpsertedEvent{
			Owner:        &proto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
			Key:          "session:web:1",
			Value:        []byte("older"),
			UpdatedAtHlc: older.String(),
			OriginNodeId: originNodeID,
		},
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(oldUpsert)); err != nil {
		t.Fatalf("apply old upsert: %v", err)
	}
	loaded, err := target.GetUserMetadata(ctx, user.Key(), "session:web:1")
	if err != nil {
		t.Fatalf("get metadata after old upsert: %v", err)
	}
	if string(loaded.Value) != "older" {
		t.Fatalf("unexpected metadata after old upsert: %+v", loaded)
	}

	deleteEvent := Event{
		EventID:         1002,
		EventType:       EventTypeUserMetadataDeleted,
		Aggregate:       "user_metadata",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             newer,
		OriginNodeID:    originNodeID,
		Body: &proto.UserMetadataDeletedEvent{
			Owner:        &proto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
			Key:          "session:web:1",
			DeletedAtHlc: newer.String(),
			OriginNodeId: originNodeID,
		},
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(deleteEvent)); err != nil {
		t.Fatalf("apply delete event: %v", err)
	}
	if _, err := target.GetUserMetadata(ctx, user.Key(), "session:web:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted metadata to be hidden, got %v", err)
	}

	olderResurrection := Event{
		EventID:         1003,
		EventType:       EventTypeUserMetadataUpserted,
		Aggregate:       "user_metadata",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             newer,
		OriginNodeID:    originNodeID,
		Body: &proto.UserMetadataUpsertedEvent{
			Owner:        &proto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
			Key:          "session:web:1",
			Value:        []byte("still-older"),
			UpdatedAtHlc: older.String(),
			OriginNodeId: originNodeID,
		},
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(olderResurrection)); err != nil {
		t.Fatalf("apply stale resurrection: %v", err)
	}
	if _, err := target.GetUserMetadata(ctx, user.Key(), "session:web:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected stale resurrection to stay hidden, got %v", err)
	}

	newResurrection := Event{
		EventID:         1004,
		EventType:       EventTypeUserMetadataUpserted,
		Aggregate:       "user_metadata",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             newest,
		OriginNodeID:    originNodeID,
		Body: &proto.UserMetadataUpsertedEvent{
			Owner:        &proto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
			Key:          "session:web:1",
			Value:        []byte("newer"),
			UpdatedAtHlc: newest.String(),
			OriginNodeId: originNodeID,
		},
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(newResurrection)); err != nil {
		t.Fatalf("apply new resurrection: %v", err)
	}
	loaded, err = target.GetUserMetadata(ctx, user.Key(), "session:web:1")
	if err != nil {
		t.Fatalf("get resurrected metadata: %v", err)
	}
	if string(loaded.Value) != "newer" {
		t.Fatalf("unexpected resurrected metadata: %+v", loaded)
	}
}

func TestUserMetadataSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "meta-snap-source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "meta-snap-target", 2)
	defer target.Close()

	ctx := context.Background()
	user, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-meta-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if _, _, err := source.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner: user.Key(),
		Key:   "session:web:active",
		Value: []byte("active"),
	}); err != nil {
		t.Fatalf("upsert active metadata: %v", err)
	}
	if _, _, err := source.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner: user.Key(),
		Key:   "session:web:deleted",
		Value: []byte("deleted"),
	}); err != nil {
		t.Fatalf("upsert deleted metadata: %v", err)
	}
	if _, _, err := source.DeleteUserMetadata(ctx, DeleteUserMetadataParams{
		Owner: user.Key(),
		Key:   "session:web:deleted",
	}); err != nil {
		t.Fatalf("delete metadata: %v", err)
	}
	expiredAt := currentUserMetadataWallTime(source.clock).Add(-time.Minute)
	if _, _, err := source.UpsertUserMetadata(ctx, UpsertUserMetadataParams{
		Owner:     user.Key(),
		Key:       "session:web:expired",
		Value:     []byte("expired"),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("upsert expired metadata: %v", err)
	}

	userChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, userChunk); err != nil {
		t.Fatalf("apply users snapshot chunk: %v", err)
	}
	metadataChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUserMetadataPartition)
	if err != nil {
		t.Fatalf("build metadata snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, metadataChunk); err != nil {
		t.Fatalf("apply metadata snapshot chunk: %v", err)
	}

	active, err := target.GetUserMetadata(ctx, user.Key(), "session:web:active")
	if err != nil {
		t.Fatalf("get active metadata after snapshot: %v", err)
	}
	if string(active.Value) != "active" {
		t.Fatalf("unexpected active metadata after snapshot: %+v", active)
	}
	if _, err := target.GetUserMetadata(ctx, user.Key(), "session:web:deleted"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted metadata to stay hidden after snapshot, got %v", err)
	}
	if _, err := target.GetUserMetadata(ctx, user.Key(), "session:web:expired"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired metadata to stay hidden after snapshot, got %v", err)
	}
}
