package store

import (
	"context"
	"sync"
	"testing"
)

func TestSubscriptionChangeNotificationsFollowCommittedLocalState(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	subscriber, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "subscription-notify-subscriber",
		PasswordHash: "!",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create subscriber: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "subscription-notify-channel",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	var mu sync.Mutex
	var batches [][]SubscriptionChange
	unsubscribe := st.SubscribeSubscriptionChanges(func(changes []SubscriptionChange) {
		mu.Lock()
		batches = append(batches, append([]SubscriptionChange(nil), changes...))
		mu.Unlock()
	})
	defer unsubscribe()

	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe channel: %v", err)
	}
	if _, _, err := st.UnsubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("unsubscribe channel: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := [][]SubscriptionChange{
		{{Subscriber: subscriber.Key(), Channel: channel.Key(), Active: true}},
		{{Subscriber: subscriber.Key(), Channel: channel.Key(), Active: false}},
	}
	if len(batches) != len(want) {
		t.Fatalf("unexpected notification batch count: got=%d want=%d batches=%+v", len(batches), len(want), batches)
	}
	for idx := range want {
		if len(batches[idx]) != 1 || batches[idx][0] != want[idx][0] {
			t.Fatalf("unexpected notification batch %d: got=%+v want=%+v", idx, batches[idx], want[idx])
		}
	}
}

func TestSubscriptionChangeNotificationsFollowGenericAttachmentState(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	subscriber, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "generic-attachment-notify-subscriber",
		PasswordHash: "!",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create subscriber: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "generic-attachment-notify-channel",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	var changes []SubscriptionChange
	unsubscribe := st.SubscribeSubscriptionChanges(func(batch []SubscriptionChange) {
		changes = append(changes, batch...)
	})
	defer unsubscribe()

	if _, _, err := st.UpsertAttachment(ctx, UpsertAttachmentParams{
		Owner:   subscriber.Key(),
		Subject: channel.Key(),
		Type:    AttachmentTypeChannelSubscription,
	}); err != nil {
		t.Fatalf("upsert generic subscription attachment: %v", err)
	}
	if _, _, err := st.DeleteAttachment(ctx, DeleteAttachmentParams{
		Owner:   subscriber.Key(),
		Subject: channel.Key(),
		Type:    AttachmentTypeChannelSubscription,
	}); err != nil {
		t.Fatalf("delete generic subscription attachment: %v", err)
	}

	want := []bool{true, false}
	if len(changes) != len(want) {
		t.Fatalf("unexpected generic attachment notification count: got=%d changes=%+v", len(changes), changes)
	}
	for idx, active := range want {
		change := changes[idx]
		if change.Subscriber != subscriber.Key() || change.Channel != channel.Key() || change.Active != active || change.Reload {
			t.Fatalf("unexpected generic attachment notification %d: %+v", idx, change)
		}
	}
}

func TestSubscriptionChangeNotificationsUseFinalReplicatedState(t *testing.T) {
	source := openNamedTestStore(t, "subscription-notify-source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "subscription-notify-target", 2)
	defer target.Close()

	ctx := context.Background()
	subscriber, subscriberEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-notify-subscriber",
		PasswordHash: "!",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create source subscriber: %v", err)
	}
	channel, channelEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username: "replicated-notify-channel",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	for _, event := range []Event{subscriberEvent, channelEvent} {
		if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(event)); err != nil {
			t.Fatalf("replicate user prerequisite: %v", err)
		}
	}

	_, subscribedEvent, err := source.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	})
	if err != nil {
		t.Fatalf("subscribe source channel: %v", err)
	}
	_, unsubscribedEvent, err := source.UnsubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	})
	if err != nil {
		t.Fatalf("unsubscribe source channel: %v", err)
	}

	var mu sync.Mutex
	var changes []SubscriptionChange
	unsubscribe := target.SubscribeSubscriptionChanges(func(batch []SubscriptionChange) {
		mu.Lock()
		changes = append(changes, batch...)
		mu.Unlock()
	})
	defer unsubscribe()

	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(unsubscribedEvent)); err != nil {
		t.Fatalf("apply newer unsubscribe event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(subscribedEvent)); err != nil {
		t.Fatalf("apply stale subscribe event: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(changes) != 2 {
		t.Fatalf("unexpected replicated notification count: got=%d changes=%+v", len(changes), changes)
	}
	for idx, change := range changes {
		if change.Subscriber != subscriber.Key() || change.Channel != channel.Key() || change.Active || change.Reload {
			t.Fatalf("unexpected replicated notification %d: %+v", idx, change)
		}
	}
}

func TestSubscriptionChangeNotificationsFollowAttachmentSnapshotRepair(t *testing.T) {
	source := openNamedTestStore(t, "subscription-snapshot-source", 1)
	defer source.Close()
	target := openNamedTestStore(t, "subscription-snapshot-target", 2)
	defer target.Close()

	ctx := context.Background()
	subscriber, _, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-notify-subscriber",
		PasswordHash: "!",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create source subscriber: %v", err)
	}
	channel, _, err := source.CreateUser(ctx, CreateUserParams{
		Username: "snapshot-notify-channel",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	if _, _, err := source.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe source channel: %v", err)
	}
	usersChunk, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users snapshot: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, usersChunk); err != nil {
		t.Fatalf("apply users snapshot: %v", err)
	}

	var mu sync.Mutex
	var changes []SubscriptionChange
	unsubscribe := target.SubscribeSubscriptionChanges(func(batch []SubscriptionChange) {
		mu.Lock()
		changes = append(changes, batch...)
		mu.Unlock()
	})
	defer unsubscribe()

	applyAttachmentsSnapshot := func() {
		t.Helper()
		chunk, err := source.BuildSnapshotChunk(ctx, SnapshotAttachmentsPartition)
		if err != nil {
			t.Fatalf("build attachments snapshot: %v", err)
		}
		if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
			t.Fatalf("apply attachments snapshot: %v", err)
		}
	}
	applyAttachmentsSnapshot()
	if _, _, err := source.UnsubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("unsubscribe source channel: %v", err)
	}
	applyAttachmentsSnapshot()

	mu.Lock()
	defer mu.Unlock()
	want := []bool{true, false}
	if len(changes) != len(want) {
		t.Fatalf("unexpected snapshot notification count: got=%d changes=%+v", len(changes), changes)
	}
	for idx, active := range want {
		change := changes[idx]
		if change.Subscriber != subscriber.Key() || change.Channel != channel.Key() || change.Active != active || change.Reload {
			t.Fatalf("unexpected snapshot notification %d: %+v", idx, change)
		}
	}
}
