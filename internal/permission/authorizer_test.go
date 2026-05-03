package permission

import (
	"context"
	"errors"
	"testing"

	"github.com/tursom/turntf/internal/store"
)

type relationKey struct {
	channel store.UserKey
	subject store.UserKey
}

type fakeFactResolver struct {
	users               map[store.UserKey]store.User
	channelManagers     map[relationKey]bool
	channelWriters      map[relationKey]bool
	getUserCalls        int
	channelManagerCalls int
	channelWriterCalls  int
	getUserErr          error
	channelManagerErr   error
	channelWriterErr    error
}

func (r *fakeFactResolver) GetUser(_ context.Context, key store.UserKey) (store.User, error) {
	r.getUserCalls++
	if r.getUserErr != nil {
		return store.User{}, r.getUserErr
	}
	user, ok := r.users[key]
	if !ok {
		return store.User{}, store.ErrNotFound
	}
	return user, nil
}

func (r *fakeFactResolver) IsChannelManager(_ context.Context, channel, subject store.UserKey) (bool, error) {
	r.channelManagerCalls++
	if r.channelManagerErr != nil {
		return false, r.channelManagerErr
	}
	return r.channelManagers[relationKey{channel: channel, subject: subject}], nil
}

func (r *fakeFactResolver) IsChannelWriter(_ context.Context, channel, subject store.UserKey) (bool, error) {
	r.channelWriterCalls++
	if r.channelWriterErr != nil {
		return false, r.channelWriterErr
	}
	return r.channelWriters[relationKey{channel: channel, subject: subject}], nil
}

func TestAuthorizerDisabledBypassesResolver(t *testing.T) {
	t.Parallel()

	resolver := &fakeFactResolver{}
	authorizer := NewAuthorizer(resolver, false)
	channel := testUser(store.RoleChannel, 10, false)

	if err := authorizer.CreateMessage(context.Background(), nil, channel.Key()); err != nil {
		t.Fatalf("disabled authorizer should bypass checks: %v", err)
	}
	if resolver.getUserCalls != 0 || resolver.channelWriterCalls != 0 || resolver.channelManagerCalls != 0 {
		t.Fatalf("disabled authorizer should not query resolver: %+v", resolver)
	}
}

func TestAuthorizerCreateMessageResolvesChannelWriter(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)
	resolver := &fakeFactResolver{
		users: map[store.UserKey]store.User{
			channel.Key(): *channel,
		},
		channelWriters: map[relationKey]bool{
			{channel: channel.Key(), subject: actor.Key()}: true,
		},
	}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.CreateMessage(context.Background(), actor, channel.Key()); err != nil {
		t.Fatalf("expected channel writer authorization to succeed: %v", err)
	}
	if resolver.getUserCalls != 1 || resolver.channelWriterCalls != 1 {
		t.Fatalf("expected one target lookup and one writer lookup, got users=%d writers=%d", resolver.getUserCalls, resolver.channelWriterCalls)
	}
}

func TestAuthorizerCreateMessageAdminAndSelfSkipResolver(t *testing.T) {
	t.Parallel()

	admin := testActor(store.RoleAdmin, 1)
	user := testActor(store.RoleUser, 2)
	target := store.UserKey{NodeID: 1, UserID: 99}
	resolver := &fakeFactResolver{}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.CreateMessage(context.Background(), admin, target); err != nil {
		t.Fatalf("admin should bypass resolver-backed create message checks: %v", err)
	}
	if err := authorizer.CreateMessage(context.Background(), user, user.Key()); err != nil {
		t.Fatalf("self send should bypass resolver-backed create message checks: %v", err)
	}
	if resolver.getUserCalls != 0 || resolver.channelWriterCalls != 0 {
		t.Fatalf("admin/self paths should not query resolver: %+v", resolver)
	}
}

func TestAuthorizerUpdateAndDeleteResolveChannelManager(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)
	resolver := &fakeFactResolver{
		channelManagers: map[relationKey]bool{
			{channel: channel.Key(), subject: actor.Key()}: true,
		},
	}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.UpdateUser(context.Background(), actor, *channel, nil, false, false); err != nil {
		t.Fatalf("channel manager should update channel: %v", err)
	}
	if err := authorizer.DeleteUser(context.Background(), actor, *channel); err != nil {
		t.Fatalf("channel manager should delete channel: %v", err)
	}
	if resolver.channelManagerCalls != 2 {
		t.Fatalf("expected two channel manager checks, got %d", resolver.channelManagerCalls)
	}
}

func TestAuthorizerManageAttachmentResolvesChannelManager(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)
	resolver := &fakeFactResolver{
		channelManagers: map[relationKey]bool{
			{channel: channel.Key(), subject: actor.Key()}: true,
		},
	}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.ManageAttachment(context.Background(), actor, channel.Key(), store.AttachmentTypeChannelWriter); err != nil {
		t.Fatalf("channel manager should manage channel writer attachment: %v", err)
	}
	if resolver.channelManagerCalls != 1 || resolver.getUserCalls != 0 {
		t.Fatalf("expected one channel manager lookup and no user lookups, got managers=%d users=%d", resolver.channelManagerCalls, resolver.getUserCalls)
	}
}

func TestAuthorizerReadAndWriteUserMetadataResolveChannelManager(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)
	resolver := &fakeFactResolver{
		channelManagers: map[relationKey]bool{
			{channel: channel.Key(), subject: actor.Key()}: true,
		},
	}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.ReadUserMetadata(context.Background(), actor, *channel); err != nil {
		t.Fatalf("channel manager should read channel metadata: %v", err)
	}
	if err := authorizer.WriteUserMetadata(context.Background(), actor, *channel); err != nil {
		t.Fatalf("channel manager should write channel metadata: %v", err)
	}
	if resolver.channelManagerCalls != 2 {
		t.Fatalf("expected two channel manager checks, got %d", resolver.channelManagerCalls)
	}
}

func TestAuthorizerListAttachmentResolvesChannelManagerForTypedChannelAttachments(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)
	resolver := &fakeFactResolver{
		channelManagers: map[relationKey]bool{
			{channel: channel.Key(), subject: actor.Key()}: true,
		},
	}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.ListAttachment(context.Background(), actor, channel.Key(), store.AttachmentTypeChannelWriter); err != nil {
		t.Fatalf("channel manager should list typed channel attachments: %v", err)
	}
	if resolver.channelManagerCalls != 1 || resolver.getUserCalls != 0 {
		t.Fatalf("expected typed list to only query channel manager, got managers=%d users=%d", resolver.channelManagerCalls, resolver.getUserCalls)
	}
}

func TestAuthorizerListAttachmentResolvesOwnerRoleAndChannelManager(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)
	resolver := &fakeFactResolver{
		users: map[store.UserKey]store.User{
			channel.Key(): *channel,
		},
		channelManagers: map[relationKey]bool{
			{channel: channel.Key(), subject: actor.Key()}: true,
		},
	}
	authorizer := NewAuthorizer(resolver, true)

	if err := authorizer.ListAttachment(context.Background(), actor, channel.Key(), ""); err != nil {
		t.Fatalf("channel manager should list untyped channel attachments: %v", err)
	}
	if resolver.getUserCalls != 1 || resolver.channelManagerCalls != 1 {
		t.Fatalf("expected one owner lookup and one channel manager lookup, got users=%d managers=%d", resolver.getUserCalls, resolver.channelManagerCalls)
	}
}

func TestAuthorizerPropagatesResolverErrors(t *testing.T) {
	t.Parallel()

	actor := testActor(store.RoleUser, 1)
	channel := testUser(store.RoleChannel, 2, false)

	t.Run("get user", func(t *testing.T) {
		t.Parallel()

		resolver := &fakeFactResolver{getUserErr: store.ErrNotFound}
		authorizer := NewAuthorizer(resolver, true)
		if err := authorizer.CreateMessage(context.Background(), actor, channel.Key()); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("expected get user error to propagate, got %v", err)
		}
	})

	t.Run("channel manager", func(t *testing.T) {
		t.Parallel()

		resolver := &fakeFactResolver{
			channelManagerErr: store.ErrForbidden,
		}
		authorizer := NewAuthorizer(resolver, true)
		if err := authorizer.ManageAttachment(context.Background(), actor, channel.Key(), store.AttachmentTypeChannelWriter); !errors.Is(err, store.ErrForbidden) {
			t.Fatalf("expected channel manager error to propagate, got %v", err)
		}
	})

	t.Run("channel writer", func(t *testing.T) {
		t.Parallel()

		resolver := &fakeFactResolver{
			users: map[store.UserKey]store.User{
				channel.Key(): *channel,
			},
			channelWriterErr: store.ErrForbidden,
		}
		authorizer := NewAuthorizer(resolver, true)
		if err := authorizer.CreateMessage(context.Background(), actor, channel.Key()); !errors.Is(err, store.ErrForbidden) {
			t.Fatalf("expected channel writer error to propagate, got %v", err)
		}
	})
}
