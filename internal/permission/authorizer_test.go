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

func TestAuthorizerSimpleOperationsRespectPermissions(t *testing.T) {
	t.Parallel()

	admin := testActor(store.RoleAdmin, 1)
	alice := testActor(store.RoleUser, 2)
	bob := testActor(store.RoleUser, 3)
	authorizer := NewAuthorizer(nil, true)

	checks := []struct {
		name    string
		allowed func() error
		denied  func() error
	}{
		{name: "list users", allowed: func() error { return authorizer.ListUsers(admin) }, denied: func() error { return authorizer.ListUsers(alice) }},
		{name: "create user", allowed: func() error { return authorizer.CreateUser(admin, store.RoleUser) }, denied: func() error { return authorizer.CreateUser(alice, store.RoleUser) }},
		{name: "view user", allowed: func() error { return authorizer.ViewUser(alice, alice.Key()) }, denied: func() error { return authorizer.ViewUser(alice, bob.Key()) }},
		{name: "list messages", allowed: func() error { return authorizer.ListMessages(alice, *alice) }, denied: func() error { return authorizer.ListMessages(alice, *bob) }},
		{name: "manage subscription", allowed: func() error { return authorizer.ManageSubscription(alice, alice.Key()) }, denied: func() error { return authorizer.ManageSubscription(alice, bob.Key()) }},
		{name: "list subscription", allowed: func() error { return authorizer.ListSubscription(alice, alice.Key()) }, denied: func() error { return authorizer.ListSubscription(alice, bob.Key()) }},
		{name: "manage blacklist", allowed: func() error { return authorizer.ManageBlacklist(alice, alice.Key()) }, denied: func() error { return authorizer.ManageBlacklist(alice, bob.Key()) }},
		{name: "list blacklist", allowed: func() error { return authorizer.ListBlacklist(alice, alice.Key()) }, denied: func() error { return authorizer.ListBlacklist(alice, bob.Key()) }},
		{name: "list events", allowed: func() error { return authorizer.ListEvents(admin) }, denied: func() error { return authorizer.ListEvents(alice) }},
		{name: "read operations status", allowed: func() error { return authorizer.ReadOpsStatus(admin) }, denied: func() error { return authorizer.ReadOpsStatus(alice) }},
		{name: "read metrics", allowed: func() error { return authorizer.ReadMetrics(admin) }, denied: func() error { return authorizer.ReadMetrics(alice) }},
		{name: "list cluster nodes", allowed: func() error { return authorizer.ListClusterNodes(alice) }, denied: func() error { return authorizer.ListClusterNodes(nil) }},
		{name: "list logged-in users", allowed: func() error { return authorizer.ListLoggedInUsers(alice) }, denied: func() error { return authorizer.ListLoggedInUsers(nil) }},
	}
	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.allowed(); err != nil {
				t.Fatalf("allowed operation failed: %v", err)
			}
			if err := tt.denied(); !errors.Is(err, store.ErrForbidden) {
				t.Fatalf("denied operation returned %v", err)
			}
		})
	}
}

func TestDisabledAuthorizerBypassesEveryOperation(t *testing.T) {
	t.Parallel()

	var authorizer *Authorizer = NewAuthorizer(nil, false)
	user := testActor(store.RoleUser, 1)
	target := *testUser(store.RoleChannel, 2, false)
	ctx := context.Background()

	checks := []struct {
		name  string
		check func() error
	}{
		{name: "list users", check: func() error { return authorizer.ListUsers(nil) }},
		{name: "create user", check: func() error { return authorizer.CreateUser(nil, store.RoleAdmin) }},
		{name: "view user", check: func() error { return authorizer.ViewUser(nil, target.Key()) }},
		{name: "update user", check: func() error { return authorizer.UpdateUser(ctx, nil, target, nil, true, true) }},
		{name: "delete user", check: func() error { return authorizer.DeleteUser(ctx, nil, target) }},
		{name: "create message", check: func() error { return authorizer.CreateMessage(ctx, nil, target.Key()) }},
		{name: "list messages", check: func() error { return authorizer.ListMessages(nil, target) }},
		{name: "read metadata", check: func() error { return authorizer.ReadUserMetadata(ctx, nil, target) }},
		{name: "write metadata", check: func() error { return authorizer.WriteUserMetadata(ctx, nil, target) }},
		{name: "manage attachment", check: func() error {
			return authorizer.ManageAttachment(ctx, nil, target.Key(), store.AttachmentTypeChannelWriter)
		}},
		{name: "list attachment", check: func() error {
			return authorizer.ListAttachment(ctx, nil, target.Key(), store.AttachmentTypeChannelWriter)
		}},
		{name: "manage subscription", check: func() error { return authorizer.ManageSubscription(nil, target.Key()) }},
		{name: "list subscription", check: func() error { return authorizer.ListSubscription(nil, target.Key()) }},
		{name: "manage blacklist", check: func() error { return authorizer.ManageBlacklist(nil, target.Key()) }},
		{name: "list blacklist", check: func() error { return authorizer.ListBlacklist(nil, target.Key()) }},
		{name: "list events", check: func() error { return authorizer.ListEvents(nil) }},
		{name: "read operations status", check: func() error { return authorizer.ReadOpsStatus(nil) }},
		{name: "read metrics", check: func() error { return authorizer.ReadMetrics(nil) }},
		{name: "list cluster nodes", check: func() error { return authorizer.ListClusterNodes(nil) }},
		{name: "list logged-in users", check: func() error { return authorizer.ListLoggedInUsers(nil) }},
	}
	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.check(); err != nil {
				t.Fatalf("disabled authorizer should allow operation: %v", err)
			}
		})
	}

	if err := authorizer.CreateMessage(ctx, user, target.Key()); err != nil {
		t.Fatalf("disabled authorizer should not require a resolver: %v", err)
	}
}

func TestEnabledAuthorizerRequiresResolverForRelationshipFacts(t *testing.T) {
	t.Parallel()

	authorizer := NewAuthorizer(nil, true)
	actor := testActor(store.RoleUser, 1)
	channel := *testUser(store.RoleChannel, 2, false)

	checks := []struct {
		name  string
		check func() error
	}{
		{name: "update channel", check: func() error { return authorizer.UpdateUser(context.Background(), actor, channel, nil, false, false) }},
		{name: "delete channel", check: func() error { return authorizer.DeleteUser(context.Background(), actor, channel) }},
		{name: "send to channel", check: func() error { return authorizer.CreateMessage(context.Background(), actor, channel.Key()) }},
		{name: "read channel metadata", check: func() error { return authorizer.ReadUserMetadata(context.Background(), actor, channel) }},
		{name: "write channel metadata", check: func() error { return authorizer.WriteUserMetadata(context.Background(), actor, channel) }},
		{name: "manage channel attachment", check: func() error {
			return authorizer.ManageAttachment(context.Background(), actor, channel.Key(), store.AttachmentTypeChannelWriter)
		}},
		{name: "list typed channel attachment", check: func() error {
			return authorizer.ListAttachment(context.Background(), actor, channel.Key(), store.AttachmentTypeChannelWriter)
		}},
		{name: "list all channel attachments", check: func() error { return authorizer.ListAttachment(context.Background(), actor, channel.Key(), "") }},
	}
	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.check(); !errors.Is(err, store.ErrInvalidInput) {
				t.Fatalf("missing resolver should return invalid input, got %v", err)
			}
		})
	}
}
