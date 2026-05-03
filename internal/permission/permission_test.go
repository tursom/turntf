package permission

import (
	"errors"
	"testing"

	"github.com/tursom/turntf/internal/store"
)

func TestCanCreateUser(t *testing.T) {
	t.Parallel()

	superAdmin := testActor(store.RoleSuperAdmin, 1)
	admin := testActor(store.RoleAdmin, 2)
	user := testActor(store.RoleUser, 3)

	if err := CanCreateUser(CreateUserContext{Actor: superAdmin, RequestedRole: store.RoleAdmin}); err != nil {
		t.Fatalf("super_admin should create admin: %v", err)
	}
	if err := CanCreateUser(CreateUserContext{Actor: admin, RequestedRole: store.RoleUser}); err != nil {
		t.Fatalf("admin should create user: %v", err)
	}
	if err := CanCreateUser(CreateUserContext{Actor: admin, RequestedRole: store.RoleAdmin}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("admin should not create admin, got %v", err)
	}
	if err := CanCreateUser(CreateUserContext{Actor: user, RequestedRole: store.RoleUser}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("user should not create user, got %v", err)
	}
}

func TestCanUpdateUser(t *testing.T) {
	t.Parallel()

	superAdmin := testActor(store.RoleSuperAdmin, 1)
	admin := testActor(store.RoleAdmin, 2)
	channelManager := testActor(store.RoleUser, 3)

	adminTarget := testUser(store.RoleAdmin, 4, false)
	regularTarget := testUser(store.RoleUser, 5, false)
	channelTarget := testUser(store.RoleChannel, 6, false)
	reservedTarget := testUser(store.RoleBroadcast, store.BroadcastUserID, true)

	if err := CanUpdateUser(UpdateUserContext{
		Actor:         superAdmin,
		Target:        *adminTarget,
		RequestedRole: stringPtr(store.RoleUser),
	}); err != nil {
		t.Fatalf("super_admin should update admin: %v", err)
	}
	if err := CanUpdateUser(UpdateUserContext{
		Actor:  admin,
		Target: *regularTarget,
	}); err != nil {
		t.Fatalf("admin should update user: %v", err)
	}
	if err := CanUpdateUser(UpdateUserContext{
		Actor:  admin,
		Target: *adminTarget,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("admin should not update admin: %v", err)
	}
	if err := CanUpdateUser(UpdateUserContext{
		Actor:         admin,
		Target:        *regularTarget,
		RequestedRole: stringPtr(store.RoleAdmin),
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("admin should not promote admin: %v", err)
	}
	if err := CanUpdateUser(UpdateUserContext{
		Actor:             channelManager,
		Target:            *channelTarget,
		ChannelManager:    true,
		UpdatingLoginName: false,
	}); err != nil {
		t.Fatalf("channel manager should update channel profile/username: %v", err)
	}
	if err := CanUpdateUser(UpdateUserContext{
		Actor:            channelManager,
		Target:           *channelTarget,
		ChannelManager:   true,
		UpdatingPassword: true,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("channel manager should not update channel password: %v", err)
	}
	if err := CanUpdateUser(UpdateUserContext{
		Actor:  superAdmin,
		Target: *reservedTarget,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("reserved user should still be protected: %v", err)
	}
}

func TestCanDeleteUser(t *testing.T) {
	t.Parallel()

	superAdmin := testActor(store.RoleSuperAdmin, 1)
	admin := testActor(store.RoleAdmin, 2)
	channelManager := testActor(store.RoleUser, 3)

	adminTarget := testUser(store.RoleAdmin, 4, false)
	regularTarget := testUser(store.RoleUser, 5, false)
	channelTarget := testUser(store.RoleChannel, 6, false)
	reservedTarget := testUser(store.RoleNode, store.NodeIngressUserID, true)

	if err := CanDeleteUser(DeleteUserContext{Actor: admin, Target: *regularTarget}); err != nil {
		t.Fatalf("admin should delete user: %v", err)
	}
	if err := CanDeleteUser(DeleteUserContext{Actor: admin, Target: *adminTarget}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("admin should not delete admin: %v", err)
	}
	if err := CanDeleteUser(DeleteUserContext{
		Actor:          channelManager,
		Target:         *channelTarget,
		ChannelManager: true,
	}); err != nil {
		t.Fatalf("channel manager should delete channel: %v", err)
	}
	if err := CanDeleteUser(DeleteUserContext{Actor: superAdmin, Target: *reservedTarget}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("reserved user should still be protected: %v", err)
	}
}

func TestSelfScopedPermissions(t *testing.T) {
	t.Parallel()

	admin := testActor(store.RoleAdmin, 1)
	alice := testActor(store.RoleUser, 2)
	bob := testUser(store.RoleUser, 3, false)
	channel := testUser(store.RoleChannel, 4, false)
	reserved := testUser(store.RoleBroadcast, store.BroadcastUserID, true)

	if err := CanReadUserMetadata(UserMetadataContext{Actor: alice, Owner: *testUser(store.RoleUser, alice.ID, false)}); err != nil {
		t.Fatalf("self metadata read should be allowed: %v", err)
	}
	if err := CanReadUserMetadata(UserMetadataContext{Actor: alice, Owner: *bob}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("cross-user metadata read should be forbidden: %v", err)
	}
	if err := CanReadUserMetadata(UserMetadataContext{Actor: alice, Owner: *channel, ChannelManager: true}); err != nil {
		t.Fatalf("channel manager metadata read should be allowed: %v", err)
	}
	if err := CanReadUserMetadata(UserMetadataContext{Actor: admin, Owner: *reserved}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("system reserved metadata should stay forbidden: %v", err)
	}
	if err := CanManageSubscription(SelfScopedContext{Actor: admin, TargetKey: bob.Key()}); err != nil {
		t.Fatalf("admin should manage subscriptions: %v", err)
	}
	if err := CanListBlacklist(SelfScopedContext{Actor: admin, TargetKey: bob.Key()}); err != nil {
		t.Fatalf("admin should list blacklist: %v", err)
	}
}

func TestAttachmentPermissions(t *testing.T) {
	t.Parallel()

	admin := testActor(store.RoleAdmin, 1)
	alice := testActor(store.RoleUser, 2)
	channelOwner := store.UserKey{NodeID: 1, UserID: 100}
	userOwner := alice.Key()

	if err := CanManageAttachment(ManageAttachmentContext{
		Actor:          admin,
		Owner:          channelOwner,
		AttachmentType: store.AttachmentTypeChannelWriter,
	}); err != nil {
		t.Fatalf("admin should manage channel attachment: %v", err)
	}
	if err := CanManageAttachment(ManageAttachmentContext{
		Actor:          alice,
		Owner:          userOwner,
		AttachmentType: store.AttachmentTypeChannelSubscription,
	}); err != nil {
		t.Fatalf("self subscription should be allowed: %v", err)
	}
	if err := CanManageAttachment(ManageAttachmentContext{
		Actor:          alice,
		Owner:          channelOwner,
		AttachmentType: store.AttachmentTypeChannelWriter,
		ChannelManager: false,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("non-manager channel writer update should be forbidden: %v", err)
	}
	if err := CanListAttachment(ListAttachmentContext{
		Actor:          alice,
		Owner:          channelOwner,
		OwnerRole:      store.RoleChannel,
		ChannelManager: true,
	}); err != nil {
		t.Fatalf("channel manager should list channel attachments: %v", err)
	}
}

func TestCanCreateMessage(t *testing.T) {
	t.Parallel()

	admin := testActor(store.RoleAdmin, 1)
	alice := testActor(store.RoleUser, 2)
	bob := testUser(store.RoleUser, 3, false)
	channel := testUser(store.RoleChannel, 4, false)
	broadcast := testUser(store.RoleBroadcast, 5, false)
	node := testUser(store.RoleNode, 6, false)

	if err := CanCreateMessage(CreateMessageContext{
		Actor:     admin,
		TargetKey: bob.Key(),
	}); err != nil {
		t.Fatalf("admin should send to any target: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:     alice,
		TargetKey: alice.Key(),
	}); err != nil {
		t.Fatalf("self send should be allowed: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:     alice,
		TargetKey: bob.Key(),
		Target:    bob,
	}); err != nil {
		t.Fatalf("user should send to login target: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:         alice,
		TargetKey:     channel.Key(),
		Target:        channel,
		ChannelWriter: true,
	}); err != nil {
		t.Fatalf("channel writer should send to channel: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:     alice,
		TargetKey: channel.Key(),
		Target:    channel,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("non-writer channel send should be forbidden: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:     alice,
		TargetKey: broadcast.Key(),
		Target:    broadcast,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("broadcast send should be forbidden: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:     alice,
		TargetKey: node.Key(),
		Target:    node,
	}); !errors.Is(err, store.ErrForbidden) {
		t.Fatalf("node send should be forbidden: %v", err)
	}
	if err := CanCreateMessage(CreateMessageContext{
		Actor:     alice,
		TargetKey: bob.Key(),
	}); !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("missing target facts should be invalid input: %v", err)
	}
}

func testActor(role string, userID int64) *store.User {
	return testUser(role, userID, false)
}

func testUser(role string, userID int64, reserved bool) *store.User {
	return &store.User{
		NodeID:         1,
		ID:             userID,
		Username:       "user",
		Role:           role,
		SystemReserved: reserved,
	}
}

func stringPtr(value string) *string {
	return &value
}
