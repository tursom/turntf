package permission

import (
	"strings"

	"github.com/tursom/turntf/internal/store"
)

type ActorContext struct {
	Actor *store.User
}

type SelfScopedContext struct {
	Actor     *store.User
	TargetKey store.UserKey
}

type CreateUserContext struct {
	Actor         *store.User
	RequestedRole string
}

type UpdateUserContext struct {
	Actor             *store.User
	Target            store.User
	RequestedRole     *string
	UpdatingPassword  bool
	UpdatingLoginName bool
	ChannelManager    bool
}

type DeleteUserContext struct {
	Actor          *store.User
	Target         store.User
	ChannelManager bool
}

type CreateMessageContext struct {
	Actor         *store.User
	TargetKey     store.UserKey
	Target        *store.User
	ChannelWriter bool
}

type ListMessagesContext struct {
	Actor  *store.User
	Target store.User
}

type ManageAttachmentContext struct {
	Actor          *store.User
	Owner          store.UserKey
	AttachmentType store.AttachmentType
	ChannelManager bool
}

type ListAttachmentContext struct {
	Actor          *store.User
	Owner          store.UserKey
	OwnerRole      string
	AttachmentType store.AttachmentType
	ChannelManager bool
}

func CanListUsers(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

func CanCreateUser(ctx CreateUserContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if isSuperAdminRole(ctx.Actor.Role) {
		return nil
	}
	if !IsAdminRole(ctx.Actor.Role) {
		return store.ErrForbidden
	}
	if requestsPrivilegedRole(ctx.RequestedRole) {
		return store.ErrForbidden
	}
	return nil
}

func CanViewUser(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanUpdateUser(ctx UpdateUserContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if ctx.Target.SystemReserved {
		return store.ErrForbidden
	}
	if isSuperAdminRole(ctx.Actor.Role) {
		return nil
	}
	if IsAdminRole(ctx.Actor.Role) {
		if isPrivilegedRole(ctx.Target.Role) {
			return store.ErrForbidden
		}
		if requestsPrivilegedRolePtr(ctx.RequestedRole) {
			return store.ErrForbidden
		}
		return nil
	}
	if ctx.Target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	if !ctx.ChannelManager {
		return store.ErrForbidden
	}
	if ctx.UpdatingPassword || ctx.RequestedRole != nil || ctx.UpdatingLoginName {
		return store.ErrForbidden
	}
	return nil
}

func CanDeleteUser(ctx DeleteUserContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if ctx.Target.SystemReserved {
		return store.ErrForbidden
	}
	if isSuperAdminRole(ctx.Actor.Role) {
		return nil
	}
	if IsAdminRole(ctx.Actor.Role) {
		if isPrivilegedRole(ctx.Target.Role) {
			return store.ErrForbidden
		}
		return nil
	}
	if ctx.Target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	if !ctx.ChannelManager {
		return store.ErrForbidden
	}
	return nil
}

func CanListMessages(ctx ListMessagesContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) || ctx.Actor.Key() == ctx.Target.Key() {
		return nil
	}
	return store.ErrForbidden
}

func CanCreateMessage(ctx CreateMessageContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) || ctx.Actor.Key() == ctx.TargetKey {
		return nil
	}
	if ctx.Target == nil {
		return store.ErrInvalidInput
	}
	if ctx.Target.CanLogin() {
		return nil
	}
	if ctx.Target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	if !ctx.ChannelWriter {
		return store.ErrForbidden
	}
	return nil
}

func CanReadUserMetadata(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanWriteUserMetadata(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanManageAttachment(ctx ManageAttachmentContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) {
		return nil
	}
	switch ctx.AttachmentType {
	case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
		if ctx.ChannelManager {
			return nil
		}
		return store.ErrForbidden
	case store.AttachmentTypeChannelSubscription, store.AttachmentTypeUserBlacklist:
		if ctx.Actor.Key() == ctx.Owner {
			return nil
		}
		return store.ErrForbidden
	default:
		return store.ErrInvalidInput
	}
}

func CanListAttachment(ctx ListAttachmentContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) {
		return nil
	}
	if ctx.AttachmentType != "" {
		return CanManageAttachment(ManageAttachmentContext{
			Actor:          ctx.Actor,
			Owner:          ctx.Owner,
			AttachmentType: ctx.AttachmentType,
			ChannelManager: ctx.ChannelManager,
		})
	}
	if ctx.Actor.Key() == ctx.Owner {
		return nil
	}
	if strings.TrimSpace(ctx.OwnerRole) == store.RoleChannel && ctx.ChannelManager {
		return nil
	}
	return store.ErrForbidden
}

func CanManageSubscription(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanListSubscription(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanManageBlacklist(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanListBlacklist(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

func CanListEvents(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

func CanReadOpsStatus(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

func CanReadMetrics(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

func CanListClusterNodes(ctx ActorContext) error {
	return requireAuthenticated(ctx.Actor)
}

func CanListLoggedInUsers(ctx ActorContext) error {
	return requireAuthenticated(ctx.Actor)
}

func requireAuthenticated(actor *store.User) error {
	if actor == nil {
		return store.ErrForbidden
	}
	return nil
}

func requireAdmin(actor *store.User) error {
	if actor == nil || !IsAdminRole(actor.Role) {
		return store.ErrForbidden
	}
	return nil
}

func requireSelfOrAdmin(actor *store.User, target store.UserKey) error {
	if actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(actor.Role) || actor.Key() == target {
		return nil
	}
	return store.ErrForbidden
}

func IsAdminRole(role string) bool {
	return role == store.RoleSuperAdmin || role == store.RoleAdmin
}

func isSuperAdminRole(role string) bool {
	return role == store.RoleSuperAdmin
}

func isPrivilegedRole(role string) bool {
	switch strings.TrimSpace(role) {
	case store.RoleAdmin, store.RoleSuperAdmin:
		return true
	default:
		return false
	}
}

func requestsPrivilegedRole(role string) bool {
	return isPrivilegedRole(role)
}

func requestsPrivilegedRolePtr(role *string) bool {
	if role == nil {
		return false
	}
	return isPrivilegedRole(*role)
}
