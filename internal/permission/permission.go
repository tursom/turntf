// Package permission 提供基于角色的权限控制（RBAC），定义了系统中所有操作的权限检查规则。
//
// 角色层次：super_admin > admin > user/channel/node/broadcast
// 核心原则：管理员可管理低权限用户，但不能操作同级或上级；普通用户只能操作自己或自己管理的频道。
package permission

import (
	"strings"

	"github.com/tursom/turntf/internal/store"
)

// ActorContext 仅包含操作者身份，用于不需要区分目标的权限检查（如列出所有用户）。
type ActorContext struct {
	Actor *store.User
}

// SelfScopedContext 包含操作者和目标用户标识，用于"本人或管理员"模式的权限检查。
// 适用于：查看/修改用户元数据、管理订阅、管理黑名单等场景。
type SelfScopedContext struct {
	Actor     *store.User
	TargetKey store.UserKey
}

// UserMetadataContext 描述 metadata 访问权限所需的上下文。
// 频道 owner 需要额外的 ChannelManager 事实，其余用户沿用本人或管理员规则。
type UserMetadataContext struct {
	Actor          *store.User
	Owner          store.User
	ChannelManager bool
}

// CreateUserContext 创建用户时的权限上下文。
// RequestedRole 是请求创建的用户角色。
type CreateUserContext struct {
	Actor         *store.User
	RequestedRole string
}

// UpdateUserContext 更新用户信息时的权限上下文。
// RequestedRole 为 nil 表示不修改角色；UpdatingPassword/UpdatingLoginName 标记敏感字段的修改；
// ChannelManager 表示操作者是否为目标频道的管理者。
type UpdateUserContext struct {
	Actor             *store.User
	Target            store.User
	RequestedRole     *string
	UpdatingPassword  bool
	UpdatingLoginName bool
	ChannelManager    bool
}

// DeleteUserContext 删除用户时的权限上下文。
type DeleteUserContext struct {
	Actor          *store.User
	Target         store.User
	ChannelManager bool
}

// CreateMessageContext 发送消息时的权限上下文。
// TargetKey 是接收者标识；Target 为接收者实体（可能为 nil）；
// ChannelWriter 表示操作者是否为目标频道的写入者。
type CreateMessageContext struct {
	Actor         *store.User
	TargetKey     store.UserKey
	Target        *store.User
	ChannelWriter bool
}

// ListMessagesContext 查看消息列表时的权限上下文，包含操作者和消息所属用户。
type ListMessagesContext struct {
	Actor  *store.User
	Target store.User
}

// ManageAttachmentContext 管理附件（如频道管理者列表、频道写入者列表、订阅、黑名单）的权限上下文。
// Owner 是附件所属实体；AttachmentType 区分附件类型。
type ManageAttachmentContext struct {
	Actor          *store.User
	Owner          store.UserKey
	AttachmentType store.AttachmentType
	ChannelManager bool
}

// ListAttachmentContext 列出附件时的权限上下文。
// OwnerRole 仅在无指定附件类型时用于判断频道管理权限。
type ListAttachmentContext struct {
	Actor          *store.User
	Owner          store.UserKey
	OwnerRole      string
	AttachmentType store.AttachmentType
	ChannelManager bool
}

// CanListUsers 检查是否允许列出所有用户。仅管理员及以上可执行。
func CanListUsers(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

// CanCreateUser 检查是否允许创建用户。
//   - super_admin：可创建任意角色
//   - admin：只能创建非特权角色（user/channel/node 等）
//   - 普通用户：禁止
func CanCreateUser(ctx CreateUserContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	// super_admin 无限制
	if isSuperAdminRole(ctx.Actor.Role) {
		return nil
	}
	// 非管理员禁止
	if !IsAdminRole(ctx.Actor.Role) {
		return store.ErrForbidden
	}
	// admin 不允许创建特权角色（admin/super_admin）
	if requestsPrivilegedRole(ctx.RequestedRole) {
		return store.ErrForbidden
	}
	return nil
}

// CanViewUser 检查是否允许查看用户信息。本人或管理员可查看。
func CanViewUser(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

// CanUpdateUser 检查是否允许更新用户信息。权限层级从高到低：
//   - super_admin：可更新任意非系统保留用户
//   - admin：可更新非特权角色的用户，但不能将其提升为特权角色
//   - 频道管理员（ChannelManager）：仅可更新频道用户的非敏感信息（显示名等），不能改密码、角色或登录名
func CanUpdateUser(ctx UpdateUserContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	// 系统保留用户（如广播、节点）不可被更新
	if ctx.Target.SystemReserved {
		return store.ErrForbidden
	}
	// super_admin 无限制
	if isSuperAdminRole(ctx.Actor.Role) {
		return nil
	}
	// admin：可更新非特权用户，但不能将其提升为特权角色
	if IsAdminRole(ctx.Actor.Role) {
		if isPrivilegedRole(ctx.Target.Role) {
			return store.ErrForbidden
		}
		if requestsPrivilegedRolePtr(ctx.RequestedRole) {
			return store.ErrForbidden
		}
		return nil
	}
	// 非管理员仅能操作频道用户，且必须是频道管理者
	if ctx.Target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	if !ctx.ChannelManager {
		return store.ErrForbidden
	}
	// 频道管理者不能修改频道用户的密码、角色或登录名
	if ctx.UpdatingPassword || ctx.RequestedRole != nil || ctx.UpdatingLoginName {
		return store.ErrForbidden
	}
	return nil
}

// CanDeleteUser 检查是否允许删除用户。逻辑与 Update 类似但不涉及字段级限制：
//   - super_admin：可删除任意非系统保留用户
//   - admin：可删除非特权角色的用户
//   - 频道管理员：仅可删除其管理的频道用户
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
	// 非管理员仅能删除频道用户，且必须是频道管理者
	if ctx.Target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	if !ctx.ChannelManager {
		return store.ErrForbidden
	}
	return nil
}

// CanListMessages 检查是否允许查看消息列表。管理员或消息所属用户本人可查看。
func CanListMessages(ctx ListMessagesContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) || ctx.Actor.Key() == ctx.Target.Key() {
		return nil
	}
	return store.ErrForbidden
}

// CanCreateMessage 检查是否允许向目标用户发送消息。
//   - 管理员：可向任意用户发送
//   - 自己：可向自己发送
//   - 目标可登录（普通用户/管理员）：允许发送
//   - 目标为频道：需 ChannelWriter 权限
//   - 其他角色（如 broadcast/node）：禁止
func CanCreateMessage(ctx CreateMessageContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	// 管理员或自己可直接发送
	if IsAdminRole(ctx.Actor.Role) || ctx.Actor.Key() == ctx.TargetKey {
		return nil
	}
	// 需要知道目标用户信息才能进一步判断
	if ctx.Target == nil {
		return store.ErrInvalidInput
	}
	// 目标可登录（用户/管理员）：允许直接发送
	if ctx.Target.CanLogin() {
		return nil
	}
	// 频道用户：需要 ChannelWriter 权限
	if ctx.Target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	if !ctx.ChannelWriter {
		return store.ErrForbidden
	}
	return nil
}

func canAccessUserMetadata(ctx UserMetadataContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if ctx.Actor.Key() == ctx.Owner.Key() {
		return nil
	}
	if ctx.Owner.SystemReserved {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) {
		return nil
	}
	if ctx.Owner.Role == store.RoleChannel {
		if ctx.ChannelManager {
			return nil
		}
		return store.ErrForbidden
	}
	return store.ErrForbidden
}

// CanReadUserMetadata 检查是否允许读取用户元数据。
func CanReadUserMetadata(ctx UserMetadataContext) error {
	return canAccessUserMetadata(ctx)
}

// CanWriteUserMetadata 检查是否允许修改用户元数据。
func CanWriteUserMetadata(ctx UserMetadataContext) error {
	return canAccessUserMetadata(ctx)
}

// CanManageAttachment 检查是否允许管理附件（增/删/改）。规则按附件类型划分：
//   - 管理员：无限制
//   - 频道管理/写入者附件：需 ChannelManager
//   - 订阅/黑名单附件：仅允许操作自己的
func CanManageAttachment(ctx ManageAttachmentContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	// 管理员无限制
	if IsAdminRole(ctx.Actor.Role) {
		return nil
	}
	switch ctx.AttachmentType {
	case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
		// 频道相关附件：需频道管理权限
		if ctx.ChannelManager {
			return nil
		}
		return store.ErrForbidden
	case store.AttachmentTypeChannelSubscription, store.AttachmentTypeUserBlacklist:
		// 订阅和黑名单：仅允许操作自己的
		if ctx.Actor.Key() == ctx.Owner {
			return nil
		}
		return store.ErrForbidden
	default:
		return store.ErrInvalidInput
	}
}

// CanListAttachment 检查是否允许列出附件。
//   - 指定附件类型时：复用 CanManageAttachment 的权限逻辑
//   - 不指定类型时：本人可查看自己的；频道管理员可查看频道的
func CanListAttachment(ctx ListAttachmentContext) error {
	if ctx.Actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(ctx.Actor.Role) {
		return nil
	}
	// 指定了附件类型：直接按管理权限检查
	if ctx.AttachmentType != "" {
		return CanManageAttachment(ManageAttachmentContext{
			Actor:          ctx.Actor,
			Owner:          ctx.Owner,
			AttachmentType: ctx.AttachmentType,
			ChannelManager: ctx.ChannelManager,
		})
	}
	// 查看自己的附件
	if ctx.Actor.Key() == ctx.Owner {
		return nil
	}
	// 频道管理员可查看自己频道的附件
	if strings.TrimSpace(ctx.OwnerRole) == store.RoleChannel && ctx.ChannelManager {
		return nil
	}
	return store.ErrForbidden
}

// CanManageSubscription 检查是否允许管理订阅。本人或管理员。
func CanManageSubscription(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

// CanListSubscription 检查是否允许查看订阅列表。本人或管理员。
func CanListSubscription(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

// CanManageBlacklist 检查是否允许管理黑名单。本人或管理员。
func CanManageBlacklist(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

// CanListBlacklist 检查是否允许查看黑名单。本人或管理员。
func CanListBlacklist(ctx SelfScopedContext) error {
	return requireSelfOrAdmin(ctx.Actor, ctx.TargetKey)
}

// CanListEvents 检查是否允许查看系统事件。仅管理员及以上。
func CanListEvents(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

// CanReadOpsStatus 检查是否允许读取运维状态。仅管理员及以上。
func CanReadOpsStatus(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

// CanReadMetrics 检查是否允许读取系统指标。仅管理员及以上。
func CanReadMetrics(ctx ActorContext) error {
	return requireAdmin(ctx.Actor)
}

// CanListClusterNodes 检查是否允许列出集群节点。任意已认证用户。
func CanListClusterNodes(ctx ActorContext) error {
	return requireAuthenticated(ctx.Actor)
}

// CanListLoggedInUsers 检查是否允许查看在线用户列表。任意已认证用户。
func CanListLoggedInUsers(ctx ActorContext) error {
	return requireAuthenticated(ctx.Actor)
}

// requireAuthenticated 要求操作者已认证（非 nil）。
func requireAuthenticated(actor *store.User) error {
	if actor == nil {
		return store.ErrForbidden
	}
	return nil
}

// requireAdmin 要求操作者为管理员及以上角色。
func requireAdmin(actor *store.User) error {
	if actor == nil || !IsAdminRole(actor.Role) {
		return store.ErrForbidden
	}
	return nil
}

// requireSelfOrAdmin 要求操作者为目标本人或管理员。
func requireSelfOrAdmin(actor *store.User, target store.UserKey) error {
	if actor == nil {
		return store.ErrForbidden
	}
	if IsAdminRole(actor.Role) || actor.Key() == target {
		return nil
	}
	return store.ErrForbidden
}

// IsAdminRole 判断角色是否为管理员或超级管理员。
func IsAdminRole(role string) bool {
	return role == store.RoleSuperAdmin || role == store.RoleAdmin
}

// isSuperAdminRole 判断角色是否为超级管理员。
func isSuperAdminRole(role string) bool {
	return role == store.RoleSuperAdmin
}

// isPrivilegedRole 判断角色是否为特权角色（admin 或 super_admin）。
// 普通管理员不能操作这些角色的用户。
func isPrivilegedRole(role string) bool {
	switch strings.TrimSpace(role) {
	case store.RoleAdmin, store.RoleSuperAdmin:
		return true
	default:
		return false
	}
}

// requestsPrivilegedRole 检查请求的角色是否为特权角色，用于阻止 admin 越权创建/提升。
func requestsPrivilegedRole(role string) bool {
	return isPrivilegedRole(role)
}

// requestsPrivilegedRolePtr 同 requestsPrivilegedRole，但接受指针参数。nil 表示不修改角色。
func requestsPrivilegedRolePtr(role *string) bool {
	if role == nil {
		return false
	}
	return isPrivilegedRole(*role)
}
