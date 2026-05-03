package permission

import (
	"context"

	"github.com/tursom/turntf/internal/store"
)

// FactResolver 提供权限检查所需的外部事实数据。
// 当纯权限函数需要判断频道管理/写入关系时，通过此接口查询存储层。
type FactResolver interface {
	GetUser(context.Context, store.UserKey) (store.User, error)
	IsChannelManager(context.Context, store.UserKey, store.UserKey) (bool, error)
	IsChannelWriter(context.Context, store.UserKey, store.UserKey) (bool, error)
}

// Authorizer 权限授权器，将纯权限函数（Can*）与外部事实解析（FactResolver）结合。
// 当 enabled 为 false 时跳过所有权限检查。
// 对于频道相关的操作，Authorizer 会先判断是否可以走快速路径（管理员/本人），
// 否则通过 FactResolver 查询频道成员关系后再调用对应的 Can* 函数。
type Authorizer struct {
	resolver FactResolver
	enabled  bool
}

// NewAuthorizer 创建权限授权器。enabled 为 false 时所有检查直接通过。
func NewAuthorizer(resolver FactResolver, enabled bool) *Authorizer {
	return &Authorizer{
		resolver: resolver,
		enabled:  enabled,
	}
}

// ListUsers 检查是否允许列出所有用户。
func (a *Authorizer) ListUsers(actor *store.User) error {
	return a.authorize(func() error {
		return CanListUsers(ActorContext{Actor: actor})
	})
}

// CreateUser 检查是否允许创建指定角色的用户。
func (a *Authorizer) CreateUser(actor *store.User, requestedRole string) error {
	return a.authorize(func() error {
		return CanCreateUser(CreateUserContext{
			Actor:         actor,
			RequestedRole: requestedRole,
		})
	})
}

// ViewUser 检查是否允许查看指定用户的信息。
func (a *Authorizer) ViewUser(actor *store.User, key store.UserKey) error {
	return a.authorize(func() error {
		return CanViewUser(SelfScopedContext{
			Actor:     actor,
			TargetKey: key,
		})
	})
}

// UpdateUser 检查是否允许更新用户。当操作目标为频道用户且操作者非管理员时，
// 通过 FactResolver 查询是否为频道管理者。
func (a *Authorizer) UpdateUser(ctx context.Context, actor *store.User, target store.User, requestedRole *string, updatingPassword, updatingLoginName bool) error {
	return a.authorize(func() error {
		permCtx := UpdateUserContext{
			Actor:             actor,
			Target:            target,
			RequestedRole:     requestedRole,
			UpdatingPassword:  updatingPassword,
			UpdatingLoginName: updatingLoginName,
		}
		// 管理员或非频道目标走快速路径，无需查询频道关系
		if actor == nil || IsAdminRole(actor.Role) || target.Role != store.RoleChannel {
			return CanUpdateUser(permCtx)
		}
		resolver, err := a.requireResolver()
		if err != nil {
			return err
		}
		channelManager, err := resolver.IsChannelManager(ctx, target.Key(), actor.Key())
		if err != nil {
			return err
		}
		permCtx.ChannelManager = channelManager
		return CanUpdateUser(permCtx)
	})
}

// DeleteUser 检查是否允许删除用户。当操作目标为频道用户且操作者非管理员时，
// 通过 FactResolver 查询是否为频道管理者。
func (a *Authorizer) DeleteUser(ctx context.Context, actor *store.User, target store.User) error {
	return a.authorize(func() error {
		permCtx := DeleteUserContext{
			Actor:  actor,
			Target: target,
		}
		if actor == nil || IsAdminRole(actor.Role) || target.Role != store.RoleChannel {
			return CanDeleteUser(permCtx)
		}
		resolver, err := a.requireResolver()
		if err != nil {
			return err
		}
		channelManager, err := resolver.IsChannelManager(ctx, target.Key(), actor.Key())
		if err != nil {
			return err
		}
		permCtx.ChannelManager = channelManager
		return CanDeleteUser(permCtx)
	})
}

// CreateMessage 检查是否允许向目标用户发送消息。当操作者非管理员且非向自己发送时，
// 通过 FactResolver 获取目标用户信息及频道写入权限。
func (a *Authorizer) CreateMessage(ctx context.Context, actor *store.User, key store.UserKey) error {
	return a.authorize(func() error {
		permCtx := CreateMessageContext{
			Actor:     actor,
			TargetKey: key,
		}
		// 管理员或向自己发送，无需查询外部事实
		if actor == nil || IsAdminRole(actor.Role) || actor.Key() == key {
			return CanCreateMessage(permCtx)
		}
		resolver, err := a.requireResolver()
		if err != nil {
			return err
		}
		target, err := resolver.GetUser(ctx, key)
		if err != nil {
			return err
		}
		permCtx.Target = &target
		// 目标为频道用户时，需查询 ChannelWriter 权限
		if target.Role == store.RoleChannel {
			channelWriter, err := resolver.IsChannelWriter(ctx, key, actor.Key())
			if err != nil {
				return err
			}
			permCtx.ChannelWriter = channelWriter
		}
		return CanCreateMessage(permCtx)
	})
}

// ListMessages 检查是否允许查看消息列表。
func (a *Authorizer) ListMessages(actor *store.User, target store.User) error {
	return a.authorize(func() error {
		return CanListMessages(ListMessagesContext{
			Actor:  actor,
			Target: target,
		})
	})
}

// ReadUserMetadata 检查是否允许读取用户元数据。
func (a *Authorizer) ReadUserMetadata(ctx context.Context, actor *store.User, owner store.User) error {
	return a.authorize(func() error {
		permCtx := UserMetadataContext{
			Actor: actor,
			Owner: owner,
		}
		if actor == nil || IsAdminRole(actor.Role) || actor.Key() == owner.Key() || owner.Role != store.RoleChannel {
			return CanReadUserMetadata(permCtx)
		}
		resolver, err := a.requireResolver()
		if err != nil {
			return err
		}
		channelManager, err := resolver.IsChannelManager(ctx, owner.Key(), actor.Key())
		if err != nil {
			return err
		}
		permCtx.ChannelManager = channelManager
		return CanReadUserMetadata(permCtx)
	})
}

// WriteUserMetadata 检查是否允许修改用户元数据。
func (a *Authorizer) WriteUserMetadata(ctx context.Context, actor *store.User, owner store.User) error {
	return a.authorize(func() error {
		permCtx := UserMetadataContext{
			Actor: actor,
			Owner: owner,
		}
		if actor == nil || IsAdminRole(actor.Role) || actor.Key() == owner.Key() || owner.Role != store.RoleChannel {
			return CanWriteUserMetadata(permCtx)
		}
		resolver, err := a.requireResolver()
		if err != nil {
			return err
		}
		channelManager, err := resolver.IsChannelManager(ctx, owner.Key(), actor.Key())
		if err != nil {
			return err
		}
		permCtx.ChannelManager = channelManager
		return CanWriteUserMetadata(permCtx)
	})
}

// ManageAttachment 检查是否允许管理附件。当操作者非管理员且操作为频道相关附件类型时，
// 通过 FactResolver 查询是否为频道管理者。
func (a *Authorizer) ManageAttachment(ctx context.Context, actor *store.User, owner store.UserKey, attachmentType store.AttachmentType) error {
	return a.authorize(func() error {
		permCtx := ManageAttachmentContext{
			Actor:          actor,
			Owner:          owner,
			AttachmentType: attachmentType,
		}
		// 管理员走快速路径
		if actor == nil || IsAdminRole(actor.Role) {
			return CanManageAttachment(permCtx)
		}
		// 频道相关附件需查询 ChannelManager
		switch attachmentType {
		case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
			resolver, err := a.requireResolver()
			if err != nil {
				return err
			}
			channelManager, err := resolver.IsChannelManager(ctx, owner, actor.Key())
			if err != nil {
				return err
			}
			permCtx.ChannelManager = channelManager
		}
		return CanManageAttachment(permCtx)
	})
}

// ListAttachment 检查是否允许列出附件。处理逻辑：
//   - 指定附件类型：走管理权限检查路径
//   - 不指定类型且非本人：查询 owner 角色和频道管理关系后判断
func (a *Authorizer) ListAttachment(ctx context.Context, actor *store.User, owner store.UserKey, attachmentType store.AttachmentType) error {
	return a.authorize(func() error {
		permCtx := ListAttachmentContext{
			Actor:          actor,
			Owner:          owner,
			AttachmentType: attachmentType,
		}
		if actor == nil || IsAdminRole(actor.Role) {
			return CanListAttachment(permCtx)
		}
		// 指定了附件类型，可能需要频道管理查询
		if attachmentType != "" {
			switch attachmentType {
			case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
				resolver, err := a.requireResolver()
				if err != nil {
					return err
				}
				channelManager, err := resolver.IsChannelManager(ctx, owner, actor.Key())
				if err != nil {
					return err
				}
				permCtx.ChannelManager = channelManager
			}
			return CanListAttachment(permCtx)
		}
		// 查看自己的附件
		if actor.Key() == owner {
			return CanListAttachment(permCtx)
		}
		// 不确定类型：查 owner 角色和频道管理关系
		resolver, err := a.requireResolver()
		if err != nil {
			return err
		}
		ownerUser, err := resolver.GetUser(ctx, owner)
		if err != nil {
			return err
		}
		permCtx.OwnerRole = ownerUser.Role
		if ownerUser.Role == store.RoleChannel {
			channelManager, err := resolver.IsChannelManager(ctx, owner, actor.Key())
			if err != nil {
				return err
			}
			permCtx.ChannelManager = channelManager
		}
		return CanListAttachment(permCtx)
	})
}

// ManageSubscription 检查是否允许管理订阅。
func (a *Authorizer) ManageSubscription(actor *store.User, subscriber store.UserKey) error {
	return a.authorize(func() error {
		return CanManageSubscription(SelfScopedContext{
			Actor:     actor,
			TargetKey: subscriber,
		})
	})
}

// ListSubscription 检查是否允许查看订阅列表。
func (a *Authorizer) ListSubscription(actor *store.User, subscriber store.UserKey) error {
	return a.authorize(func() error {
		return CanListSubscription(SelfScopedContext{
			Actor:     actor,
			TargetKey: subscriber,
		})
	})
}

// ManageBlacklist 检查是否允许管理黑名单。
func (a *Authorizer) ManageBlacklist(actor *store.User, owner store.UserKey) error {
	return a.authorize(func() error {
		return CanManageBlacklist(SelfScopedContext{
			Actor:     actor,
			TargetKey: owner,
		})
	})
}

// ListBlacklist 检查是否允许查看黑名单。
func (a *Authorizer) ListBlacklist(actor *store.User, owner store.UserKey) error {
	return a.authorize(func() error {
		return CanListBlacklist(SelfScopedContext{
			Actor:     actor,
			TargetKey: owner,
		})
	})
}

// ListEvents 检查是否允许查看系统事件。
func (a *Authorizer) ListEvents(actor *store.User) error {
	return a.authorize(func() error {
		return CanListEvents(ActorContext{Actor: actor})
	})
}

// ReadOpsStatus 检查是否允许读取运维状态。
func (a *Authorizer) ReadOpsStatus(actor *store.User) error {
	return a.authorize(func() error {
		return CanReadOpsStatus(ActorContext{Actor: actor})
	})
}

// ReadMetrics 检查是否允许读取系统指标。
func (a *Authorizer) ReadMetrics(actor *store.User) error {
	return a.authorize(func() error {
		return CanReadMetrics(ActorContext{Actor: actor})
	})
}

// ListClusterNodes 检查是否允许列出集群节点。
func (a *Authorizer) ListClusterNodes(actor *store.User) error {
	return a.authorize(func() error {
		return CanListClusterNodes(ActorContext{Actor: actor})
	})
}

// ListLoggedInUsers 检查是否允许查看在线用户列表。
func (a *Authorizer) ListLoggedInUsers(actor *store.User) error {
	return a.authorize(func() error {
		return CanListLoggedInUsers(ActorContext{Actor: actor})
	})
}

// authorize 权限检查的总闸门。当 Authorizer 为 nil 或 disabled 时跳过所有检查。
func (a *Authorizer) authorize(fn func() error) error {
	if a == nil {
		return store.ErrInvalidInput
	}
	if !a.enabled {
		return nil
	}
	return fn()
}

// requireResolver 获取 FactResolver 实例，若不可用则返回错误。
func (a *Authorizer) requireResolver() (FactResolver, error) {
	if a == nil || a.resolver == nil {
		return nil, store.ErrInvalidInput
	}
	return a.resolver, nil
}
