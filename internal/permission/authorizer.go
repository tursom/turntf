package permission

import (
	"context"

	"github.com/tursom/turntf/internal/store"
)

type FactResolver interface {
	GetUser(context.Context, store.UserKey) (store.User, error)
	IsChannelManager(context.Context, store.UserKey, store.UserKey) (bool, error)
	IsChannelWriter(context.Context, store.UserKey, store.UserKey) (bool, error)
}

type Authorizer struct {
	resolver FactResolver
	enabled  bool
}

func NewAuthorizer(resolver FactResolver, enabled bool) *Authorizer {
	return &Authorizer{
		resolver: resolver,
		enabled:  enabled,
	}
}

func (a *Authorizer) ListUsers(actor *store.User) error {
	return a.authorize(func() error {
		return CanListUsers(ActorContext{Actor: actor})
	})
}

func (a *Authorizer) CreateUser(actor *store.User, requestedRole string) error {
	return a.authorize(func() error {
		return CanCreateUser(CreateUserContext{
			Actor:         actor,
			RequestedRole: requestedRole,
		})
	})
}

func (a *Authorizer) ViewUser(actor *store.User, key store.UserKey) error {
	return a.authorize(func() error {
		return CanViewUser(SelfScopedContext{
			Actor:     actor,
			TargetKey: key,
		})
	})
}

func (a *Authorizer) UpdateUser(ctx context.Context, actor *store.User, target store.User, requestedRole *string, updatingPassword, updatingLoginName bool) error {
	return a.authorize(func() error {
		permCtx := UpdateUserContext{
			Actor:             actor,
			Target:            target,
			RequestedRole:     requestedRole,
			UpdatingPassword:  updatingPassword,
			UpdatingLoginName: updatingLoginName,
		}
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

func (a *Authorizer) CreateMessage(ctx context.Context, actor *store.User, key store.UserKey) error {
	return a.authorize(func() error {
		permCtx := CreateMessageContext{
			Actor:     actor,
			TargetKey: key,
		}
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

func (a *Authorizer) ListMessages(actor *store.User, target store.User) error {
	return a.authorize(func() error {
		return CanListMessages(ListMessagesContext{
			Actor:  actor,
			Target: target,
		})
	})
}

func (a *Authorizer) ReadUserMetadata(actor *store.User, owner store.UserKey) error {
	return a.authorize(func() error {
		return CanReadUserMetadata(SelfScopedContext{
			Actor:     actor,
			TargetKey: owner,
		})
	})
}

func (a *Authorizer) WriteUserMetadata(actor *store.User, owner store.UserKey) error {
	return a.authorize(func() error {
		return CanWriteUserMetadata(SelfScopedContext{
			Actor:     actor,
			TargetKey: owner,
		})
	})
}

func (a *Authorizer) ManageAttachment(ctx context.Context, actor *store.User, owner store.UserKey, attachmentType store.AttachmentType) error {
	return a.authorize(func() error {
		permCtx := ManageAttachmentContext{
			Actor:          actor,
			Owner:          owner,
			AttachmentType: attachmentType,
		}
		if actor == nil || IsAdminRole(actor.Role) {
			return CanManageAttachment(permCtx)
		}
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
		if actor.Key() == owner {
			return CanListAttachment(permCtx)
		}
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

func (a *Authorizer) ManageSubscription(actor *store.User, subscriber store.UserKey) error {
	return a.authorize(func() error {
		return CanManageSubscription(SelfScopedContext{
			Actor:     actor,
			TargetKey: subscriber,
		})
	})
}

func (a *Authorizer) ListSubscription(actor *store.User, subscriber store.UserKey) error {
	return a.authorize(func() error {
		return CanListSubscription(SelfScopedContext{
			Actor:     actor,
			TargetKey: subscriber,
		})
	})
}

func (a *Authorizer) ManageBlacklist(actor *store.User, owner store.UserKey) error {
	return a.authorize(func() error {
		return CanManageBlacklist(SelfScopedContext{
			Actor:     actor,
			TargetKey: owner,
		})
	})
}

func (a *Authorizer) ListBlacklist(actor *store.User, owner store.UserKey) error {
	return a.authorize(func() error {
		return CanListBlacklist(SelfScopedContext{
			Actor:     actor,
			TargetKey: owner,
		})
	})
}

func (a *Authorizer) ListEvents(actor *store.User) error {
	return a.authorize(func() error {
		return CanListEvents(ActorContext{Actor: actor})
	})
}

func (a *Authorizer) ReadOpsStatus(actor *store.User) error {
	return a.authorize(func() error {
		return CanReadOpsStatus(ActorContext{Actor: actor})
	})
}

func (a *Authorizer) ReadMetrics(actor *store.User) error {
	return a.authorize(func() error {
		return CanReadMetrics(ActorContext{Actor: actor})
	})
}

func (a *Authorizer) ListClusterNodes(actor *store.User) error {
	return a.authorize(func() error {
		return CanListClusterNodes(ActorContext{Actor: actor})
	})
}

func (a *Authorizer) ListLoggedInUsers(actor *store.User) error {
	return a.authorize(func() error {
		return CanListLoggedInUsers(ActorContext{Actor: actor})
	})
}

func (a *Authorizer) authorize(fn func() error) error {
	if a == nil {
		return store.ErrInvalidInput
	}
	if !a.enabled {
		return nil
	}
	return fn()
}

func (a *Authorizer) requireResolver() (FactResolver, error) {
	if a == nil || a.resolver == nil {
		return nil, store.ErrInvalidInput
	}
	return a.resolver, nil
}
