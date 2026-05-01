package api

import (
	"context"

	"github.com/tursom/turntf/internal/permission"
	"github.com/tursom/turntf/internal/store"
)

func (h *HTTP) permissionsEnabled() bool {
	return h != nil && h.signer != nil
}

func actorFromPrincipal(principal *requestPrincipal) *store.User {
	if principal == nil {
		return nil
	}
	return &principal.User
}

func (h *HTTP) authorizeWithPermissions(fn func() error) error {
	if !h.permissionsEnabled() {
		return nil
	}
	return fn()
}

func (h *HTTP) authorizeListUsers(principal *requestPrincipal) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListUsers(permission.ActorContext{Actor: actorFromPrincipal(principal)})
	})
}

func (h *HTTP) authorizeCreateUser(principal *requestPrincipal, requestedRole string) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanCreateUser(permission.CreateUserContext{
			Actor:         actorFromPrincipal(principal),
			RequestedRole: requestedRole,
		})
	})
}

func (h *HTTP) authorizeViewUser(principal *requestPrincipal, key store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanViewUser(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: key,
		})
	})
}

func (h *HTTP) authorizeUpdateUser(ctx context.Context, principal *requestPrincipal, target store.User, requestedRole *string, updatingPassword, updatingLoginName bool) error {
	return h.authorizeWithPermissions(func() error {
		channelManager, err := h.channelManagerForTarget(ctx, principal, target)
		if err != nil {
			return err
		}
		return permission.CanUpdateUser(permission.UpdateUserContext{
			Actor:             actorFromPrincipal(principal),
			Target:            target,
			RequestedRole:     requestedRole,
			UpdatingPassword:  updatingPassword,
			UpdatingLoginName: updatingLoginName,
			ChannelManager:    channelManager,
		})
	})
}

func (h *HTTP) authorizeDeleteUser(ctx context.Context, principal *requestPrincipal, target store.User) error {
	return h.authorizeWithPermissions(func() error {
		channelManager, err := h.channelManagerForTarget(ctx, principal, target)
		if err != nil {
			return err
		}
		return permission.CanDeleteUser(permission.DeleteUserContext{
			Actor:          actorFromPrincipal(principal),
			Target:         target,
			ChannelManager: channelManager,
		})
	})
}

func (h *HTTP) authorizeCreateMessage(ctx context.Context, principal *requestPrincipal, key store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		actor := actorFromPrincipal(principal)
		permCtx := permission.CreateMessageContext{
			Actor:     actor,
			TargetKey: key,
		}
		if actor == nil {
			return permission.CanCreateMessage(permCtx)
		}
		if actor != nil && (permission.IsAdminRole(actor.Role) || actor.Key() == key) {
			return permission.CanCreateMessage(permCtx)
		}
		target, err := h.service.GetUser(ctx, key)
		if err != nil {
			return err
		}
		permCtx.Target = &target
		if actor != nil && target.Role == store.RoleChannel {
			channelWriter, err := h.service.IsChannelWriter(ctx, key, actor.Key())
			if err != nil {
				return err
			}
			permCtx.ChannelWriter = channelWriter
		}
		return permission.CanCreateMessage(permCtx)
	})
}

func (h *HTTP) authorizeListMessages(principal *requestPrincipal, target store.User) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListMessages(permission.ListMessagesContext{
			Actor:  actorFromPrincipal(principal),
			Target: target,
		})
	})
}

func (h *HTTP) authorizeReadUserMetadata(principal *requestPrincipal, owner store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanReadUserMetadata(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: owner,
		})
	})
}

func (h *HTTP) authorizeWriteUserMetadata(principal *requestPrincipal, owner store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanWriteUserMetadata(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: owner,
		})
	})
}

func (h *HTTP) authorizeManageAttachment(ctx context.Context, principal *requestPrincipal, owner store.UserKey, attachmentType store.AttachmentType) error {
	return h.authorizeWithPermissions(func() error {
		channelManager, err := h.channelManagerForAttachment(ctx, principal, owner, attachmentType)
		if err != nil {
			return err
		}
		return permission.CanManageAttachment(permission.ManageAttachmentContext{
			Actor:          actorFromPrincipal(principal),
			Owner:          owner,
			AttachmentType: attachmentType,
			ChannelManager: channelManager,
		})
	})
}

func (h *HTTP) authorizeListAttachment(ctx context.Context, principal *requestPrincipal, owner store.UserKey, attachmentType store.AttachmentType) error {
	return h.authorizeWithPermissions(func() error {
		ownerRole, channelManager, err := h.listAttachmentFacts(ctx, principal, owner, attachmentType)
		if err != nil {
			return err
		}
		return permission.CanListAttachment(permission.ListAttachmentContext{
			Actor:          actorFromPrincipal(principal),
			Owner:          owner,
			OwnerRole:      ownerRole,
			AttachmentType: attachmentType,
			ChannelManager: channelManager,
		})
	})
}

func (h *HTTP) authorizeManageSubscription(principal *requestPrincipal, subscriber store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanManageSubscription(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: subscriber,
		})
	})
}

func (h *HTTP) authorizeListSubscription(principal *requestPrincipal, subscriber store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListSubscription(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: subscriber,
		})
	})
}

func (h *HTTP) authorizeManageBlacklist(principal *requestPrincipal, owner store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanManageBlacklist(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: owner,
		})
	})
}

func (h *HTTP) authorizeListBlacklist(principal *requestPrincipal, owner store.UserKey) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListBlacklist(permission.SelfScopedContext{
			Actor:     actorFromPrincipal(principal),
			TargetKey: owner,
		})
	})
}

func (h *HTTP) authorizeListEvents(principal *requestPrincipal) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListEvents(permission.ActorContext{Actor: actorFromPrincipal(principal)})
	})
}

func (h *HTTP) authorizeReadOpsStatus(principal *requestPrincipal) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanReadOpsStatus(permission.ActorContext{Actor: actorFromPrincipal(principal)})
	})
}

func (h *HTTP) authorizeReadMetrics(principal *requestPrincipal) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanReadMetrics(permission.ActorContext{Actor: actorFromPrincipal(principal)})
	})
}

func (h *HTTP) authorizeListClusterNodes(principal *requestPrincipal) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListClusterNodes(permission.ActorContext{Actor: actorFromPrincipal(principal)})
	})
}

func (h *HTTP) authorizeListLoggedInUsers(principal *requestPrincipal) error {
	return h.authorizeWithPermissions(func() error {
		return permission.CanListLoggedInUsers(permission.ActorContext{Actor: actorFromPrincipal(principal)})
	})
}

func (h *HTTP) channelManagerForTarget(ctx context.Context, principal *requestPrincipal, target store.User) (bool, error) {
	if principal == nil || permission.IsAdminRole(principal.User.Role) || target.Role != store.RoleChannel {
		return false, nil
	}
	return h.service.IsChannelManager(ctx, target.Key(), principal.User.Key())
}

func (h *HTTP) channelManagerForAttachment(ctx context.Context, principal *requestPrincipal, owner store.UserKey, attachmentType store.AttachmentType) (bool, error) {
	if principal == nil || permission.IsAdminRole(principal.User.Role) {
		return false, nil
	}
	switch attachmentType {
	case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
		return h.service.IsChannelManager(ctx, owner, principal.User.Key())
	default:
		return false, nil
	}
}

func (h *HTTP) listAttachmentFacts(ctx context.Context, principal *requestPrincipal, owner store.UserKey, attachmentType store.AttachmentType) (string, bool, error) {
	if principal == nil || permission.IsAdminRole(principal.User.Role) {
		return "", false, nil
	}
	if attachmentType != "" {
		channelManager, err := h.channelManagerForAttachment(ctx, principal, owner, attachmentType)
		return "", channelManager, err
	}
	if principal.User.Key() == owner {
		return "", false, nil
	}
	ownerUser, err := h.service.GetUser(ctx, owner)
	if err != nil {
		return "", false, err
	}
	channelManager := false
	if ownerUser.Role == store.RoleChannel {
		channelManager, err = h.service.IsChannelManager(ctx, owner, principal.User.Key())
		if err != nil {
			return "", false, err
		}
	}
	return ownerUser.Role, channelManager, nil
}
