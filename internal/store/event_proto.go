package store

import (
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

type EventType string

const (
	EventTypeUserCreated         EventType = "user_created"
	EventTypeUserUpdated         EventType = "user_updated"
	EventTypeUserDeleted         EventType = "user_deleted"
	EventTypeMessageCreated      EventType = "message_created"
	EventTypeChannelSubscribed   EventType = "channel_subscribed"
	EventTypeChannelUnsubscribed EventType = "channel_unsubscribed"
	EventTypeUserBlocked         EventType = "user_blocked"
	EventTypeUserUnblocked       EventType = "user_unblocked"
)

func eventTypeOf(body internalproto.EventBody) EventType {
	return EventType(internalproto.EventTypeFromBody(body))
}

func userCreatedProtoFromUser(user User) *internalproto.UserCreatedEvent {
	return &internalproto.UserCreatedEvent{
		NodeId:              user.NodeID,
		UserId:              user.ID,
		Username:            user.Username,
		PasswordHash:        user.PasswordHash,
		Profile:             user.Profile,
		Role:                user.Role,
		SystemReserved:      user.SystemReserved,
		CreatedAtHlc:        user.CreatedAt.String(),
		UpdatedAtHlc:        user.UpdatedAt.String(),
		VersionUsername:     user.VersionUsername.String(),
		VersionPasswordHash: user.VersionPasswordHash.String(),
		VersionProfile:      user.VersionProfile.String(),
		VersionRole:         user.VersionRole.String(),
		OriginNodeId:        user.OriginNodeID,
	}
}

func userUpdatedProtoFromUser(user User) *internalproto.UserUpdatedEvent {
	event := &internalproto.UserUpdatedEvent{
		NodeId:              user.NodeID,
		UserId:              user.ID,
		Username:            user.Username,
		PasswordHash:        user.PasswordHash,
		Profile:             user.Profile,
		Role:                user.Role,
		SystemReserved:      user.SystemReserved,
		CreatedAtHlc:        user.CreatedAt.String(),
		UpdatedAtHlc:        user.UpdatedAt.String(),
		VersionUsername:     user.VersionUsername.String(),
		VersionPasswordHash: user.VersionPasswordHash.String(),
		VersionProfile:      user.VersionProfile.String(),
		VersionRole:         user.VersionRole.String(),
		OriginNodeId:        user.OriginNodeID,
	}
	if user.DeletedAt != nil {
		event.DeletedAtHlc = user.DeletedAt.String()
	}
	if user.VersionDeleted != nil {
		event.VersionDeleted = user.VersionDeleted.String()
	}
	return event
}

func userDeletedProtoFromKey(key UserKey, deletedAt clock.Timestamp) *internalproto.UserDeletedEvent {
	return &internalproto.UserDeletedEvent{
		NodeId:       key.NodeID,
		UserId:       key.UserID,
		DeletedAtHlc: deletedAt.String(),
	}
}

func messageCreatedProtoFromMessage(message Message) *internalproto.MessageCreatedEvent {
	return &internalproto.MessageCreatedEvent{
		Recipient:    &internalproto.ClusterUserRef{NodeId: message.Recipient.NodeID, UserId: message.Recipient.UserID},
		NodeId:       message.NodeID,
		Seq:          message.Seq,
		Sender:       &internalproto.ClusterUserRef{NodeId: message.Sender.NodeID, UserId: message.Sender.UserID},
		Body:         message.Body,
		CreatedAtHlc: message.CreatedAt.String(),
	}
}

func channelSubscribedProtoFromSubscription(subscription Subscription) *internalproto.ChannelSubscribedEvent {
	return &internalproto.ChannelSubscribedEvent{
		Subscriber:      &internalproto.ClusterUserRef{NodeId: subscription.Subscriber.NodeID, UserId: subscription.Subscriber.UserID},
		Channel:         &internalproto.ClusterUserRef{NodeId: subscription.Channel.NodeID, UserId: subscription.Channel.UserID},
		SubscribedAtHlc: subscription.SubscribedAt.String(),
		OriginNodeId:    subscription.OriginNodeID,
	}
}

func channelUnsubscribedProtoFromSubscription(subscription Subscription) *internalproto.ChannelUnsubscribedEvent {
	event := &internalproto.ChannelUnsubscribedEvent{
		Subscriber:      &internalproto.ClusterUserRef{NodeId: subscription.Subscriber.NodeID, UserId: subscription.Subscriber.UserID},
		Channel:         &internalproto.ClusterUserRef{NodeId: subscription.Channel.NodeID, UserId: subscription.Channel.UserID},
		SubscribedAtHlc: subscription.SubscribedAt.String(),
		OriginNodeId:    subscription.OriginNodeID,
	}
	if subscription.DeletedAt != nil {
		event.DeletedAtHlc = subscription.DeletedAt.String()
	}
	return event
}

func userBlockedProtoFromBlacklist(entry BlacklistEntry) *internalproto.UserBlockedEvent {
	return &internalproto.UserBlockedEvent{
		Owner:        &internalproto.ClusterUserRef{NodeId: entry.Owner.NodeID, UserId: entry.Owner.UserID},
		Blocked:      &internalproto.ClusterUserRef{NodeId: entry.Blocked.NodeID, UserId: entry.Blocked.UserID},
		BlockedAtHlc: entry.BlockedAt.String(),
		OriginNodeId: entry.OriginNodeID,
	}
}

func userUnblockedProtoFromBlacklist(entry BlacklistEntry) *internalproto.UserUnblockedEvent {
	event := &internalproto.UserUnblockedEvent{
		Owner:        &internalproto.ClusterUserRef{NodeId: entry.Owner.NodeID, UserId: entry.Owner.UserID},
		Blocked:      &internalproto.ClusterUserRef{NodeId: entry.Blocked.NodeID, UserId: entry.Blocked.UserID},
		BlockedAtHlc: entry.BlockedAt.String(),
		OriginNodeId: entry.OriginNodeID,
	}
	if entry.DeletedAt != nil {
		event.DeletedAtHlc = entry.DeletedAt.String()
	}
	return event
}
