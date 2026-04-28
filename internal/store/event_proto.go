package store

import (
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

type EventType string

const (
	EventTypeUserCreated            EventType = "user_created"
	EventTypeUserUpdated            EventType = "user_updated"
	EventTypeUserDeleted            EventType = "user_deleted"
	EventTypeMessageCreated         EventType = "message_created"
	EventTypeUserAttachmentUpserted EventType = "user_attachment_upserted"
	EventTypeUserAttachmentDeleted  EventType = "user_attachment_deleted"
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

func userAttachmentUpsertedProtoFromAttachment(attachment Attachment) *internalproto.UserAttachmentUpsertedEvent {
	return &internalproto.UserAttachmentUpsertedEvent{
		Owner:          &internalproto.ClusterUserRef{NodeId: attachment.Owner.NodeID, UserId: attachment.Owner.UserID},
		Subject:        &internalproto.ClusterUserRef{NodeId: attachment.Subject.NodeID, UserId: attachment.Subject.UserID},
		AttachmentType: string(attachment.Type),
		ConfigJson:     attachment.ConfigJSON,
		AttachedAtHlc:  attachment.AttachedAt.String(),
		OriginNodeId:   attachment.OriginNodeID,
	}
}

func userAttachmentDeletedProtoFromAttachment(attachment Attachment) *internalproto.UserAttachmentDeletedEvent {
	event := &internalproto.UserAttachmentDeletedEvent{
		Owner:          &internalproto.ClusterUserRef{NodeId: attachment.Owner.NodeID, UserId: attachment.Owner.UserID},
		Subject:        &internalproto.ClusterUserRef{NodeId: attachment.Subject.NodeID, UserId: attachment.Subject.UserID},
		AttachmentType: string(attachment.Type),
		ConfigJson:     attachment.ConfigJSON,
		AttachedAtHlc:  attachment.AttachedAt.String(),
		OriginNodeId:   attachment.OriginNodeID,
	}
	if attachment.DeletedAt != nil {
		event.DeletedAtHlc = attachment.DeletedAt.String()
	}
	return event
}
