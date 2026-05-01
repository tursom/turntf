package store

import (
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

// EventType 是事件溯源系统中事件的类型枚举，由对应 protobuf 消息类型派生。
type EventType string

const (
	// EventTypeUserCreated 表示用户创建事件。
	EventTypeUserCreated EventType = "user_created"
	// EventTypeUserUpdated 表示用户更新事件。
	EventTypeUserUpdated EventType = "user_updated"
	// EventTypeUserDeleted 表示用户删除事件。
	EventTypeUserDeleted EventType = "user_deleted"
	// EventTypeMessageCreated 表示消息创建事件。
	EventTypeMessageCreated EventType = "message_created"
	// EventTypeUserAttachmentUpserted 表示附件创建或更新事件。
	EventTypeUserAttachmentUpserted EventType = "user_attachment_upserted"
	// EventTypeUserAttachmentDeleted 表示附件删除事件。
	EventTypeUserAttachmentDeleted EventType = "user_attachment_deleted"
	// EventTypeUserMetadataUpserted 表示用户元数据创建或更新事件。
	EventTypeUserMetadataUpserted EventType = "user_metadata_upserted"
	// EventTypeUserMetadataDeleted 表示用户元数据删除事件。
	EventTypeUserMetadataDeleted EventType = "user_metadata_deleted"
	// EventTypeUserLoginNameUpserted 表示登录名绑定创建或更新事件。
	EventTypeUserLoginNameUpserted EventType = "user_login_name_upserted"
	// EventTypeUserLoginNameDeleted 表示登录名绑定删除事件。
	EventTypeUserLoginNameDeleted EventType = "user_login_name_deleted"
)

// eventTypeOf 从 protobuf EventBody 提取对应的 EventType。
func eventTypeOf(body internalproto.EventBody) EventType {
	return EventType(internalproto.EventTypeFromBody(body))
}

// userCreatedProtoFromUser 将 User 领域对象转换为 UserCreatedEvent protobuf 消息。
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

// userUpdatedProtoFromUser 将 User 领域对象转换为 UserUpdatedEvent protobuf 消息。
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

// userDeletedProtoFromKey 从 UserKey 和删除时间戳构造 UserDeletedEvent protobuf 消息。
func userDeletedProtoFromKey(key UserKey, deletedAt clock.Timestamp) *internalproto.UserDeletedEvent {
	return &internalproto.UserDeletedEvent{
		NodeId:       key.NodeID,
		UserId:       key.UserID,
		DeletedAtHlc: deletedAt.String(),
	}
}

// messageCreatedProtoFromMessage 将 Message 领域对象转换为 MessageCreatedEvent protobuf 消息。
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

// userAttachmentUpsertedProtoFromAttachment 将 Attachment 转换为 UserAttachmentUpsertedEvent protobuf 消息。
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

// userAttachmentDeletedProtoFromAttachment 将 Attachment 转换为 UserAttachmentDeletedEvent protobuf 消息。
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

// userMetadataUpsertedProtoFromUserMetadata 将 UserMetadata 转换为 UserMetadataUpsertedEvent protobuf 消息。
func userMetadataUpsertedProtoFromUserMetadata(metadata UserMetadata) *internalproto.UserMetadataUpsertedEvent {
	event := &internalproto.UserMetadataUpsertedEvent{
		Owner:        &internalproto.ClusterUserRef{NodeId: metadata.Owner.NodeID, UserId: metadata.Owner.UserID},
		Key:          metadata.Key,
		Value:        append([]byte(nil), metadata.Value...),
		UpdatedAtHlc: metadata.UpdatedAt.String(),
		OriginNodeId: metadata.OriginNodeID,
	}
	if metadata.ExpiresAt != nil {
		event.ExpiresAt = FormatUserMetadataExpiresAt(*metadata.ExpiresAt)
	}
	return event
}

// userMetadataDeletedProtoFromUserMetadata 将 UserMetadata 转换为 UserMetadataDeletedEvent protobuf 消息。
func userMetadataDeletedProtoFromUserMetadata(metadata UserMetadata) *internalproto.UserMetadataDeletedEvent {
	event := &internalproto.UserMetadataDeletedEvent{
		Owner:        &internalproto.ClusterUserRef{NodeId: metadata.Owner.NodeID, UserId: metadata.Owner.UserID},
		Key:          metadata.Key,
		OriginNodeId: metadata.OriginNodeID,
	}
	if metadata.DeletedAt != nil {
		event.DeletedAtHlc = metadata.DeletedAt.String()
	}
	return event
}

// userLoginNameUpsertedProtoFromBinding 将 UserLoginName 转换为 UserLoginNameUpsertedEvent protobuf 消息。
func userLoginNameUpsertedProtoFromBinding(binding UserLoginName) *internalproto.UserLoginNameUpsertedEvent {
	return &internalproto.UserLoginNameUpsertedEvent{
		LoginName:    binding.LoginName,
		UserNodeId:   binding.User.NodeID,
		UserId:       binding.User.UserID,
		BoundAtHlc:   binding.BoundAt.String(),
		OriginNodeId: binding.OriginNodeID,
	}
}

// userLoginNameDeletedProtoFromBinding 将 UserLoginName 转换为 UserLoginNameDeletedEvent protobuf 消息。
func userLoginNameDeletedProtoFromBinding(binding UserLoginName) *internalproto.UserLoginNameDeletedEvent {
	event := &internalproto.UserLoginNameDeletedEvent{
		LoginName:    binding.LoginName,
		UserNodeId:   binding.User.NodeID,
		UserId:       binding.User.UserID,
		BoundAtHlc:   binding.BoundAt.String(),
		OriginNodeId: binding.OriginNodeID,
	}
	if binding.DeletedAt != nil {
		event.DeletedAtHlc = binding.DeletedAt.String()
	}
	return event
}
