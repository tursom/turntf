// 本文件包含 store 内部类型与 protobuf 外部类型之间的双向转换函数。
// 所有 clientProto* 函数将 store 类型转为 protobuf 类型（用于向客户端发送响应），
// 而 *FromProto 函数则将客户端请求中的 protobuf 类型转为 store 内部类型。
package api

import (
	"fmt"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// clientProtoUser 将 store.User 转换为 protobuf User 消息。
// loginName 为空时表示当前用户不可见登录名（例如非管理员查看其他用户）。
func clientProtoUser(user store.User) *internalproto.User {
	return clientProtoUserWithLoginName(user, "")
}

// clientProtoUserWithLoginName 将 store.User 转换为 protobuf User 消息，显式指定返回的 loginName。
func clientProtoUserWithLoginName(user store.User, loginName string) *internalproto.User {
	return &internalproto.User{
		NodeId:         user.NodeID,
		UserId:         user.ID,
		Username:       user.Username,
		Role:           user.Role,
		ProfileJson:    []byte(user.Profile),
		SystemReserved: user.SystemReserved,
		CreatedAt:      user.CreatedAt.String(),
		UpdatedAt:      user.UpdatedAt.String(),
		OriginNodeId:   user.OriginNodeID,
		LoginName:      loginName,
	}
}

// clientProtoSessionRef 将 store.SessionRef 转换为 protobuf SessionRef 消息。
// 无效引用返回 nil。
func clientProtoSessionRef(ref store.SessionRef) *internalproto.SessionRef {
	if !ref.Valid() {
		return nil
	}
	return &internalproto.SessionRef{
		ServingNodeId: ref.ServingNodeID,
		SessionId:     ref.SessionID,
	}
}

// clientProtoMessage 将 store.Message 转换为 protobuf Message 消息。
// Body 字节会进行深拷贝以防止外部修改。
func clientProtoMessage(message store.Message) *internalproto.Message {
	return &internalproto.Message{
		Recipient:    &internalproto.UserRef{NodeId: message.Recipient.NodeID, UserId: message.Recipient.UserID},
		NodeId:       message.NodeID,
		Seq:          message.Seq,
		Sender:       &internalproto.UserRef{NodeId: message.Sender.NodeID, UserId: message.Sender.UserID},
		Body:         append([]byte(nil), message.Body...),
		CreatedAtHlc: message.CreatedAt.String(),
	}
}

// clientProtoPacket 将 store.TransientPacket 转换为 protobuf Packet 消息（即时消息的完整表示）。
func clientProtoPacket(packet store.TransientPacket) *internalproto.Packet {
	return &internalproto.Packet{
		PacketId:      packet.PacketID,
		SourceNodeId:  packet.SourceNodeID,
		TargetNodeId:  packet.TargetNodeID,
		Recipient:     &internalproto.UserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		Sender:        &internalproto.UserRef{NodeId: packet.Sender.NodeID, UserId: packet.Sender.UserID},
		Body:          packet.Body,
		DeliveryMode:  clientDeliveryModeProto(packet.DeliveryMode),
		TargetSession: clientProtoSessionRef(packet.TargetSession),
	}
}

// clientProtoTransientAccepted 将 store.TransientPacket 转换为 TransientAccepted 事件（即时消息已送达确认）。
func clientProtoTransientAccepted(packet store.TransientPacket) *internalproto.TransientAccepted {
	return &internalproto.TransientAccepted{
		PacketId:      packet.PacketID,
		SourceNodeId:  packet.SourceNodeID,
		TargetNodeId:  packet.TargetNodeID,
		Recipient:     &internalproto.UserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		DeliveryMode:  clientDeliveryModeProto(packet.DeliveryMode),
		TargetSession: clientProtoSessionRef(packet.TargetSession),
	}
}

// clientProtoOnlineNodePresence 将在线节点存在性信息转换为 protobuf 类型。
func clientProtoOnlineNodePresence(presence store.OnlineNodePresence) *internalproto.OnlineNodePresence {
	return &internalproto.OnlineNodePresence{
		ServingNodeId: presence.ServingNodeID,
		SessionCount:  presence.SessionCount,
		TransportHint: presence.TransportHint,
	}
}

// clientProtoResolvedSession 将在线会话信息转换为 protobuf 类型，包含会话引用和传输能力。
func clientProtoResolvedSession(session store.OnlineSession) *internalproto.ResolvedSession {
	return &internalproto.ResolvedSession{
		Session:          clientProtoSessionRef(session.SessionRef),
		Transport:        session.Transport,
		TransientCapable: session.TransientCapable,
	}
}

// clientProtoUserMetadata 将 store.UserMetadata 转换为 protobuf UserMetadata 消息。
// 处理了可选字段 DeletedAt 和 ExpiresAt。
func clientProtoUserMetadata(metadata store.UserMetadata) *internalproto.UserMetadata {
	item := &internalproto.UserMetadata{
		Owner:        &internalproto.UserRef{NodeId: metadata.Owner.NodeID, UserId: metadata.Owner.UserID},
		Key:          metadata.Key,
		Value:        append([]byte(nil), metadata.Value...),
		UpdatedAt:    metadata.UpdatedAt.String(),
		OriginNodeId: metadata.OriginNodeID,
	}
	if metadata.DeletedAt != nil {
		item.DeletedAt = metadata.DeletedAt.String()
	}
	if metadata.ExpiresAt != nil {
		item.ExpiresAt = store.FormatUserMetadataExpiresAt(*metadata.ExpiresAt)
	}
	return item
}

// attachmentTypeFromProto 将 protobuf 附件类型枚举转为内部 store.AttachmentType 字符串。
func attachmentTypeFromProto(kind internalproto.AttachmentType) (store.AttachmentType, error) {
	switch kind {
	case internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_MANAGER:
		return store.AttachmentTypeChannelManager, nil
	case internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_WRITER:
		return store.AttachmentTypeChannelWriter, nil
	case internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION:
		return store.AttachmentTypeChannelSubscription, nil
	case internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST:
		return store.AttachmentTypeUserBlacklist, nil
	case internalproto.AttachmentType_ATTACHMENT_TYPE_UNSPECIFIED:
		return "", nil
	default:
		return "", fmt.Errorf("%w: unsupported attachment type %q", store.ErrInvalidInput, kind.String())
	}
}

// attachmentTypeToProto 将内部 store.AttachmentType 字符串转为 protobuf 附件类型枚举。
func attachmentTypeToProto(kind store.AttachmentType) internalproto.AttachmentType {
	switch kind {
	case store.AttachmentTypeChannelManager:
		return internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_MANAGER
	case store.AttachmentTypeChannelWriter:
		return internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_WRITER
	case store.AttachmentTypeChannelSubscription:
		return internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
	case store.AttachmentTypeUserBlacklist:
		return internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST
	default:
		return internalproto.AttachmentType_ATTACHMENT_TYPE_UNSPECIFIED
	}
}

// clientDeliveryKindFromProto 将客户端请求中的投递类型枚举转为内部 deliveryKind 常量。
func clientDeliveryKindFromProto(kind internalproto.ClientDeliveryKind) (deliveryKind, error) {
	switch kind {
	case internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_UNSPECIFIED, internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_PERSISTENT:
		return deliveryKindPersistent, nil
	case internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT:
		return deliveryKindTransient, nil
	default:
		return "", fmt.Errorf("%w: unsupported delivery kind %q", store.ErrInvalidInput, kind.String())
	}
}

// clientDeliveryModeProto 将 store 投递模式转为 protobuf 投递模式枚举。
func clientDeliveryModeProto(mode store.DeliveryMode) internalproto.ClientDeliveryMode {
	switch mode {
	case store.DeliveryModeRouteRetry:
		return internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY
	default:
		return internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT
	}
}

// clientDeliveryModeString 将 protobuf 投递模式枚举转为字符串表示。
func clientDeliveryModeString(mode internalproto.ClientDeliveryMode) string {
	switch mode {
	case internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY:
		return string(store.DeliveryModeRouteRetry)
	case internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT, internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_UNSPECIFIED:
		return string(store.DeliveryModeBestEffort)
	default:
		return ""
	}
}

// sessionRefFromProto 将 protobuf SessionRef 消息转为 store.SessionRef。
// 返回错误当引用格式无效。
func sessionRefFromProto(ref *internalproto.SessionRef) (store.SessionRef, error) {
	if ref == nil {
		return store.SessionRef{}, nil
	}
	sessionRef := store.SessionRef{
		ServingNodeID: ref.GetServingNodeId(),
		SessionID:     ref.GetSessionId(),
	}
	if !sessionRef.Valid() {
		return store.SessionRef{}, fmt.Errorf("%w: target_session is invalid", store.ErrInvalidInput)
	}
	return sessionRef, nil
}

// clientMessageSyncModeFromProto 将客户端请求中的消息同步模式枚举转为 store 内部同步模式。
func clientMessageSyncModeFromProto(mode internalproto.ClientMessageSyncMode) (store.PebbleMessageSyncMode, error) {
	switch mode {
	case internalproto.ClientMessageSyncMode_CLIENT_MESSAGE_SYNC_MODE_UNSPECIFIED:
		return store.PebbleMessageSyncModeDefault, nil
	case internalproto.ClientMessageSyncMode_CLIENT_MESSAGE_SYNC_MODE_FORCE_SYNC:
		return store.PebbleMessageSyncModeForceSync, nil
	case internalproto.ClientMessageSyncMode_CLIENT_MESSAGE_SYNC_MODE_NO_SYNC:
		return store.PebbleMessageSyncModeNoSync, nil
	default:
		return "", fmt.Errorf("%w: unsupported message sync mode %q", store.ErrInvalidInput, mode.String())
	}
}

// messageFromClientPushEvent 从事件存储的事件中提取 store.Message。
// 第二个返回值指示事件是否包含消息（false 表示事件类型不匹配，不是错误）。
func messageFromClientPushEvent(event store.Event) (store.Message, bool, error) {
	body, ok := event.Body.(*internalproto.MessageCreatedEvent)
	if !ok {
		return store.Message{}, false, nil
	}
	if body == nil {
		return store.Message{}, false, fmt.Errorf("message created event cannot be nil")
	}
	createdAt, err := clock.ParseTimestamp(body.CreatedAtHlc)
	if err != nil {
		return store.Message{}, false, err
	}
	if body.Recipient == nil {
		return store.Message{}, false, fmt.Errorf("message created event recipient cannot be nil")
	}
	if body.Sender == nil {
		return store.Message{}, false, fmt.Errorf("message created event sender cannot be nil")
	}
	return store.Message{
		Recipient: store.UserKey{NodeID: body.Recipient.NodeId, UserID: body.Recipient.UserId},
		NodeID:    body.NodeId,
		Seq:       body.Seq,
		Sender:    store.UserKey{NodeID: body.Sender.NodeId, UserID: body.Sender.UserId},
		Body:      append([]byte(nil), body.Body...),
		CreatedAt: createdAt,
	}, true, nil
}
