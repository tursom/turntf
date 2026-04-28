package api

import (
	"fmt"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func clientProtoUser(user store.User) *internalproto.User {
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
	}
}

func clientProtoSessionRef(ref store.SessionRef) *internalproto.SessionRef {
	if !ref.Valid() {
		return nil
	}
	return &internalproto.SessionRef{
		ServingNodeId: ref.ServingNodeID,
		SessionId:     ref.SessionID,
	}
}

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

func clientProtoPacket(packet store.TransientPacket) *internalproto.Packet {
	return &internalproto.Packet{
		PacketId:      packet.PacketID,
		SourceNodeId:  packet.SourceNodeID,
		TargetNodeId:  packet.TargetNodeID,
		Recipient:     &internalproto.UserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		Sender:        &internalproto.UserRef{NodeId: packet.Sender.NodeID, UserId: packet.Sender.UserID},
		Body:          append([]byte(nil), packet.Body...),
		DeliveryMode:  clientDeliveryModeProto(packet.DeliveryMode),
		TargetSession: clientProtoSessionRef(packet.TargetSession),
	}
}

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

func clientProtoOnlineNodePresence(presence store.OnlineNodePresence) *internalproto.OnlineNodePresence {
	return &internalproto.OnlineNodePresence{
		ServingNodeId: presence.ServingNodeID,
		SessionCount:  presence.SessionCount,
		TransportHint: presence.TransportHint,
	}
}

func clientProtoResolvedSession(session store.OnlineSession) *internalproto.ResolvedSession {
	return &internalproto.ResolvedSession{
		Session:          clientProtoSessionRef(session.SessionRef),
		Transport:        session.Transport,
		TransientCapable: session.TransientCapable,
	}
}

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

func clientDeliveryModeProto(mode store.DeliveryMode) internalproto.ClientDeliveryMode {
	switch mode {
	case store.DeliveryModeRouteRetry:
		return internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY
	default:
		return internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT
	}
}

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
