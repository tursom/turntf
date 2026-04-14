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
		PacketId:     packet.PacketID,
		SourceNodeId: packet.SourceNodeID,
		TargetNodeId: packet.TargetNodeID,
		Recipient:    &internalproto.UserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		Sender:       &internalproto.UserRef{NodeId: packet.Sender.NodeID, UserId: packet.Sender.UserID},
		Body:         append([]byte(nil), packet.Body...),
		DeliveryMode: clientDeliveryModeProto(packet.DeliveryMode),
	}
}

func clientProtoTransientAccepted(packet store.TransientPacket) *internalproto.TransientAccepted {
	return &internalproto.TransientAccepted{
		PacketId:     packet.PacketID,
		SourceNodeId: packet.SourceNodeID,
		TargetNodeId: packet.TargetNodeID,
		Recipient:    &internalproto.UserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		DeliveryMode: clientDeliveryModeProto(packet.DeliveryMode),
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
