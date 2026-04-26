package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (s *clientWSSession) handleSendMessage(ctx context.Context, req *internalproto.SendMessageRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "send_message cannot be empty", 0)
	}
	if req.Target == nil || req.Target.NodeId <= 0 || req.Target.UserId <= 0 {
		return s.writeError("invalid_request", "target is required", req.RequestId)
	}
	target := store.UserKey{NodeID: req.Target.NodeId, UserID: req.Target.UserId}
	if err := s.http.authorizeCreateMessage(ctx, s.principal, target); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	sender, err := messageSenderFromPrincipal(s.principal)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	deliveryKind, err := clientDeliveryKindFromProto(req.DeliveryKind)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if deliveryKind == deliveryKindTransient {
		if req.SyncMode != internalproto.ClientMessageSyncMode_CLIENT_MESSAGE_SYNC_MODE_UNSPECIFIED {
			return s.writeStoreOrRequestError(req.RequestId, fmt.Errorf("%w: sync_mode is only allowed for persistent messages", store.ErrInvalidInput))
		}
		mode, err := store.NormalizeDeliveryMode(clientDeliveryModeString(req.DeliveryMode))
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		packet, err := s.http.service.DispatchTransientPacket(ctx, target, sender, req.Body, mode)
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		return s.writeEnvelope(&internalproto.ServerEnvelope{
			Body: &internalproto.ServerEnvelope_SendMessageResponse{
				SendMessageResponse: &internalproto.SendMessageResponse{
					RequestId: req.RequestId,
					Body: &internalproto.SendMessageResponse_TransientAccepted{
						TransientAccepted: clientProtoTransientAccepted(packet),
					},
				},
			},
		})
	}
	if req.DeliveryMode != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_UNSPECIFIED {
		return s.writeStoreOrRequestError(req.RequestId, fmt.Errorf("%w: delivery_mode is only allowed for transient messages", store.ErrInvalidInput))
	}
	syncMode, err := clientMessageSyncModeFromProto(req.SyncMode)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	message, _, err := s.http.service.CreateMessage(ctx, store.CreateMessageParams{
		UserKey:               target,
		Sender:                sender,
		Body:                  req.Body,
		PebbleMessageSyncMode: syncMode,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.markSeen(message.NodeID, message.Seq)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_SendMessageResponse{
			SendMessageResponse: &internalproto.SendMessageResponse{
				RequestId: req.RequestId,
				Body: &internalproto.SendMessageResponse_Message{
					Message: clientProtoMessage(message),
				},
			},
		},
	})
}

func (s *clientWSSession) handleCreateUser(ctx context.Context, req *internalproto.CreateUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "create_user cannot be empty", 0)
	}
	if err := s.requireAdminPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	profile, err := normalizeJSONValue(req.ProfileJson, "{}")
	if err != nil {
		return s.writeError("invalid_request", "profile must be valid JSON", req.RequestId)
	}
	passwordHash := ""
	if strings.TrimSpace(req.Role) != store.RoleChannel {
		passwordHash, err = hashPasswordFromWS(req.Password)
		if err != nil {
			return s.writeError("invalid_request", err.Error(), req.RequestId)
		}
	}
	user, _, err := s.http.service.CreateUser(ctx, store.CreateUserParams{
		Username:     req.Username,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateTargetRoleCache(user.Key())
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_CreateUserResponse{
			CreateUserResponse: &internalproto.CreateUserResponse{
				RequestId: req.RequestId,
				User:      clientProtoUser(user),
			},
		},
	})
}

func (s *clientWSSession) handleGetUser(ctx context.Context, req *internalproto.GetUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "get_user cannot be empty", 0)
	}
	key, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(key); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	user, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_GetUserResponse{
			GetUserResponse: &internalproto.GetUserResponse{
				RequestId: req.RequestId,
				User:      clientProtoUser(user),
			},
		},
	})
}

func (s *clientWSSession) handleUpdateUser(ctx context.Context, req *internalproto.UpdateUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "update_user cannot be empty", 0)
	}
	key, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireAdminPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}

	var profile *string
	if req.ProfileJson != nil {
		normalized, err := normalizeJSONValue(req.ProfileJson.Value, "{}")
		if err != nil {
			return s.writeError("invalid_request", "profile must be valid JSON", req.RequestId)
		}
		profile = &normalized
	}

	var passwordHash *string
	if req.Password != nil {
		hashed, err := hashPasswordFromWS(req.Password.Value)
		if err != nil {
			return s.writeError("invalid_request", err.Error(), req.RequestId)
		}
		passwordHash = &hashed
	}

	user, _, err := s.http.service.UpdateUser(ctx, store.UpdateUserParams{
		Key:          key,
		Username:     stringPtrValue(req.Username),
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         stringPtrValue(req.Role),
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateTargetRoleCache(user.Key())
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_UpdateUserResponse{
			UpdateUserResponse: &internalproto.UpdateUserResponse{
				RequestId: req.RequestId,
				User:      clientProtoUser(user),
			},
		},
	})
}

func (s *clientWSSession) handleDeleteUser(ctx context.Context, req *internalproto.DeleteUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "delete_user cannot be empty", 0)
	}
	key, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireAdminPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if _, err := s.http.service.DeleteUser(ctx, key); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateTargetRoleCache(key)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_DeleteUserResponse{
			DeleteUserResponse: &internalproto.DeleteUserResponse{
				RequestId: req.RequestId,
				Status:    "deleted",
				User:      &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
			},
		},
	})
}

func (s *clientWSSession) handleListMessages(ctx context.Context, req *internalproto.ListMessagesRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_messages cannot be empty", 0)
	}
	key, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.authorizeListMessages(ctx, key); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	limit := 100
	if req.Limit != 0 {
		limit = int(req.Limit)
	}
	messages, err := s.http.service.ListMessagesByUser(ctx, key, limit)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.Message, 0, len(messages))
	for _, message := range messages {
		items = append(items, clientProtoMessage(message))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListMessagesResponse{
			ListMessagesResponse: &internalproto.ListMessagesResponse{
				RequestId: req.RequestId,
				Items:     items,
				Count:     int32(len(items)),
			},
		},
	})
}

func (s *clientWSSession) handleSubscribeChannel(ctx context.Context, req *internalproto.SubscribeChannelRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "subscribe_channel cannot be empty", 0)
	}
	subscriber, err := userKeyFromProto(req.Subscriber)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	channel, err := userKeyFromProto(req.Channel)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(subscriber); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	subscription, _, err := s.http.service.SubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: subscriber,
		Channel:    channel,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateUserChannelSubscriptionCache(subscriber, channel)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_SubscribeChannelResponse{
			SubscribeChannelResponse: &internalproto.SubscribeChannelResponse{
				RequestId:    req.RequestId,
				Subscription: clientProtoSubscription(subscription),
			},
		},
	})
}

func (s *clientWSSession) handleUnsubscribeChannel(ctx context.Context, req *internalproto.UnsubscribeChannelRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "unsubscribe_channel cannot be empty", 0)
	}
	subscriber, err := userKeyFromProto(req.Subscriber)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	channel, err := userKeyFromProto(req.Channel)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(subscriber); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	subscription, _, err := s.http.service.UnsubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: subscriber,
		Channel:    channel,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateUserChannelSubscriptionCache(subscriber, channel)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_UnsubscribeChannelResponse{
			UnsubscribeChannelResponse: &internalproto.UnsubscribeChannelResponse{
				RequestId:    req.RequestId,
				Subscription: clientProtoSubscription(subscription),
			},
		},
	})
}

func (s *clientWSSession) handleListSubscriptions(ctx context.Context, req *internalproto.ListSubscriptionsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_subscriptions cannot be empty", 0)
	}
	subscriber, err := userKeyFromProto(req.Subscriber)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(subscriber); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	subscriptions, err := s.http.service.ListChannelSubscriptions(ctx, subscriber)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.Subscription, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		items = append(items, clientProtoSubscription(subscription))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListSubscriptionsResponse{
			ListSubscriptionsResponse: &internalproto.ListSubscriptionsResponse{
				RequestId: req.RequestId,
				Items:     items,
				Count:     int32(len(items)),
			},
		},
	})
}

func (s *clientWSSession) handleBlockUser(ctx context.Context, req *internalproto.BlockUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "block_user cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	blocked, err := userKeyFromProto(req.Blocked)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	entry, _, err := s.http.service.BlockUser(ctx, store.BlacklistParams{
		Owner:   owner,
		Blocked: blocked,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateUserBlacklistCache(owner, blocked)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_BlockUserResponse{
			BlockUserResponse: &internalproto.BlockUserResponse{
				RequestId: req.RequestId,
				Entry:     clientProtoBlacklistEntry(entry),
			},
		},
	})
}

func (s *clientWSSession) handleUnblockUser(ctx context.Context, req *internalproto.UnblockUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "unblock_user cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	blocked, err := userKeyFromProto(req.Blocked)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	entry, _, err := s.http.service.UnblockUser(ctx, store.BlacklistParams{
		Owner:   owner,
		Blocked: blocked,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateUserBlacklistCache(owner, blocked)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_UnblockUserResponse{
			UnblockUserResponse: &internalproto.UnblockUserResponse{
				RequestId: req.RequestId,
				Entry:     clientProtoBlacklistEntry(entry),
			},
		},
	})
}

func (s *clientWSSession) handleListBlockedUsers(ctx context.Context, req *internalproto.ListBlockedUsersRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_blocked_users cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.requireSelfOrAdminPrincipal(owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	entries, err := s.http.service.ListBlockedUsers(ctx, owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.BlacklistEntry, 0, len(entries))
	for _, entry := range entries {
		items = append(items, clientProtoBlacklistEntry(entry))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListBlockedUsersResponse{
			ListBlockedUsersResponse: &internalproto.ListBlockedUsersResponse{
				RequestId: req.RequestId,
				Items:     items,
				Count:     int32(len(items)),
			},
		},
	})
}

func (s *clientWSSession) handleListEvents(ctx context.Context, req *internalproto.ListEventsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_events cannot be empty", 0)
	}
	if err := s.requireAdminPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	limit := 100
	if req.Limit != 0 {
		limit = int(req.Limit)
	}
	events, err := s.http.service.ListEvents(ctx, req.After, limit)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.Event, 0, len(events))
	for _, event := range events {
		item, err := clientProtoEvent(event)
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		items = append(items, item)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListEventsResponse{
			ListEventsResponse: &internalproto.ListEventsResponse{
				RequestId: req.RequestId,
				Items:     items,
				Count:     int32(len(items)),
			},
		},
	})
}

func (s *clientWSSession) handleOperationsStatus(ctx context.Context, req *internalproto.OperationsStatusRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "operations_status cannot be empty", 0)
	}
	if err := s.requireAdminPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	status, err := s.http.service.OperationsStatus(ctx)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_OperationsStatusResponse{
			OperationsStatusResponse: &internalproto.OperationsStatusResponse{
				RequestId: req.RequestId,
				Status:    clientProtoOperationsStatus(status),
			},
		},
	})
}

func (s *clientWSSession) handleListClusterNodes(ctx context.Context, req *internalproto.ListClusterNodesRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_cluster_nodes cannot be empty", 0)
	}
	if err := s.requireAuthenticatedPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	nodes, err := s.http.service.ClusterNodes(ctx)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.ClusterNode, 0, len(nodes.Nodes))
	for _, node := range nodes.Nodes {
		items = append(items, clientProtoClusterNode(node))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListClusterNodesResponse{
			ListClusterNodesResponse: &internalproto.ListClusterNodesResponse{
				RequestId: req.RequestId,
				Items:     items,
				Count:     int32(len(items)),
			},
		},
	})
}

func (s *clientWSSession) handleListNodeLoggedInUsers(ctx context.Context, req *internalproto.ListNodeLoggedInUsersRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_node_logged_in_users cannot be empty", 0)
	}
	if err := s.requireAuthenticatedPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	users, err := s.http.service.ListNodeLoggedInUsers(ctx, req.NodeId)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.LoggedInUser, 0, len(users.Items))
	for _, user := range users.Items {
		items = append(items, clientProtoLoggedInUser(user))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListNodeLoggedInUsersResponse{
			ListNodeLoggedInUsersResponse: &internalproto.ListNodeLoggedInUsersResponse{
				RequestId:    req.RequestId,
				TargetNodeId: users.TargetNodeID,
				Items:        items,
				Count:        int32(len(items)),
			},
		},
	})
}

func (s *clientWSSession) handleMetrics(ctx context.Context, req *internalproto.MetricsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "metrics cannot be empty", 0)
	}
	if err := s.requireAdminPrincipal(); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	text, err := s.http.service.Metrics(ctx)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_MetricsResponse{
			MetricsResponse: &internalproto.MetricsResponse{
				RequestId: req.RequestId,
				Text:      text,
			},
		},
	})
}

func (s *clientWSSession) requireAdminPrincipal() error {
	if s.principal == nil || !isAdminRole(s.principal.User.Role) {
		return store.ErrForbidden
	}
	return nil
}

func (s *clientWSSession) requireAuthenticatedPrincipal() error {
	if s.principal == nil {
		return store.ErrForbidden
	}
	return nil
}

func (s *clientWSSession) requireSelfOrAdminPrincipal(key store.UserKey) error {
	if s.principal == nil {
		return store.ErrForbidden
	}
	if isAdminRole(s.principal.User.Role) || s.principal.User.Key() == key {
		return nil
	}
	return store.ErrForbidden
}

func (s *clientWSSession) authorizeListMessages(ctx context.Context, key store.UserKey) error {
	target, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		return err
	}
	if target.CanLogin() {
		return s.requireSelfOrAdminPrincipal(key)
	}
	return s.requireAdminPrincipal()
}

func userKeyFromProto(ref *internalproto.UserRef) (store.UserKey, error) {
	if ref == nil {
		return store.UserKey{}, fmt.Errorf("%w: user is required", store.ErrInvalidInput)
	}
	key := store.UserKey{NodeID: ref.NodeId, UserID: ref.UserId}
	if err := key.Validate(); err != nil {
		return store.UserKey{}, err
	}
	return key, nil
}

func stringPtrValue(field *internalproto.StringField) *string {
	if field == nil {
		return nil
	}
	value := field.Value
	return &value
}

func hashPasswordFromWS(password string) (string, error) {
	hashed, err := auth.HashPassword(password)
	if err != nil {
		return "", err
	}
	return hashed, nil
}

func clientProtoSubscription(subscription store.Subscription) *internalproto.Subscription {
	item := &internalproto.Subscription{
		Subscriber:   &internalproto.UserRef{NodeId: subscription.Subscriber.NodeID, UserId: subscription.Subscriber.UserID},
		Channel:      &internalproto.UserRef{NodeId: subscription.Channel.NodeID, UserId: subscription.Channel.UserID},
		SubscribedAt: subscription.SubscribedAt.String(),
		OriginNodeId: subscription.OriginNodeID,
	}
	if subscription.DeletedAt != nil {
		item.DeletedAt = subscription.DeletedAt.String()
	}
	return item
}

func clientProtoBlacklistEntry(entry store.BlacklistEntry) *internalproto.BlacklistEntry {
	item := &internalproto.BlacklistEntry{
		Owner:        &internalproto.UserRef{NodeId: entry.Owner.NodeID, UserId: entry.Owner.UserID},
		Blocked:      &internalproto.UserRef{NodeId: entry.Blocked.NodeID, UserId: entry.Blocked.UserID},
		BlockedAt:    entry.BlockedAt.String(),
		OriginNodeId: entry.OriginNodeID,
	}
	if entry.DeletedAt != nil {
		item.DeletedAt = entry.DeletedAt.String()
	}
	return item
}

func clientProtoEvent(event store.Event) (*internalproto.Event, error) {
	eventJSON, err := json.Marshal(event.Body)
	if err != nil {
		return nil, err
	}
	return &internalproto.Event{
		Sequence:        event.Sequence,
		EventId:         event.EventID,
		EventType:       string(event.EventType),
		Aggregate:       event.Aggregate,
		AggregateNodeId: event.AggregateNodeID,
		AggregateId:     event.AggregateID,
		Hlc:             event.HLC.String(),
		OriginNodeId:    event.OriginNodeID,
		EventJson:       eventJSON,
	}, nil
}

func clientProtoOperationsStatus(status operationsStatus) *internalproto.OperationsStatus {
	peers := make([]*internalproto.PeerStatus, 0, len(status.Peers))
	for _, peer := range status.Peers {
		origins := make([]*internalproto.PeerOriginStatus, 0, len(peer.Origins))
		for _, origin := range peer.Origins {
			origins = append(origins, &internalproto.PeerOriginStatus{
				OriginNodeId:      origin.OriginNodeID,
				AckedEventId:      origin.AckedEventID,
				AppliedEventId:    origin.AppliedEventID,
				UnconfirmedEvents: origin.UnconfirmedEvents,
				CursorUpdatedAt:   origin.CursorUpdatedAt,
				RemoteLastEventId: origin.RemoteLastEventID,
				PendingCatchup:    origin.PendingCatchup,
			})
		}
		peers = append(peers, &internalproto.PeerStatus{
			NodeId:                       peer.NodeID,
			ConfiguredUrl:                peer.ConfiguredURL,
			Connected:                    peer.Connected,
			SessionDirection:             peer.SessionDirection,
			Origins:                      origins,
			PendingSnapshotPartitions:    int32(peer.PendingSnapshotPartitions),
			RemoteSnapshotVersion:        peer.RemoteSnapshotVersion,
			RemoteMessageWindowSize:      int32(peer.RemoteMessageWindowSize),
			ClockOffsetMs:                peer.ClockOffsetMs,
			LastClockSync:                peer.LastClockSync,
			SnapshotDigestsSentTotal:     peer.SnapshotDigestsSentTotal,
			SnapshotDigestsReceivedTotal: peer.SnapshotDigestsRecvTotal,
			SnapshotChunksSentTotal:      peer.SnapshotChunksSentTotal,
			SnapshotChunksReceivedTotal:  peer.SnapshotChunksRecvTotal,
			LastSnapshotDigestAt:         peer.LastSnapshotDigestAt,
			LastSnapshotChunkAt:          peer.LastSnapshotChunkAt,
			Source:                       peer.Source,
			DiscoveredUrl:                peer.DiscoveredURL,
			DiscoveryState:               peer.DiscoveryState,
			LastDiscoveredAt:             peer.LastDiscoveredAt,
			LastConnectedAt:              peer.LastConnectedAt,
			LastDiscoveryError:           peer.LastDiscoveryError,
		})
	}
	return &internalproto.OperationsStatus{
		NodeId:            status.NodeID,
		MessageWindowSize: int32(status.MessageWindowSize),
		LastEventSequence: status.LastEventSequence,
		WriteGateReady:    status.WriteGateReady,
		ConflictTotal:     status.ConflictTotal,
		MessageTrim: &internalproto.MessageTrimStatus{
			TrimmedTotal:  status.MessageTrim.TrimmedTotal,
			LastTrimmedAt: status.MessageTrim.LastTrimmedAt,
		},
		EventLogTrim: &internalproto.EventLogTrimStatus{
			TrimmedTotal:  status.EventLogTrim.TrimmedTotal,
			LastTrimmedAt: status.EventLogTrim.LastTrimmedAt,
		},
		Projection: &internalproto.ProjectionStatus{
			PendingTotal: status.Projection.PendingTotal,
			LastFailedAt: status.Projection.LastFailedAt,
		},
		Peers: peers,
	}
}

func clientProtoClusterNode(node clusterNodeResponse) *internalproto.ClusterNode {
	return &internalproto.ClusterNode{
		NodeId:        node.NodeID,
		IsLocal:       node.IsLocal,
		ConfiguredUrl: node.ConfiguredURL,
		Source:        node.Source,
	}
}

func clientProtoLoggedInUser(user loggedInUserResponse) *internalproto.LoggedInUser {
	return &internalproto.LoggedInUser{
		NodeId:   user.NodeID,
		UserId:   user.UserID,
		Username: user.Username,
	}
}

func (s *clientWSSession) writeStoreOrRequestError(requestID uint64, err error) error {
	code := "internal_error"
	message := "internal server error"
	switch {
	case errors.Is(err, app.ErrClockNotSynchronized):
		code = "service_unavailable"
		message = app.ErrClockNotSynchronized.Error()
	case errors.Is(err, app.ErrServiceUnavailable):
		code = "service_unavailable"
		message = err.Error()
	case errors.Is(err, store.ErrBlockedByBlacklist):
		code = "forbidden"
		message = "forbidden"
	case errors.Is(err, store.ErrForbidden):
		code = "forbidden"
		message = "forbidden"
	case errors.Is(err, store.ErrInvalidInput):
		code = "invalid_request"
		message = err.Error()
	case errors.Is(err, store.ErrNotFound):
		code = "not_found"
		message = "resource not found"
	case errors.Is(err, store.ErrConflict):
		code = "conflict"
		message = "resource conflict"
	}
	return s.writeError(code, message, requestID)
}
