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
	var creator *store.UserKey
	if s.principal != nil {
		key := s.principal.User.Key()
		creator = &key
	}
	user, _, err := s.http.service.CreateUserAs(ctx, store.CreateUserParams{
		Username:     req.Username,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	}, creator)
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
	target, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.authorizeManageUserPrincipal(ctx, target); err != nil {
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
	if !isAdminRole(s.principal.User.Role) && target.Role == store.RoleChannel {
		if passwordHash != nil || req.Role != nil {
			return s.writeStoreOrRequestError(req.RequestId, store.ErrForbidden)
		}
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
	target, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.authorizeManageUserPrincipal(ctx, target); err != nil {
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

func (s *clientWSSession) handleUpsertUserAttachment(ctx context.Context, req *internalproto.UpsertUserAttachmentRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "upsert_user_attachment cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	subject, err := userKeyFromProto(req.Subject)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	attachmentType, err := attachmentTypeFromProto(req.AttachmentType)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.authorizeAttachmentPrincipal(ctx, owner, attachmentType); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	attachment, _, err := s.http.service.UpsertAttachment(ctx, store.UpsertAttachmentParams{
		Owner:      owner,
		Subject:    subject,
		Type:       attachmentType,
		ConfigJSON: string(req.ConfigJson),
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateAttachmentCaches(attachment)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_UpsertUserAttachmentResponse{
			UpsertUserAttachmentResponse: &internalproto.UpsertUserAttachmentResponse{
				RequestId:  req.RequestId,
				Attachment: clientProtoAttachment(attachment),
			},
		},
	})
}

func (s *clientWSSession) handleDeleteUserAttachment(ctx context.Context, req *internalproto.DeleteUserAttachmentRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "delete_user_attachment cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	subject, err := userKeyFromProto(req.Subject)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	attachmentType, err := attachmentTypeFromProto(req.AttachmentType)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.authorizeAttachmentPrincipal(ctx, owner, attachmentType); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	attachment, _, err := s.http.service.DeleteAttachment(ctx, store.DeleteAttachmentParams{
		Owner:   owner,
		Subject: subject,
		Type:    attachmentType,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateAttachmentCaches(attachment)
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_DeleteUserAttachmentResponse{
			DeleteUserAttachmentResponse: &internalproto.DeleteUserAttachmentResponse{
				RequestId:  req.RequestId,
				Attachment: clientProtoAttachment(attachment),
			},
		},
	})
}

func (s *clientWSSession) handleListUserAttachments(ctx context.Context, req *internalproto.ListUserAttachmentsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_user_attachments cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	attachmentType, err := attachmentTypeFromProto(req.AttachmentType)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.authorizeAttachmentOwnerPrincipal(ctx, owner, attachmentType); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	attachments, err := s.http.service.ListUserAttachments(ctx, owner, attachmentType)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.Attachment, 0, len(attachments))
	for _, attachment := range attachments {
		items = append(items, clientProtoAttachment(attachment))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ListUserAttachmentsResponse{
			ListUserAttachmentsResponse: &internalproto.ListUserAttachmentsResponse{
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

func (s *clientWSSession) authorizeManageUserPrincipal(ctx context.Context, target store.User) error {
	if s.principal == nil {
		return store.ErrForbidden
	}
	if isAdminRole(s.principal.User.Role) {
		return nil
	}
	if target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	allowed, err := s.http.service.IsChannelManager(ctx, target.Key(), s.principal.User.Key())
	if err != nil {
		return err
	}
	if !allowed {
		return store.ErrForbidden
	}
	return nil
}

func (s *clientWSSession) authorizeAttachmentPrincipal(ctx context.Context, owner store.UserKey, attachmentType store.AttachmentType) error {
	if s.principal == nil {
		return store.ErrForbidden
	}
	if isAdminRole(s.principal.User.Role) {
		return nil
	}
	switch attachmentType {
	case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
		allowed, err := s.http.service.IsChannelManager(ctx, owner, s.principal.User.Key())
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
		return store.ErrForbidden
	case store.AttachmentTypeChannelSubscription, store.AttachmentTypeUserBlacklist:
		if s.principal.User.Key() == owner {
			return nil
		}
		return store.ErrForbidden
	default:
		return store.ErrInvalidInput
	}
}

func (s *clientWSSession) authorizeAttachmentOwnerPrincipal(ctx context.Context, owner store.UserKey, attachmentType store.AttachmentType) error {
	if s.principal == nil {
		return store.ErrForbidden
	}
	if isAdminRole(s.principal.User.Role) {
		return nil
	}
	if attachmentType != "" {
		return s.authorizeAttachmentPrincipal(ctx, owner, attachmentType)
	}
	ownerUser, err := s.http.service.GetUser(ctx, owner)
	if err != nil {
		return err
	}
	if ownerUser.Role == store.RoleChannel {
		allowed, err := s.http.service.IsChannelManager(ctx, owner, s.principal.User.Key())
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
		return store.ErrForbidden
	}
	if s.principal.User.Key() == owner {
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

func clientProtoAttachment(attachment store.Attachment) *internalproto.Attachment {
	item := &internalproto.Attachment{
		Owner:          &internalproto.UserRef{NodeId: attachment.Owner.NodeID, UserId: attachment.Owner.UserID},
		Subject:        &internalproto.UserRef{NodeId: attachment.Subject.NodeID, UserId: attachment.Subject.UserID},
		AttachmentType: attachmentTypeToProto(attachment.Type),
		ConfigJson:     []byte(attachment.ConfigJSON),
		AttachedAt:     attachment.AttachedAt.String(),
		OriginNodeId:   attachment.OriginNodeID,
	}
	if attachment.DeletedAt != nil {
		item.DeletedAt = attachment.DeletedAt.String()
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
