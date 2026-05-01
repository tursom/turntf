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

// handleSendMessage 处理客户端发送消息请求，同时支持持久化消息和即时消息（transient）。
// 即时消息通过 TransientPacketRouter 跨节点投递；持久化消息写入 store 并发布事件。
func (s *clientWSSession) handleSendMessage(ctx context.Context, req *internalproto.SendMessageRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "send_message cannot be empty", 0)
	}
	if req.Target == nil || req.Target.NodeId <= 0 || req.Target.UserId <= 0 {
		return s.writeError("invalid_request", "target is required", req.RequestId)
	}
	target := store.UserKey{NodeID: req.Target.NodeId, UserID: req.Target.UserId}
	if err := s.http.authorizer.CreateMessage(ctx, actorFromPrincipal(s.principal), target); err != nil {
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
		targetSession, err := sessionRefFromProto(req.TargetSession)
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		mode, err := store.NormalizeDeliveryMode(clientDeliveryModeString(req.DeliveryMode))
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		packet, err := s.http.service.DispatchTransientPacketTo(ctx, target, sender, req.Body, mode, targetSession)
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
	if req.TargetSession != nil {
		return s.writeStoreOrRequestError(req.RequestId, fmt.Errorf("%w: target_session is only allowed for transient messages", store.ErrInvalidInput))
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

// handleCreateUser 处理创建用户请求。频道用户（RoleChannel）不需要密码，且创建者会自动成为频道管理员。
func (s *clientWSSession) handleCreateUser(ctx context.Context, req *internalproto.CreateUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "create_user cannot be empty", 0)
	}
	if err := s.http.authorizer.CreateUser(actorFromPrincipal(s.principal), req.Role); err != nil {
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
		LoginName:    req.LoginName,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	}, creator)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateTargetRoleCache(user.Key())
	protoUser, err := s.clientProtoUserForResponse(ctx, user)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_CreateUserResponse{
			CreateUserResponse: &internalproto.CreateUserResponse{
				RequestId: req.RequestId,
				User:      protoUser,
			},
		},
	})
}

// handleGetUser 处理查询用户请求。
func (s *clientWSSession) handleGetUser(ctx context.Context, req *internalproto.GetUserRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "get_user cannot be empty", 0)
	}
	key, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.ViewUser(actorFromPrincipal(s.principal), key); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	user, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	protoUser, err := s.clientProtoUserForResponse(ctx, user)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_GetUserResponse{
			GetUserResponse: &internalproto.GetUserResponse{
				RequestId: req.RequestId,
				User:      protoUser,
			},
		},
	})
}

// handleUpdateUser 处理更新用户请求。仅传递非 nil 字段。部分字段的修改需要额外权限验证。
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
	if err := s.http.authorizer.UpdateUser(ctx, actorFromPrincipal(s.principal), target, stringPtrValue(req.Role), req.Password != nil, req.LoginName != nil); err != nil {
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
		LoginName:    stringPtrValue(req.LoginName),
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         stringPtrValue(req.Role),
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	s.http.invalidateTargetRoleCache(user.Key())
	protoUser, err := s.clientProtoUserForResponse(ctx, user)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_UpdateUserResponse{
			UpdateUserResponse: &internalproto.UpdateUserResponse{
				RequestId: req.RequestId,
				User:      protoUser,
			},
		},
	})
}

// handleDeleteUser 处理删除用户请求。
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
	if err := s.http.authorizer.DeleteUser(ctx, actorFromPrincipal(s.principal), target); err != nil {
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

// handleListMessages 处理查询用户消息列表请求，默认返回最近 100 条。
func (s *clientWSSession) handleListMessages(ctx context.Context, req *internalproto.ListMessagesRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_messages cannot be empty", 0)
	}
	key, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	target, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.ListMessages(actorFromPrincipal(s.principal), target); err != nil {
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

// handleGetUserMetadata 处理获取用户元数据请求。
func (s *clientWSSession) handleGetUserMetadata(ctx context.Context, req *internalproto.GetUserMetadataRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "get_user_metadata cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.ReadUserMetadata(actorFromPrincipal(s.principal), owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	metadata, err := s.http.service.GetUserMetadata(ctx, owner, req.Key)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_GetUserMetadataResponse{
			GetUserMetadataResponse: &internalproto.GetUserMetadataResponse{
				RequestId: req.RequestId,
				Metadata:  clientProtoUserMetadata(metadata),
			},
		},
	})
}

// handleUpsertUserMetadata 处理创建/更新用户元数据请求。
func (s *clientWSSession) handleUpsertUserMetadata(ctx context.Context, req *internalproto.UpsertUserMetadataRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "upsert_user_metadata cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.WriteUserMetadata(actorFromPrincipal(s.principal), owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	expiresAt, err := parseOptionalMetadataExpiresAt(stringPtrValue(req.ExpiresAt))
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	metadata, _, err := s.http.service.UpsertUserMetadata(ctx, store.UpsertUserMetadataParams{
		Owner:     owner,
		Key:       req.Key,
		Value:     req.Value,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_UpsertUserMetadataResponse{
			UpsertUserMetadataResponse: &internalproto.UpsertUserMetadataResponse{
				RequestId: req.RequestId,
				Metadata:  clientProtoUserMetadata(metadata),
			},
		},
	})
}

// handleDeleteUserMetadata 处理删除用户元数据请求（软删除）。
func (s *clientWSSession) handleDeleteUserMetadata(ctx context.Context, req *internalproto.DeleteUserMetadataRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "delete_user_metadata cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.WriteUserMetadata(actorFromPrincipal(s.principal), owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	metadata, _, err := s.http.service.DeleteUserMetadata(ctx, store.DeleteUserMetadataParams{
		Owner: owner,
		Key:   req.Key,
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_DeleteUserMetadataResponse{
			DeleteUserMetadataResponse: &internalproto.DeleteUserMetadataResponse{
				RequestId: req.RequestId,
				Metadata:  clientProtoUserMetadata(metadata),
			},
		},
	})
}

// handleScanUserMetadata 处理按前缀分页扫描用户元数据请求。
func (s *clientWSSession) handleScanUserMetadata(ctx context.Context, req *internalproto.ScanUserMetadataRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "scan_user_metadata cannot be empty", 0)
	}
	owner, err := userKeyFromProto(req.Owner)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.ReadUserMetadata(actorFromPrincipal(s.principal), owner); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	result, err := s.http.service.ScanUserMetadata(ctx, store.ScanUserMetadataParams{
		Owner:  owner,
		Prefix: req.Prefix,
		After:  req.After,
		Limit:  int(req.Limit),
	})
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	items := make([]*internalproto.UserMetadata, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, clientProtoUserMetadata(item))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ScanUserMetadataResponse{
			ScanUserMetadataResponse: &internalproto.ScanUserMetadataResponse{
				RequestId: req.RequestId,
				Items:     items,
				Count:     int32(len(items)),
				NextAfter: result.NextAfter,
			},
		},
	})
}

// handleUpsertUserAttachment 处理创建/更新附件请求（频道管理、黑名单、订阅等关联关系）。
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
	if err := s.http.authorizer.ManageAttachment(ctx, actorFromPrincipal(s.principal), owner, attachmentType); err != nil {
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

// handleDeleteUserAttachment 处理删除附件请求。
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
	if err := s.http.authorizer.ManageAttachment(ctx, actorFromPrincipal(s.principal), owner, attachmentType); err != nil {
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

// handleListUserAttachments 处理查询用户附件列表请求。
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
	if err := s.http.authorizer.ListAttachment(ctx, actorFromPrincipal(s.principal), owner, attachmentType); err != nil {
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

// handleListEvents 处理查询事件日志请求，从指定序列号之后开始拉取。
func (s *clientWSSession) handleListEvents(ctx context.Context, req *internalproto.ListEventsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_events cannot be empty", 0)
	}
	if err := s.http.authorizer.ListEvents(actorFromPrincipal(s.principal)); err != nil {
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

// handleOperationsStatus 处理查询集群运维状态请求（时钟、mesh、存储修剪等综合状态）。
func (s *clientWSSession) handleOperationsStatus(ctx context.Context, req *internalproto.OperationsStatusRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "operations_status cannot be empty", 0)
	}
	if err := s.http.authorizer.ReadOpsStatus(actorFromPrincipal(s.principal)); err != nil {
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

// handleListClusterNodes 处理查询集群节点列表请求。
func (s *clientWSSession) handleListClusterNodes(ctx context.Context, req *internalproto.ListClusterNodesRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_cluster_nodes cannot be empty", 0)
	}
	if err := s.http.authorizer.ListClusterNodes(actorFromPrincipal(s.principal)); err != nil {
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

// handleListNodeLoggedInUsers 处理查询指定节点已登录用户列表请求。
func (s *clientWSSession) handleListNodeLoggedInUsers(ctx context.Context, req *internalproto.ListNodeLoggedInUsersRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "list_node_logged_in_users cannot be empty", 0)
	}
	if err := s.http.authorizer.ListLoggedInUsers(actorFromPrincipal(s.principal)); err != nil {
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

// handleResolveUserSessions 处理查询用户在线会话请求。返回在线节点存在性信息和详细会话列表。
// 如果目标用户在本节点但远程查询未返回会话，则回退到本地会话列表。
func (s *clientWSSession) handleResolveUserSessions(ctx context.Context, req *internalproto.ResolveUserSessionsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "resolve_user_sessions cannot be empty", 0)
	}
	user, err := userKeyFromProto(req.User)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if err := s.http.authorizer.CreateMessage(ctx, actorFromPrincipal(s.principal), user); err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	presence, err := s.http.service.QueryOnlineUserPresence(ctx, user)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	sessions, err := s.http.service.ResolveUserSessions(ctx, user)
	if err != nil {
		return s.writeStoreOrRequestError(req.RequestId, err)
	}
	if len(sessions) == 0 && user.NodeID == s.http.nodeID {
		sessions, err = s.http.ListLocalUserSessions(ctx, user)
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		if len(presence) == 0 && len(sessions) > 0 {
			presence = []store.OnlineNodePresence{{
				User:          user,
				ServingNodeID: s.http.nodeID,
				SessionCount:  int32(len(sessions)),
				TransportHint: localSessionTransportHint(sessions),
			}}
		}
	}
	presenceItems := make([]*internalproto.OnlineNodePresence, 0, len(presence))
	for _, item := range presence {
		presenceItems = append(presenceItems, clientProtoOnlineNodePresence(item))
	}
	sessionItems := make([]*internalproto.ResolvedSession, 0, len(sessions))
	for _, item := range sessions {
		sessionItems = append(sessionItems, clientProtoResolvedSession(item))
	}
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_ResolveUserSessionsResponse{
			ResolveUserSessionsResponse: &internalproto.ResolveUserSessionsResponse{
				RequestId: req.RequestId,
				User:      &internalproto.UserRef{NodeId: user.NodeID, UserId: user.UserID},
				Presence:  presenceItems,
				Items:     sessionItems,
				Count:     int32(len(sessionItems)),
			},
		},
	})
}

// localSessionTransportHint 返回本地会话的传输方式提示：如果所有会话同一种传输方式则返回该方式名，否则返回 "mixed"。
func localSessionTransportHint(items []store.OnlineSession) string {
	if len(items) == 0 {
		return ""
	}
	hint := items[0].Transport
	for _, item := range items[1:] {
		if item.Transport != hint {
			return "mixed"
		}
	}
	return hint
}

// handleMetrics 处理查询 Prometheus 格式指标请求。
func (s *clientWSSession) handleMetrics(ctx context.Context, req *internalproto.MetricsRequest) error {
	if req == nil {
		return s.writeError("invalid_request", "metrics cannot be empty", 0)
	}
	if err := s.http.authorizer.ReadMetrics(actorFromPrincipal(s.principal)); err != nil {
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

// userKeyFromProto 将 protobuf UserRef 转换为 store.UserKey。
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

// stringPtrValue 将 protobuf 的可选字符串字段转为 *string，nil 字段返回 nil。
func stringPtrValue(field *internalproto.StringField) *string {
	if field == nil {
		return nil
	}
	value := field.Value
	return &value
}

// hashPasswordFromWS 对 WebSocket 客户端提供的明文密码进行哈希处理。
func hashPasswordFromWS(password string) (string, error) {
	hashed, err := auth.HashPassword(password)
	if err != nil {
		return "", err
	}
	return hashed, nil
}

// clientProtoAttachment 将 store.Attachment 转换为 protobuf Attachment 消息。
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

// clientProtoEvent 将 store.Event 转换为 protobuf Event 消息。事件体被序列化为 JSON 字节。
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

// clientProtoOperationsStatus 将 operationsStatus 转换为 protobuf OperationsStatus 消息。
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

// clientProtoUserForResponse 查询用户登录名后构造 protobuf User 响应，使返回的用户信息包含 login_name。
func (s *clientWSSession) clientProtoUserForResponse(ctx context.Context, user store.User) (*internalproto.User, error) {
	if s == nil || s.http == nil || s.http.service == nil {
		return clientProtoUser(user), nil
	}
	loginName, err := s.http.service.GetUserLoginName(ctx, user.Key())
	if err != nil {
		return nil, err
	}
	return clientProtoUserWithLoginName(user, loginName), nil
}

// clientProtoClusterNode 将 clusterNodeResponse 转换为 protobuf ClusterNode 消息。
func clientProtoClusterNode(node clusterNodeResponse) *internalproto.ClusterNode {
	return &internalproto.ClusterNode{
		NodeId:        node.NodeID,
		IsLocal:       node.IsLocal,
		ConfiguredUrl: node.ConfiguredURL,
		Source:        node.Source,
	}
}

// clientProtoLoggedInUser 将 loggedInUserResponse 转换为 protobuf LoggedInUser 消息。
func clientProtoLoggedInUser(user loggedInUserResponse) *internalproto.LoggedInUser {
	return &internalproto.LoggedInUser{
		NodeId:    user.NodeID,
		UserId:    user.UserID,
		Username:  user.Username,
		LoginName: user.LoginName,
	}
}

// writeStoreOrRequestError 将 store 或 app 层错误映射为客户端可理解的错误码和消息：
//   - ErrClockNotSynchronized / ErrServiceUnavailable → "service_unavailable"
//   - ErrBlockedByBlacklist / ErrForbidden → "forbidden"
//   - ErrInvalidInput → "invalid_request"
//   - ErrNotFound → "not_found"
//   - ErrConflict → "conflict"
//   - 其他 → "internal_error"
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
