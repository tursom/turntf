package api

import (
	"context"
	"errors"

	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func (s *clientWSSession) readLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		data, err := s.conn.Receive(ctx)
		if err != nil {
			if errors.Is(err, errNonBinaryClientFrame) {
				s.logWarn("client_invalid_frame", err).
					Msg("client transport received invalid frame")
				if writeErr := s.writeError("invalid_frame", "client transport only accepts protobuf binary frames", 0); writeErr != nil {
					return writeErr
				}
				continue
			}
			return err
		}
		var envelope internalproto.ClientEnvelope
		if err := gproto.Unmarshal(data, &envelope); err != nil {
			s.logWarn("client_invalid_protobuf", err).
				Msg("client transport received invalid protobuf")
			if writeErr := s.writeError("invalid_protobuf", "invalid protobuf frame", 0); writeErr != nil {
				return writeErr
			}
			continue
		}
		switch body := envelope.Body.(type) {
		case *internalproto.ClientEnvelope_SendMessage:
			if s.realtimeOnly && body.SendMessage.GetDeliveryKind() != internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT {
				if err := s.writeError("invalid_request", "realtime stream only supports transient send_message", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("send_message", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.SendMessage.GetTarget().GetNodeId()).
				Int64("target_user_id", body.SendMessage.GetTarget().GetUserId()).
				Str("delivery_kind", body.SendMessage.GetDeliveryKind().String()).
				Msg("client transport request")
			if err := s.handleSendMessage(ctx, body.SendMessage); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_CreateUser:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support create_user", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("create_user", requestIDForClientEnvelopeBody(body)).
				Str("role", body.CreateUser.GetRole()).
				Str("username", body.CreateUser.GetUsername()).
				Msg("client transport request")
			if err := s.handleCreateUser(ctx, body.CreateUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_GetUser:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support get_user", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("get_user", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.GetUser.GetUser().GetNodeId()).
				Int64("target_user_id", body.GetUser.GetUser().GetUserId()).
				Msg("client transport request")
			if err := s.handleGetUser(ctx, body.GetUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_UpdateUser:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support update_user", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("update_user", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.UpdateUser.GetUser().GetNodeId()).
				Int64("target_user_id", body.UpdateUser.GetUser().GetUserId()).
				Msg("client transport request")
			if err := s.handleUpdateUser(ctx, body.UpdateUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_DeleteUser:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support delete_user", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("delete_user", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.DeleteUser.GetUser().GetNodeId()).
				Int64("target_user_id", body.DeleteUser.GetUser().GetUserId()).
				Msg("client transport request")
			if err := s.handleDeleteUser(ctx, body.DeleteUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListMessages:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support list_messages", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("list_messages", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.ListMessages.GetUser().GetNodeId()).
				Int64("target_user_id", body.ListMessages.GetUser().GetUserId()).
				Int32("limit", body.ListMessages.GetLimit()).
				Msg("client transport request")
			if err := s.handleListMessages(ctx, body.ListMessages); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_UpsertUserAttachment:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support upsert_user_attachment", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("upsert_user_attachment", requestIDForClientEnvelopeBody(body)).
				Int64("owner_node_id", body.UpsertUserAttachment.GetOwner().GetNodeId()).
				Int64("owner_user_id", body.UpsertUserAttachment.GetOwner().GetUserId()).
				Int64("subject_node_id", body.UpsertUserAttachment.GetSubject().GetNodeId()).
				Int64("subject_user_id", body.UpsertUserAttachment.GetSubject().GetUserId()).
				Str("attachment_type", body.UpsertUserAttachment.GetAttachmentType().String()).
				Msg("client transport request")
			if err := s.handleUpsertUserAttachment(ctx, body.UpsertUserAttachment); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_DeleteUserAttachment:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support delete_user_attachment", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("delete_user_attachment", requestIDForClientEnvelopeBody(body)).
				Int64("owner_node_id", body.DeleteUserAttachment.GetOwner().GetNodeId()).
				Int64("owner_user_id", body.DeleteUserAttachment.GetOwner().GetUserId()).
				Int64("subject_node_id", body.DeleteUserAttachment.GetSubject().GetNodeId()).
				Int64("subject_user_id", body.DeleteUserAttachment.GetSubject().GetUserId()).
				Str("attachment_type", body.DeleteUserAttachment.GetAttachmentType().String()).
				Msg("client transport request")
			if err := s.handleDeleteUserAttachment(ctx, body.DeleteUserAttachment); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListUserAttachments:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support list_user_attachments", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("list_user_attachments", requestIDForClientEnvelopeBody(body)).
				Int64("owner_node_id", body.ListUserAttachments.GetOwner().GetNodeId()).
				Int64("owner_user_id", body.ListUserAttachments.GetOwner().GetUserId()).
				Str("attachment_type", body.ListUserAttachments.GetAttachmentType().String()).
				Msg("client transport request")
			if err := s.handleListUserAttachments(ctx, body.ListUserAttachments); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListEvents:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support list_events", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("list_events", requestIDForClientEnvelopeBody(body)).
				Int64("after", body.ListEvents.GetAfter()).
				Int32("limit", body.ListEvents.GetLimit()).
				Msg("client transport request")
			if err := s.handleListEvents(ctx, body.ListEvents); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_OperationsStatus:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support operations_status", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("operations_status", requestIDForClientEnvelopeBody(body)).
				Msg("client transport request")
			if err := s.handleOperationsStatus(ctx, body.OperationsStatus); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListClusterNodes:
			s.logRequest("list_cluster_nodes", requestIDForClientEnvelopeBody(body)).
				Msg("client transport request")
			if err := s.handleListClusterNodes(ctx, body.ListClusterNodes); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListNodeLoggedInUsers:
			s.logRequest("list_node_logged_in_users", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.ListNodeLoggedInUsers.GetNodeId()).
				Msg("client transport request")
			if err := s.handleListNodeLoggedInUsers(ctx, body.ListNodeLoggedInUsers); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ResolveUserSessions:
			s.logRequest("resolve_user_sessions", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.ResolveUserSessions.GetUser().GetNodeId()).
				Int64("target_user_id", body.ResolveUserSessions.GetUser().GetUserId()).
				Msg("client transport request")
			if err := s.handleResolveUserSessions(ctx, body.ResolveUserSessions); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_Metrics:
			if s.realtimeOnly {
				if err := s.writeError("invalid_request", "realtime stream does not support metrics", requestIDForClientEnvelopeBody(body)); err != nil {
					return err
				}
				continue
			}
			s.logRequest("metrics", requestIDForClientEnvelopeBody(body)).
				Msg("client transport request")
			if err := s.handleMetrics(ctx, body.Metrics); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_AckMessage:
			if ack := body.AckMessage; ack != nil && ack.Cursor != nil {
				s.logDebug("ack_message", requestIDForClientEnvelopeBody(body)).
					Int64("cursor_node_id", ack.Cursor.NodeId).
					Int64("cursor_seq", ack.Cursor.Seq).
					Msg("client transport acknowledgement")
				s.markSeen(ack.Cursor.NodeId, ack.Cursor.Seq)
			}
		case *internalproto.ClientEnvelope_Ping:
			requestID := uint64(0)
			if body.Ping != nil {
				requestID = body.Ping.RequestId
			}
			s.logDebug("ping", requestID).
				Msg("client transport ping")
			if err := s.writeEnvelope(&internalproto.ServerEnvelope{
				Body: &internalproto.ServerEnvelope_Pong{Pong: &internalproto.Pong{RequestId: requestID}},
			}); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_Login:
			s.logWarn("client_redundant_login", errors.New("login frame after authentication")).
				Uint64("request_id", requestIDForClientEnvelopeBody(body)).
				Msg("client transport received login after authentication")
			if err := s.writeError("already_authenticated", "login is only allowed as the first frame", 0); err != nil {
				return err
			}
		default:
			s.logWarn("client_invalid_message", errors.New("unsupported client message")).
				Msg("client transport received unsupported client message")
			if err := s.writeError("invalid_message", "unsupported client message", 0); err != nil {
				return err
			}
		}
	}
}

func requestIDForClientEnvelopeBody(body any) uint64 {
	switch req := body.(type) {
	case *internalproto.ClientEnvelope_SendMessage:
		return req.SendMessage.GetRequestId()
	case *internalproto.ClientEnvelope_CreateUser:
		return req.CreateUser.GetRequestId()
	case *internalproto.ClientEnvelope_GetUser:
		return req.GetUser.GetRequestId()
	case *internalproto.ClientEnvelope_UpdateUser:
		return req.UpdateUser.GetRequestId()
	case *internalproto.ClientEnvelope_DeleteUser:
		return req.DeleteUser.GetRequestId()
	case *internalproto.ClientEnvelope_ListMessages:
		return req.ListMessages.GetRequestId()
	case *internalproto.ClientEnvelope_UpsertUserAttachment:
		return req.UpsertUserAttachment.GetRequestId()
	case *internalproto.ClientEnvelope_DeleteUserAttachment:
		return req.DeleteUserAttachment.GetRequestId()
	case *internalproto.ClientEnvelope_ListUserAttachments:
		return req.ListUserAttachments.GetRequestId()
	case *internalproto.ClientEnvelope_ListEvents:
		return req.ListEvents.GetRequestId()
	case *internalproto.ClientEnvelope_OperationsStatus:
		return req.OperationsStatus.GetRequestId()
	case *internalproto.ClientEnvelope_ListClusterNodes:
		return req.ListClusterNodes.GetRequestId()
	case *internalproto.ClientEnvelope_ListNodeLoggedInUsers:
		return req.ListNodeLoggedInUsers.GetRequestId()
	case *internalproto.ClientEnvelope_ResolveUserSessions:
		return req.ResolveUserSessions.GetRequestId()
	case *internalproto.ClientEnvelope_Metrics:
		return req.Metrics.GetRequestId()
	case *internalproto.ClientEnvelope_AckMessage:
		return 0
	case *internalproto.ClientEnvelope_Ping:
		return req.Ping.GetRequestId()
	case *internalproto.ClientEnvelope_Login:
		return 0
	default:
		return 0
	}
}
