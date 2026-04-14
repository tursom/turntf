package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	clientWSWriteWait     = 10 * time.Second
	clientWSReadTimeout   = 45 * time.Second
	clientWSPollInterval  = time.Second
	clientWSPollBatchSize = 100
)

var clientWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

type clientMessageCursor struct {
	nodeID int64
	seq    int64
}

type clientWSSession struct {
	http       *HTTP
	conn       *websocket.Conn
	remoteAddr string
	principal  *requestPrincipal
	seen       map[clientMessageCursor]struct{}
	seenMu     sync.Mutex
	writeMu    sync.Mutex
}

func (h *HTTP) handleClientWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := clientWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Warn().
			Err(err).
			Str("component", "api").
			Str("protocol", "ws").
			Str("path", r.URL.Path).
			Str("remote_addr", r.RemoteAddr).
			Str("event", "client_ws_upgrade_failed").
			Msg("client websocket upgrade failed")
		return
	}
	sess := &clientWSSession{
		http:       h,
		conn:       conn,
		remoteAddr: r.RemoteAddr,
		seen:       make(map[clientMessageCursor]struct{}),
	}
	defer conn.Close()
	log.Info().
		Str("component", "api").
		Str("protocol", "ws").
		Str("path", r.URL.Path).
		Str("remote_addr", r.RemoteAddr).
		Str("event", "client_ws_connected").
		Msg("client websocket connected")

	if err := sess.login(r.Context()); err != nil {
		sess.logWarn("client_ws_login_failed", err).
			Msg("client websocket login failed")
		_ = sess.writeEnvelope(&internalproto.ServerEnvelope{
			Body: &internalproto.ServerEnvelope_Error{
				Error: clientWSError("unauthorized", err.Error(), 0),
			},
		})
		return
	}
	defer h.unregisterClientSession(sess.principal.User.Key(), sess)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- sess.readLoop(ctx) }()
	go func() { errCh <- sess.pushLoop(ctx) }()

	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		sess.logWarn("client_ws_closed_with_error", err).
			Msg("client websocket closed with error")
		return
	}
	sess.logInfo("client_ws_closed").
		Msg("client websocket closed")
}

func (s *clientWSSession) login(ctx context.Context) error {
	messageType, data, err := s.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read login: %w", err)
	}
	if messageType != websocket.BinaryMessage {
		return fmt.Errorf("first message must be protobuf binary login")
	}
	var envelope internalproto.ClientEnvelope
	if err := gproto.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode login: %w", err)
	}
	login := envelope.GetLogin()
	if login == nil {
		return fmt.Errorf("first message must be login")
	}
	if login.User == nil {
		return fmt.Errorf("login user cannot be empty")
	}
	key := store.UserKey{NodeID: login.User.NodeId, UserID: login.User.UserId}
	user, err := s.http.service.AuthenticateUser(ctx, key, login.Password)
	if err != nil {
		return fmt.Errorf("invalid credentials")
	}
	for _, cursor := range login.SeenMessages {
		if cursor == nil || cursor.NodeId <= 0 || cursor.Seq <= 0 {
			continue
		}
		s.markSeen(cursor.NodeId, cursor.Seq)
	}
	s.principal = &requestPrincipal{User: user}
	s.http.registerClientSession(s.principal.User.Key(), s)
	s.logInfo("client_ws_authenticated").
		Msg("client websocket authenticated")
	if err := s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_LoginResponse{
			LoginResponse: &internalproto.LoginResponse{
				User:            clientProtoUser(user),
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	}); err != nil {
		s.http.unregisterClientSession(s.principal.User.Key(), s)
		return err
	}
	if err := s.pushInitialMessages(ctx); err != nil {
		s.http.unregisterClientSession(s.principal.User.Key(), s)
		return err
	}
	return nil
}

func (s *clientWSSession) readLoop(ctx context.Context) error {
	s.conn.SetReadLimit(1 << 20)
	_ = s.conn.SetReadDeadline(time.Now().Add(clientWSReadTimeout))
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(clientWSReadTimeout))
	})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		messageType, data, err := s.conn.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.BinaryMessage {
			s.logWarn("client_ws_invalid_frame", errors.New("non-binary websocket frame")).
				Int("message_type", messageType).
				Msg("client websocket received invalid frame")
			if err := s.writeError("invalid_frame", "client websocket only accepts protobuf binary frames", 0); err != nil {
				return err
			}
			continue
		}
		var envelope internalproto.ClientEnvelope
		if err := gproto.Unmarshal(data, &envelope); err != nil {
			s.logWarn("client_ws_invalid_protobuf", err).
				Msg("client websocket received invalid protobuf")
			if writeErr := s.writeError("invalid_protobuf", "invalid protobuf frame", 0); writeErr != nil {
				return writeErr
			}
			continue
		}
		switch body := envelope.Body.(type) {
		case *internalproto.ClientEnvelope_SendMessage:
			s.logRequest("send_message", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.SendMessage.GetTarget().GetNodeId()).
				Int64("target_user_id", body.SendMessage.GetTarget().GetUserId()).
				Str("delivery_kind", body.SendMessage.GetDeliveryKind().String()).
				Msg("client websocket request")
			if err := s.handleSendMessage(ctx, body.SendMessage); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_CreateUser:
			s.logRequest("create_user", requestIDForClientEnvelopeBody(body)).
				Str("role", body.CreateUser.GetRole()).
				Str("username", body.CreateUser.GetUsername()).
				Msg("client websocket request")
			if err := s.handleCreateUser(ctx, body.CreateUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_GetUser:
			s.logRequest("get_user", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.GetUser.GetUser().GetNodeId()).
				Int64("target_user_id", body.GetUser.GetUser().GetUserId()).
				Msg("client websocket request")
			if err := s.handleGetUser(ctx, body.GetUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_UpdateUser:
			s.logRequest("update_user", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.UpdateUser.GetUser().GetNodeId()).
				Int64("target_user_id", body.UpdateUser.GetUser().GetUserId()).
				Msg("client websocket request")
			if err := s.handleUpdateUser(ctx, body.UpdateUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_DeleteUser:
			s.logRequest("delete_user", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.DeleteUser.GetUser().GetNodeId()).
				Int64("target_user_id", body.DeleteUser.GetUser().GetUserId()).
				Msg("client websocket request")
			if err := s.handleDeleteUser(ctx, body.DeleteUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListMessages:
			s.logRequest("list_messages", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.ListMessages.GetUser().GetNodeId()).
				Int64("target_user_id", body.ListMessages.GetUser().GetUserId()).
				Int32("limit", body.ListMessages.GetLimit()).
				Msg("client websocket request")
			if err := s.handleListMessages(ctx, body.ListMessages); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_SubscribeChannel:
			s.logRequest("subscribe_channel", requestIDForClientEnvelopeBody(body)).
				Int64("subscriber_node_id", body.SubscribeChannel.GetSubscriber().GetNodeId()).
				Int64("subscriber_user_id", body.SubscribeChannel.GetSubscriber().GetUserId()).
				Int64("channel_node_id", body.SubscribeChannel.GetChannel().GetNodeId()).
				Int64("channel_user_id", body.SubscribeChannel.GetChannel().GetUserId()).
				Msg("client websocket request")
			if err := s.handleSubscribeChannel(ctx, body.SubscribeChannel); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_UnsubscribeChannel:
			s.logRequest("unsubscribe_channel", requestIDForClientEnvelopeBody(body)).
				Int64("subscriber_node_id", body.UnsubscribeChannel.GetSubscriber().GetNodeId()).
				Int64("subscriber_user_id", body.UnsubscribeChannel.GetSubscriber().GetUserId()).
				Int64("channel_node_id", body.UnsubscribeChannel.GetChannel().GetNodeId()).
				Int64("channel_user_id", body.UnsubscribeChannel.GetChannel().GetUserId()).
				Msg("client websocket request")
			if err := s.handleUnsubscribeChannel(ctx, body.UnsubscribeChannel); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListSubscriptions:
			s.logRequest("list_subscriptions", requestIDForClientEnvelopeBody(body)).
				Int64("subscriber_node_id", body.ListSubscriptions.GetSubscriber().GetNodeId()).
				Int64("subscriber_user_id", body.ListSubscriptions.GetSubscriber().GetUserId()).
				Msg("client websocket request")
			if err := s.handleListSubscriptions(ctx, body.ListSubscriptions); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_BlockUser:
			s.logRequest("block_user", requestIDForClientEnvelopeBody(body)).
				Int64("owner_node_id", body.BlockUser.GetOwner().GetNodeId()).
				Int64("owner_user_id", body.BlockUser.GetOwner().GetUserId()).
				Int64("blocked_node_id", body.BlockUser.GetBlocked().GetNodeId()).
				Int64("blocked_user_id", body.BlockUser.GetBlocked().GetUserId()).
				Msg("client websocket request")
			if err := s.handleBlockUser(ctx, body.BlockUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_UnblockUser:
			s.logRequest("unblock_user", requestIDForClientEnvelopeBody(body)).
				Int64("owner_node_id", body.UnblockUser.GetOwner().GetNodeId()).
				Int64("owner_user_id", body.UnblockUser.GetOwner().GetUserId()).
				Int64("blocked_node_id", body.UnblockUser.GetBlocked().GetNodeId()).
				Int64("blocked_user_id", body.UnblockUser.GetBlocked().GetUserId()).
				Msg("client websocket request")
			if err := s.handleUnblockUser(ctx, body.UnblockUser); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListBlockedUsers:
			s.logRequest("list_blocked_users", requestIDForClientEnvelopeBody(body)).
				Int64("owner_node_id", body.ListBlockedUsers.GetOwner().GetNodeId()).
				Int64("owner_user_id", body.ListBlockedUsers.GetOwner().GetUserId()).
				Msg("client websocket request")
			if err := s.handleListBlockedUsers(ctx, body.ListBlockedUsers); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListEvents:
			s.logRequest("list_events", requestIDForClientEnvelopeBody(body)).
				Int64("after", body.ListEvents.GetAfter()).
				Int32("limit", body.ListEvents.GetLimit()).
				Msg("client websocket request")
			if err := s.handleListEvents(ctx, body.ListEvents); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_OperationsStatus:
			s.logRequest("operations_status", requestIDForClientEnvelopeBody(body)).
				Msg("client websocket request")
			if err := s.handleOperationsStatus(ctx, body.OperationsStatus); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListClusterNodes:
			s.logRequest("list_cluster_nodes", requestIDForClientEnvelopeBody(body)).
				Msg("client websocket request")
			if err := s.handleListClusterNodes(ctx, body.ListClusterNodes); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_ListNodeLoggedInUsers:
			s.logRequest("list_node_logged_in_users", requestIDForClientEnvelopeBody(body)).
				Int64("target_node_id", body.ListNodeLoggedInUsers.GetNodeId()).
				Msg("client websocket request")
			if err := s.handleListNodeLoggedInUsers(ctx, body.ListNodeLoggedInUsers); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_Metrics:
			s.logRequest("metrics", requestIDForClientEnvelopeBody(body)).
				Msg("client websocket request")
			if err := s.handleMetrics(ctx, body.Metrics); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_AckMessage:
			if ack := body.AckMessage; ack != nil && ack.Cursor != nil {
				s.logDebug("ack_message", requestIDForClientEnvelopeBody(body)).
					Int64("cursor_node_id", ack.Cursor.NodeId).
					Int64("cursor_seq", ack.Cursor.Seq).
					Msg("client websocket acknowledgement")
				s.markSeen(ack.Cursor.NodeId, ack.Cursor.Seq)
			}
		case *internalproto.ClientEnvelope_Ping:
			requestID := uint64(0)
			if body.Ping != nil {
				requestID = body.Ping.RequestId
			}
			s.logDebug("ping", requestID).
				Msg("client websocket ping")
			if err := s.writeEnvelope(&internalproto.ServerEnvelope{
				Body: &internalproto.ServerEnvelope_Pong{Pong: &internalproto.Pong{RequestId: requestID}},
			}); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_Login:
			s.logWarn("client_ws_redundant_login", errors.New("login frame after authentication")).
				Uint64("request_id", requestIDForClientEnvelopeBody(body)).
				Msg("client websocket received login after authentication")
			if err := s.writeError("already_authenticated", "login is only allowed as the first frame", 0); err != nil {
				return err
			}
		default:
			s.logWarn("client_ws_invalid_message", errors.New("unsupported client message")).
				Msg("client websocket received unsupported client message")
			if err := s.writeError("invalid_message", "unsupported client message", 0); err != nil {
				return err
			}
		}
	}
}

func (s *clientWSSession) pushInitialMessages(ctx context.Context) error {
	messages, err := s.http.service.ListMessagesByUser(ctx, s.principal.User.Key(), 1000)
	if err != nil {
		return err
	}
	for i := len(messages) - 1; i >= 0; i-- {
		if err := s.pushMessage(messages[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *clientWSSession) pushLoop(ctx context.Context) error {
	afterSequence := int64(0)
	ticker := time.NewTicker(clientWSPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			events, err := s.http.service.ListEvents(ctx, afterSequence, clientWSPollBatchSize)
			if err != nil {
				if writeErr := s.writeError("internal_error", "failed to list events", 0); writeErr != nil {
					return writeErr
				}
				continue
			}
			for _, event := range events {
				if event.Sequence > afterSequence {
					afterSequence = event.Sequence
				}
				message, ok, err := messageFromClientPushEvent(event)
				if err != nil {
					if writeErr := s.writeError("internal_error", "failed to decode message event", 0); writeErr != nil {
						return writeErr
					}
					continue
				}
				if !ok {
					continue
				}
				visible, err := s.canSeeMessage(ctx, message)
				if err != nil {
					if writeErr := s.writeError("internal_error", "failed to authorize message", 0); writeErr != nil {
						return writeErr
					}
					continue
				}
				if !visible {
					continue
				}
				if err := s.pushMessage(message); err != nil {
					return err
				}
			}
		}
	}
}

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
	message, _, err := s.http.service.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: target,
		Sender:  sender,
		Body:    req.Body,
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

func (s *clientWSSession) canSeeMessage(ctx context.Context, message store.Message) (bool, error) {
	if isAdminRole(s.principal.User.Role) {
		return true, nil
	}
	key := message.UserKey()
	if key == s.principal.User.Key() {
		blocked, err := s.http.service.IsMessageBlockedByBlacklist(ctx, s.principal.User.Key(), message.Sender, message.CreatedAt)
		if err != nil {
			return false, err
		}
		return !blocked, nil
	}
	target, err := s.http.service.GetUser(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	switch target.Role {
	case store.RoleBroadcast:
		return true, nil
	case store.RoleChannel:
		return s.http.service.IsSubscribedToChannel(ctx, s.principal.User.Key(), key)
	default:
		return false, nil
	}
}

func (s *clientWSSession) pushMessage(message store.Message) error {
	cursor := clientMessageCursor{nodeID: message.NodeID, seq: message.Seq}
	s.seenMu.Lock()
	if _, ok := s.seen[cursor]; ok {
		s.seenMu.Unlock()
		return nil
	}
	s.seen[cursor] = struct{}{}
	s.seenMu.Unlock()
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_MessagePushed{
			MessagePushed: &internalproto.MessagePushed{Message: clientProtoMessage(message)},
		},
	})
}

func (s *clientWSSession) pushPacket(packet store.TransientPacket) error {
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_PacketPushed{
			PacketPushed: &internalproto.PacketPushed{Packet: clientProtoPacket(packet)},
		},
	})
}

func (s *clientWSSession) markSeen(nodeID, seq int64) {
	if nodeID <= 0 || seq <= 0 {
		return
	}
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	s.seen[clientMessageCursor{nodeID: nodeID, seq: seq}] = struct{}{}
}

func (s *clientWSSession) writeError(code, message string, requestID uint64) error {
	s.logWarn("client_ws_error_response", errors.New(message)).
		Str("code", code).
		Uint64("request_id", requestID).
		Msg("client websocket returning error")
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_Error{Error: clientWSError(code, message, requestID)},
	})
}

func (s *clientWSSession) logInfo(event string) *zerolog.Event {
	e := log.Info().
		Str("component", "api").
		Str("protocol", "ws").
		Str("event", event).
		Str("remote_addr", s.remoteAddr)
	if s.principal != nil {
		e = e.Int64("node_id", s.principal.User.NodeID).
			Int64("user_id", s.principal.User.ID).
			Str("role", s.principal.User.Role)
	}
	return e
}

func (s *clientWSSession) logWarn(event string, err error) *zerolog.Event {
	e := log.Warn().
		Str("component", "api").
		Str("protocol", "ws").
		Str("event", event).
		Str("remote_addr", s.remoteAddr)
	if err != nil {
		e = e.Err(err)
	}
	if s.principal != nil {
		e = e.Int64("node_id", s.principal.User.NodeID).
			Int64("user_id", s.principal.User.ID).
			Str("role", s.principal.User.Role)
	}
	return e
}

func (s *clientWSSession) logDebug(action string, requestID uint64) *zerolog.Event {
	e := log.Debug().
		Str("component", "api").
		Str("protocol", "ws").
		Str("event", "client_ws_request").
		Str("action", action).
		Str("remote_addr", s.remoteAddr)
	if requestID != 0 {
		e = e.Uint64("request_id", requestID)
	}
	if s.principal != nil {
		e = e.Int64("node_id", s.principal.User.NodeID).
			Int64("user_id", s.principal.User.ID).
			Str("role", s.principal.User.Role)
	}
	return e
}

func (s *clientWSSession) logRequest(action string, requestID uint64) *zerolog.Event {
	e := s.logInfo("client_ws_request").Str("action", action)
	if requestID != 0 {
		e = e.Uint64("request_id", requestID)
	}
	return e
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
	case *internalproto.ClientEnvelope_SubscribeChannel:
		return req.SubscribeChannel.GetRequestId()
	case *internalproto.ClientEnvelope_UnsubscribeChannel:
		return req.UnsubscribeChannel.GetRequestId()
	case *internalproto.ClientEnvelope_ListSubscriptions:
		return req.ListSubscriptions.GetRequestId()
	case *internalproto.ClientEnvelope_ListEvents:
		return req.ListEvents.GetRequestId()
	case *internalproto.ClientEnvelope_OperationsStatus:
		return req.OperationsStatus.GetRequestId()
	case *internalproto.ClientEnvelope_ListClusterNodes:
		return req.ListClusterNodes.GetRequestId()
	case *internalproto.ClientEnvelope_ListNodeLoggedInUsers:
		return req.ListNodeLoggedInUsers.GetRequestId()
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

func (s *clientWSSession) writeEnvelope(envelope *internalproto.ServerEnvelope) error {
	data, err := gproto.Marshal(envelope)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(clientWSWriteWait)); err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.BinaryMessage, data)
}

func clientWSError(code, message string, requestID uint64) *internalproto.Error {
	return &internalproto.Error{
		Code:      code,
		Message:   message,
		RequestId: requestID,
	}
}

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
