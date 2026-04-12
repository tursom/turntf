package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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
	http      *HTTP
	conn      *websocket.Conn
	principal *requestPrincipal
	seen      map[clientMessageCursor]struct{}
	seenMu    sync.Mutex
	writeMu   sync.Mutex
}

func (h *HTTP) handleClientWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := clientWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sess := &clientWSSession{
		http: h,
		conn: conn,
		seen: make(map[clientMessageCursor]struct{}),
	}
	defer conn.Close()

	if err := sess.login(r.Context()); err != nil {
		_ = sess.writeEnvelope(&internalproto.ServerEnvelope{
			Body: &internalproto.ServerEnvelope_Error{
				Error: clientWSError("unauthorized", err.Error(), 0),
			},
		})
		return
	}
	h.registerClientSession(sess.principal.User.Key(), sess)
	defer h.unregisterClientSession(sess.principal.User.Key(), sess)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- sess.readLoop(ctx) }()
	go func() { errCh <- sess.pushLoop(ctx) }()

	<-errCh
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
	key := store.UserKey{NodeID: login.NodeId, UserID: login.UserId}
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
	if err := s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_LoginResponse{
			LoginResponse: &internalproto.LoginResponse{
				User:            clientProtoUser(user),
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	}); err != nil {
		return err
	}
	return s.pushInitialMessages(ctx)
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
			if err := s.writeError("invalid_frame", "client websocket only accepts protobuf binary frames", 0); err != nil {
				return err
			}
			continue
		}
		var envelope internalproto.ClientEnvelope
		if err := gproto.Unmarshal(data, &envelope); err != nil {
			if writeErr := s.writeError("invalid_protobuf", "invalid protobuf frame", 0); writeErr != nil {
				return writeErr
			}
			continue
		}
		switch body := envelope.Body.(type) {
		case *internalproto.ClientEnvelope_SendMessage:
			if err := s.handleSendMessage(ctx, body.SendMessage); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_AckMessage:
			if ack := body.AckMessage; ack != nil && ack.Cursor != nil {
				s.markSeen(ack.Cursor.NodeId, ack.Cursor.Seq)
			}
		case *internalproto.ClientEnvelope_Ping:
			requestID := uint64(0)
			if body.Ping != nil {
				requestID = body.Ping.RequestId
			}
			if err := s.writeEnvelope(&internalproto.ServerEnvelope{
				Body: &internalproto.ServerEnvelope_Pong{Pong: &internalproto.Pong{RequestId: requestID}},
			}); err != nil {
				return err
			}
		case *internalproto.ClientEnvelope_Login:
			if err := s.writeError("already_authenticated", "login is only allowed as the first frame", 0); err != nil {
				return err
			}
		default:
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
	if target.UserID == store.NodeIngressUserID {
		if req.RelayTarget == nil || req.RelayTarget.NodeId <= 0 || req.RelayTarget.UserId <= 0 {
			return s.writeStoreOrRequestError(req.RequestId, fmt.Errorf("%w: relay_target is required", store.ErrInvalidInput))
		}
		mode, err := store.NormalizeDeliveryMode(clientDeliveryModeString(req.DeliveryMode))
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		packet, err := s.http.service.DispatchTransientPacket(ctx, target, store.UserKey{
			NodeID: req.RelayTarget.NodeId,
			UserID: req.RelayTarget.UserId,
		}, req.Sender, req.Body, mode)
		if err != nil {
			return s.writeStoreOrRequestError(req.RequestId, err)
		}
		return s.writeEnvelope(&internalproto.ServerEnvelope{
			Body: &internalproto.ServerEnvelope_SendMessageResponse{
				SendMessageResponse: &internalproto.SendMessageResponse{
					RequestId: req.RequestId,
					Body: &internalproto.SendMessageResponse_RelayAccepted{
						RelayAccepted: clientProtoRelayAccepted(packet),
					},
				},
			},
		})
	}
	if req.RelayTarget != nil {
		return s.writeStoreOrRequestError(req.RequestId, fmt.Errorf("%w: relay_target is only allowed for node ingress messages", store.ErrInvalidInput))
	}
	if req.DeliveryMode != internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_UNSPECIFIED {
		return s.writeStoreOrRequestError(req.RequestId, fmt.Errorf("%w: delivery_mode is only allowed for node ingress messages", store.ErrInvalidInput))
	}
	message, _, err := s.http.service.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: target,
		Sender:  req.Sender,
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
		return true, nil
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

func (s *clientWSSession) writeStoreOrRequestError(requestID uint64, err error) error {
	code := "internal_error"
	message := "internal server error"
	switch {
	case errors.Is(err, store.ErrForbidden):
		code = "forbidden"
		message = "forbidden"
	case errors.Is(err, store.ErrInvalidInput):
		code = "invalid_request"
		message = err.Error()
	case errors.Is(err, store.ErrNotFound):
		code = "not_found"
		message = "resource not found"
	}
	return s.writeError(code, message, requestID)
}

func (s *clientWSSession) writeError(code, message string, requestID uint64) error {
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_Error{Error: clientWSError(code, message, requestID)},
	})
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
		NodeId:   user.NodeID,
		UserId:   user.ID,
		Username: user.Username,
		Role:     user.Role,
	}
}

func clientProtoMessage(message store.Message) *internalproto.Message {
	return &internalproto.Message{
		UserNodeId:   message.UserNodeID,
		UserId:       message.UserID,
		NodeId:       message.NodeID,
		Seq:          message.Seq,
		Sender:       message.Sender,
		Body:         append([]byte(nil), message.Body...),
		CreatedAtHlc: message.CreatedAt.String(),
	}
}

func clientProtoPacket(packet store.TransientPacket) *internalproto.Packet {
	return &internalproto.Packet{
		PacketId:     packet.PacketID,
		SourceNodeId: packet.SourceNodeID,
		TargetNodeId: packet.TargetNodeID,
		RelayTarget:  &internalproto.UserRef{NodeId: packet.RelayTarget.NodeID, UserId: packet.RelayTarget.UserID},
		Sender:       packet.Sender,
		Body:         append([]byte(nil), packet.Body...),
		DeliveryMode: clientDeliveryModeProto(packet.DeliveryMode),
	}
}

func clientProtoRelayAccepted(packet store.TransientPacket) *internalproto.RelayAccepted {
	return &internalproto.RelayAccepted{
		PacketId:     packet.PacketID,
		SourceNodeId: packet.SourceNodeID,
		TargetNodeId: packet.TargetNodeID,
		RelayTarget:  &internalproto.UserRef{NodeId: packet.RelayTarget.NodeID, UserId: packet.RelayTarget.UserID},
		DeliveryMode: clientDeliveryModeProto(packet.DeliveryMode),
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
	return store.Message{
		UserNodeID: body.UserNodeId,
		UserID:     body.UserId,
		NodeID:     body.NodeId,
		Seq:        body.Seq,
		Sender:     strings.TrimSpace(body.Sender),
		Body:       append([]byte(nil), body.Body...),
		CreatedAt:  createdAt,
	}, true, nil
}
