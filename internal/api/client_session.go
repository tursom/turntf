package api

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type clientMessageCursor struct {
	nodeID int64
	seq    int64
}

type clientWSSession struct {
	http              *HTTP
	conn              clientTransportConn
	protocol          string
	remoteAddr        string
	principal         *requestPrincipal
	realtimeOnly      bool
	transientOnly     bool
	afterSequence     int64
	seen              map[clientMessageCursor]struct{}
	seenMu            sync.Mutex
	writeMu           sync.Mutex
	persistentMu      sync.Mutex
	persistentReady   bool
	pendingPersistent []queuedPersistentMessage
	blacklistCache    map[store.UserKey]clientBoolCacheEntry
	subscriptionCache map[store.UserKey]clientBoolCacheEntry
}

func (h *HTTP) AcceptZeroMQConn(conn clientTransportConn) {
	if conn == nil {
		return
	}
	h.serveClientConn(conn, context.Background(), "")
}

func (h *HTTP) serveClientConn(conn clientTransportConn, baseCtx context.Context, path string) {
	if conn == nil {
		return
	}
	sess := &clientWSSession{
		http:              h,
		conn:              conn,
		protocol:          conn.Transport(),
		remoteAddr:        conn.RemoteAddr(),
		realtimeOnly:      path == clientRealtimeWSPath,
		seen:              make(map[clientMessageCursor]struct{}),
		blacklistCache:    make(map[store.UserKey]clientBoolCacheEntry),
		subscriptionCache: make(map[store.UserKey]clientBoolCacheEntry),
	}
	defer conn.Close()
	log.Info().
		Str("component", "api").
		Str("protocol", sess.protocol).
		Str("path", path).
		Str("remote_addr", sess.remoteAddr).
		Str("event", "client_transport_connected").
		Msg("client transport connected")

	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if err := sess.login(baseCtx); err != nil {
		sess.logWarn("client_login_failed", err).
			Msg("client transport login failed")
		_ = sess.writeEnvelope(&internalproto.ServerEnvelope{
			Body: &internalproto.ServerEnvelope_Error{
				Error: clientWSError("unauthorized", err.Error(), 0),
			},
		})
		return
	}
	defer h.unregisterClientSession(sess.principal.User.Key(), sess)

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	err := sess.readLoop(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		sess.logWarn("client_closed_with_error", err).
			Msg("client transport closed with error")
		return
	}
	sess.logInfo("client_closed").
		Msg("client transport closed")
}

func (s *clientWSSession) login(ctx context.Context) error {
	data, err := s.conn.Receive(ctx)
	if err != nil {
		if errors.Is(err, errNonBinaryClientFrame) {
			return fmt.Errorf("first message must be protobuf binary login")
		}
		return fmt.Errorf("read login: %w", err)
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
	s.transientOnly = login.TransientOnly || s.realtimeOnly
	if s.requiresPersistentPush() {
		afterSequence, err := s.http.service.LastEventSequence(ctx)
		if err != nil {
			return fmt.Errorf("load login event watermark: %w", err)
		}
		s.afterSequence = afterSequence
	}
	s.principal = &requestPrincipal{User: user}
	s.http.registerClientSession(s.principal.User.Key(), s)
	s.logInfo("client_authenticated").
		Bool("realtime_stream", s.realtimeOnly).
		Bool("transient_only", s.transientOnly).
		Msg("client transport authenticated")
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
	if s.requiresPersistentPush() {
		if err := s.pushInitialMessages(ctx); err != nil {
			s.http.unregisterClientSession(s.principal.User.Key(), s)
			return err
		}
		if err := s.enablePersistentDispatch(); err != nil {
			s.http.unregisterClientSession(s.principal.User.Key(), s)
			return err
		}
	}
	return nil
}

func (s *clientWSSession) requiresPersistentPush() bool {
	return s != nil && !s.transientOnly
}

func (s *clientWSSession) writeError(code, message string, requestID uint64) error {
	s.logWarn("client_error_response", errors.New(message)).
		Str("code", code).
		Uint64("request_id", requestID).
		Msg("client transport returning error")
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_Error{Error: clientWSError(code, message, requestID)},
	})
}

func (s *clientWSSession) logInfo(event string) *zerolog.Event {
	e := log.Info().
		Str("component", "api").
		Str("protocol", s.protocol).
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
		Str("protocol", s.protocol).
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
		Str("protocol", s.protocol).
		Str("event", "client_request").
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
	e := s.logInfo("client_request").Str("action", action)
	if requestID != 0 {
		e = e.Uint64("request_id", requestID)
	}
	return e
}

func (s *clientWSSession) writeEnvelope(envelope *internalproto.ServerEnvelope) error {
	data, err := gproto.Marshal(envelope)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.Send(context.Background(), data)
}

func clientWSError(code, message string, requestID uint64) *internalproto.Error {
	return &internalproto.Error{
		Code:      code,
		Message:   message,
		RequestId: requestID,
	}
}
