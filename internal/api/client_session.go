package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// clientMessageCursor 标识一条已见消息的位置（节点 + 序列号），用于客户端去重。
type clientMessageCursor struct {
	nodeID int64
	seq    int64
}

type unsupportedClientProtocolVersionError struct {
	got  string
	want string
}

func (e *unsupportedClientProtocolVersionError) Error() string {
	return fmt.Sprintf("unsupported client protocol version: got=%q want=%q", e.got, e.want)
}

// clientWSSession 表示一个客户端 WebSocket/ZeroMQ 会话，管理认证、读写、消息推送等全生命周期。
// 每个客户端连接对应一个 clientWSSession 实例。
type clientWSSession struct {
	http              *HTTP                                  // 关联的 HTTP 服务实例
	conn              clientTransportConn                    // 底层传输连接
	protocol          string                                 // 传输协议标识："ws" 或 "zmq"
	remoteAddr        string                                 // 对端地址
	sessionRef        store.SessionRef                       // 本会话的全局唯一引用
	loginName         string                                 // 登录名
	principal         *requestPrincipal                      // 认证后的用户身份
	realtimeOnly      bool                                   // 仅实时流（不持久化存储）
	transientOnly     bool                                   // 仅即时消息（不接收持久化推送）
	afterSequence     int64                                  // 登录时的最新事件序列号，用于跳过已处理的持久化事件
	seen              map[clientMessageCursor]struct{}       // 已见消息集合（去重）
	seenMu            sync.Mutex                             // 保护 seen
	writeMu           sync.Mutex                             // 串行化写入
	persistentMu      sync.Mutex                             // 保护持久化推送状态
	persistentReady   bool                                   // 持久化推送是否已就绪
	pendingPersistent []queuedPersistentMessage              // 推送就绪前暂存的持久化消息
	blacklistCache    map[store.UserKey]clientBoolCacheEntry // 黑名单查询缓存
	subscriptionCache map[store.UserKey]clientBoolCacheEntry // 频道订阅查询缓存
}

// AcceptZeroMQConn 接受 ZeroMQ 客户端连接，将其纳入与 WebSocket 相同的服务流程。
func (h *HTTP) AcceptZeroMQConn(conn clientTransportConn) {
	if conn == nil {
		return
	}
	h.serveClientConn(conn, context.Background(), "")
}

// serveClientConn 处理一个客户端连接的完整生命周期：认证 → 注册 → 读写循环 → 清理。
// path 用于区分实时流（/ws/realtime）和普通客户端连接（/ws/client）。
func (h *HTTP) serveClientConn(conn clientTransportConn, baseCtx context.Context, path string) {
	h.serveClientConnWithLoginTimeout(conn, baseCtx, path, clientLoginTimeout)
}

// serveClientConnWithLoginTimeout 执行客户端连接生命周期，并允许测试缩短登录期限。
func (h *HTTP) serveClientConnWithLoginTimeout(conn clientTransportConn, baseCtx context.Context, path string, loginTimeout time.Duration) {
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
	if loginTimeout <= 0 {
		loginTimeout = clientLoginTimeout
	}
	loginCtx, loginCancel := context.WithTimeout(baseCtx, loginTimeout)
	err := sess.login(loginCtx)
	loginCancel()
	if err != nil {
		code := "unauthorized"
		logEvent := sess.logWarn("client_login_failed", err)
		var versionErr *unsupportedClientProtocolVersionError
		if errors.As(err, &versionErr) {
			code = "unsupported_protocol_version"
			logEvent = logEvent.
				Str("client_protocol_version", versionErr.got).
				Str("required_client_protocol_version", versionErr.want)
		}
		logEvent.Msg("client transport login failed")
		_ = sess.writeTerminalEnvelope(&internalproto.ServerEnvelope{
			Body: &internalproto.ServerEnvelope_Error{
				Error: clientWSError(code, err.Error(), 0),
			},
		})
		return
	}
	defer h.unregisterClientSession(sess.principal.User.Key(), sess)

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	err = sess.readLoop(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		sess.logWarn("client_closed_with_error", err).
			Msg("client transport closed with error")
		return
	}
	sess.logInfo("client_closed").
		Msg("client transport closed")
}

// login 执行客户端认证握手：
//   - 读取并解析 Login 请求（支持 user key 或 login_name 两种方式）
//   - 在读取凭据、游标或会话状态前校验客户端协议版本
//   - 验证密码
//   - 记录已见消息光标（用于去重）
//   - 处理实时流和即时消息模式
//   - 返回 LoginResponse 包含用户信息和会话引用
//   - 如果需要持久化推送，推送历史消息并启用持久化分发
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
	if login.ProtocolVersion != internalproto.ClientProtocolVersion {
		return &unsupportedClientProtocolVersionError{
			got:  login.ProtocolVersion,
			want: internalproto.ClientProtocolVersion,
		}
	}
	hasUserSelector := login.User != nil
	loginName := strings.TrimSpace(login.LoginName)
	hasLoginNameSelector := loginName != ""
	if hasUserSelector == hasLoginNameSelector {
		return fmt.Errorf("exactly one of login user or login_name must be provided")
	}
	var user store.User
	if hasLoginNameSelector {
		user, err = s.http.service.AuthenticateUserByLoginName(ctx, loginName, login.Password)
	} else {
		key := store.UserKey{NodeID: login.User.NodeId, UserID: login.User.UserId}
		user, err = s.http.service.AuthenticateUser(ctx, key, login.Password)
	}
	if err != nil {
		return fmt.Errorf("invalid credentials")
	}
	loginName, err = s.http.service.GetUserLoginName(ctx, user.Key())
	if err != nil {
		return fmt.Errorf("load login name: %w", err)
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
	s.sessionRef = s.http.newSessionRef()
	s.loginName = loginName
	s.principal = &requestPrincipal{User: user}
	s.http.registerClientSession(s.principal.User.Key(), s)
	s.logInfo("client_authenticated").
		Bool("realtime_stream", s.realtimeOnly).
		Bool("transient_only", s.transientOnly).
		Msg("client transport authenticated")
	if err := s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_LoginResponse{
			LoginResponse: &internalproto.LoginResponse{
				User:            clientProtoUserWithLoginName(user, loginName),
				ProtocolVersion: internalproto.ClientProtocolVersion,
				SessionRef:      clientProtoSessionRef(s.sessionRef),
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

// requiresPersistentPush 判断此会话是否需要持久化消息推送（非即时消息模式）。
func (s *clientWSSession) requiresPersistentPush() bool {
	return s != nil && !s.transientOnly
}

// writeError 向客户端返回一个错误响应信封。
func (s *clientWSSession) writeError(code, message string, requestID uint64) error {
	s.logWarn("client_error_response", errors.New(message)).
		Str("code", code).
		Uint64("request_id", requestID).
		Msg("client transport returning error")
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_Error{Error: clientWSError(code, message, requestID)},
	})
}

// logInfo 创建带会话上下文的 Info 级别日志事件（协议、地址、用户身份）。
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

// logWarn 创建带会话上下文和错误信息的 Warn 级别日志事件。
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

// logDebug 创建带请求上下文的 Debug 级别日志事件。
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

// logRequest 创建 Info 级别的客户端请求日志事件，包含 action 和 requestID。
func (s *clientWSSession) logRequest(action string, requestID uint64) *zerolog.Event {
	e := s.logInfo("client_request").Str("action", action)
	if requestID != 0 {
		e = e.Uint64("request_id", requestID)
	}
	return e
}

// writeEnvelope 将 protobuf 信封序列化后通过底层连接发送给客户端。使用互斥锁保证串行写入。
func (s *clientWSSession) writeEnvelope(envelope *internalproto.ServerEnvelope) error {
	data, err := gproto.Marshal(envelope)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.Send(context.Background(), data)
}

// writeTerminalEnvelope 在紧接着关闭连接的场景中等待传输层接收消息。
// WebSocket Send 本身是同步写入；ZeroMQ mux 则通过可选确认接口等待路由队列刷入 socket。
func (s *clientWSSession) writeTerminalEnvelope(envelope *internalproto.ServerEnvelope) error {
	data, err := gproto.Marshal(envelope)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if waiter, ok := s.conn.(clientTransportSendWaiter); ok {
		ctx, cancel := context.WithTimeout(context.Background(), clientWSWriteWait)
		defer cancel()
		return waiter.SendAndWait(ctx, data)
	}
	return s.conn.Send(context.Background(), data)
}

// clientWSError 构造一个客户端协议错误对象。
func clientWSError(code, message string, requestID uint64) *internalproto.Error {
	return &internalproto.Error{
		Code:      code,
		Message:   message,
		RequestId: requestID,
	}
}

// onlineSession 构造此会话对应的 OnlineSession 表示，用于向集群注册。
func (s *clientWSSession) onlineSession() store.OnlineSession {
	user := store.UserKey{}
	if s != nil && s.principal != nil {
		user = s.principal.User.Key()
	}
	return store.OnlineSession{
		User:             user,
		SessionRef:       s.sessionRef,
		Transport:        s.protocol,
		TransientCapable: true,
	}
}
