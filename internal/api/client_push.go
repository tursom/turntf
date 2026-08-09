package api

import (
	"context"
	"time"

	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	"github.com/tursom/turntf/internal/permission"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type encodedPersistentMessage struct {
	cursor  clientMessageCursor
	payload []byte
}

type initialPersistentMessageEncoder struct {
	recipient internalproto.UserRef
	sender    internalproto.UserRef
	message   internalproto.Message
	pushed    internalproto.MessagePushed
	body      internalproto.ServerEnvelope_MessagePushed
	envelope  internalproto.ServerEnvelope
}

func newInitialPersistentMessageEncoder() *initialPersistentMessageEncoder {
	encoder := &initialPersistentMessageEncoder{}
	encoder.message.Recipient = &encoder.recipient
	encoder.message.Sender = &encoder.sender
	encoder.pushed.Message = &encoder.message
	encoder.body.MessagePushed = &encoder.pushed
	encoder.envelope.Body = &encoder.body
	return encoder
}

// marshal 在一次初始补发中复用 protobuf 对象树。Body 只借用到本次同步
// marshal 返回为止；Marshal 每次仍生成独立 payload，允许传输层在 Send 后持有它。
func (e *initialPersistentMessageEncoder) marshal(message store.Message) ([]byte, error) {
	e.recipient.NodeId = message.Recipient.NodeID
	e.recipient.UserId = message.Recipient.UserID
	e.sender.NodeId = message.Sender.NodeID
	e.sender.UserId = message.Sender.UserID
	e.message.NodeId = message.NodeID
	e.message.Seq = message.Seq
	e.message.Body = message.Body
	e.message.CreatedAtHlc = message.CreatedAt.String()
	return gproto.Marshal(&e.envelope)
}

const (
	// clientWSPollInterval 持久化事件分发器在提交通知缺失时的兜底轮询间隔。
	clientWSPollInterval = time.Second
	// clientWSPollBatchSize 每次轮询拉取的最大事件数。
	clientWSPollBatchSize = 100
)

// pushInitialMessages 在客户端登录成功后推送历史消息（最多 1000 条，按时间正序）。
func (s *clientWSSession) pushInitialMessages(ctx context.Context) error {
	messages, err := s.http.service.ListMessagesByUser(ctx, s.principal.User.Key(), 1000)
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	encoder := newInitialPersistentMessageEncoder()
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		cursor := clientMessageCursor{nodeID: message.NodeID, seq: message.Seq}
		if !s.markMessageSeenIfNew(cursor) {
			continue
		}
		payload, err := encoder.marshal(message)
		if err != nil {
			return err
		}
		if err := s.writeEncodedEnvelope(payload); err != nil {
			return err
		}
	}
	return nil
}

// enablePersistentDispatch 启用持久化消息推送。先排空在就绪前暂存的消息队列，然后标记为就绪态。
// 处于"未就绪"状态是为了避免在登录流程中收到并发的事件推送导致消息乱序。
func (s *clientWSSession) enablePersistentDispatch() error {
	for {
		s.persistentMu.Lock()
		pending := append([]queuedPersistentMessage(nil), s.pendingPersistent...)
		s.pendingPersistent = nil
		if len(pending) == 0 {
			s.persistentReady = true
			s.persistentMu.Unlock()
			return nil
		}
		s.persistentMu.Unlock()

		for _, queued := range pending {
			if err := s.pushEncodedPersistentMessage(queued.message); err != nil {
				return err
			}
		}
	}
}

// shouldSkipPersistentEvent 判断事件是否应跳过（已处理过或不需持久化推送的会话）。
func (s *clientWSSession) shouldSkipPersistentEvent(eventSequence int64) bool {
	if s == nil || !s.requiresPersistentPush() {
		return true
	}
	s.persistentMu.Lock()
	defer s.persistentMu.Unlock()
	return eventSequence <= s.afterSequence
}

// handlePersistentEvent 处理一个持久化事件。如果会话尚未就绪，将消息暂存；就绪后直接推送。
func (s *clientWSSession) handlePersistentEvent(eventSequence int64, message *encodedPersistentMessage) error {
	if s == nil || !s.requiresPersistentPush() {
		return nil
	}

	s.persistentMu.Lock()
	if eventSequence <= s.afterSequence {
		s.persistentMu.Unlock()
		return nil
	}
	s.afterSequence = eventSequence
	if message == nil {
		s.persistentMu.Unlock()
		return nil
	}
	if !s.persistentReady {
		s.pendingPersistent = append(s.pendingPersistent, queuedPersistentMessage{
			eventSequence: eventSequence,
			message:       message,
		})
		s.persistentMu.Unlock()
		return nil
	}
	s.persistentMu.Unlock()
	return s.pushEncodedPersistentMessage(message)
}

// handlePersistentDispatchFailure 处理持久化推送失败：记录日志、注销会话、关闭连接。
func (s *clientWSSession) handlePersistentDispatchFailure(err error) {
	if s == nil || err == nil {
		return
	}
	s.logWarn("client_persistent_dispatch_failed", err).
		Msg("client persistent dispatcher failed to push message")
	if s.principal != nil {
		s.http.unregisterClientSession(s.principal.User.Key(), s)
	}
	_ = s.conn.Close()
}

// canReceiveDirectMessage 判断当前会话是否能接收一条 direct 消息。
// 管理员始终可见；普通接收者仍需检查消息创建时是否已拉黑发送者。
func (s *clientWSSession) canReceiveDirectMessage(ctx context.Context, message store.Message) (bool, error) {
	if permission.IsAdminRole(s.principal.User.Role) {
		return true, nil
	}
	if message.UserKey() != s.principal.User.Key() {
		return false, nil
	}
	blocked, err := s.isSenderBlockedCached(ctx, message.Sender, message.CreatedAt)
	if err != nil {
		return false, err
	}
	return !blocked, nil
}

// isSenderBlockedCached 带缓存的查询：sender 是否被当前用户拉黑。缓存 TTL 为 clientWSVisibilityCacheTTL。
func (s *clientWSSession) isSenderBlockedCached(ctx context.Context, sender store.UserKey, createdAt clock.Timestamp) (bool, error) {
	s.persistentMu.Lock()
	if entry, ok := s.blacklistCache[sender]; ok && time.Now().Before(entry.expiresAt) {
		s.persistentMu.Unlock()
		return entry.value, nil
	}
	s.persistentMu.Unlock()

	blocked, err := s.http.service.IsMessageBlockedByBlacklist(ctx, s.principal.User.Key(), sender, createdAt)
	if err != nil {
		return false, err
	}

	s.persistentMu.Lock()
	s.blacklistCache[sender] = clientBoolCacheEntry{
		value:     blocked,
		expiresAt: time.Now().Add(clientWSVisibilityCacheTTL),
	}
	s.persistentMu.Unlock()
	return blocked, nil
}

// invalidateBlacklistCache 使指定发送者的黑名单缓存失效（当用户更新黑名单时调用）。
func (s *clientWSSession) invalidateBlacklistCache(sender store.UserKey) {
	if s == nil {
		return
	}
	s.persistentMu.Lock()
	delete(s.blacklistCache, sender)
	s.persistentMu.Unlock()
}

// pushMessage 向客户端推送一条持久化消息（去重后通过 MessagePushed 信封发送）。
func (s *clientWSSession) pushMessage(message store.Message) error {
	encoded, err := encodePersistentMessage(message)
	if err != nil {
		return err
	}
	return s.pushEncodedPersistentMessage(encoded)
}

func encodePersistentMessage(message store.Message) (*encodedPersistentMessage, error) {
	payload, err := gproto.Marshal(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_MessagePushed{
			MessagePushed: &internalproto.MessagePushed{Message: clientProtoMessage(message)},
		},
	})
	if err != nil {
		return nil, err
	}
	return &encodedPersistentMessage{
		cursor:  clientMessageCursor{nodeID: message.NodeID, seq: message.Seq},
		payload: payload,
	}, nil
}

func (s *clientWSSession) pushEncodedPersistentMessage(message *encodedPersistentMessage) error {
	if message == nil {
		return nil
	}
	if !s.markMessageSeenIfNew(message.cursor) {
		return nil
	}
	return s.writeEncodedEnvelope(message.payload)
}

func (s *clientWSSession) markMessageSeenIfNew(cursor clientMessageCursor) bool {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if _, ok := s.seen[cursor]; ok {
		return false
	}
	s.seen[cursor] = struct{}{}
	return true
}

// pushPacket 向客户端推送一条即时消息（通过 PacketPushed 信封发送）。
func (s *clientWSSession) pushPacket(packet store.TransientPacket) error {
	return s.writeEnvelope(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_PacketPushed{
			PacketPushed: &internalproto.PacketPushed{Packet: clientProtoPacket(packet)},
		},
	})
}

// markSeen 标记消息为已见（用于去重，避免重复推送已收到的消息）。
func (s *clientWSSession) markSeen(nodeID, seq int64) {
	if nodeID <= 0 || seq <= 0 {
		return
	}
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	s.seen[clientMessageCursor{nodeID: nodeID, seq: seq}] = struct{}{}
}
