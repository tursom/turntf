package api

import (
	"context"
	"errors"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	clientWSPollInterval  = time.Second
	clientWSPollBatchSize = 100
)

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
			if err := s.pushMessage(queued.message); err != nil {
				return err
			}
		}
	}
}

func (s *clientWSSession) shouldSkipPersistentEvent(eventSequence int64) bool {
	if s == nil || !s.requiresPersistentPush() {
		return true
	}
	s.persistentMu.Lock()
	defer s.persistentMu.Unlock()
	return eventSequence <= s.afterSequence
}

func (s *clientWSSession) handlePersistentEvent(eventSequence int64, message *store.Message) error {
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
			message:       *message,
		})
		s.persistentMu.Unlock()
		return nil
	}
	s.persistentMu.Unlock()
	return s.pushMessage(*message)
}

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

func (s *clientWSSession) canSeeMessage(ctx context.Context, message store.Message) (bool, error) {
	if isAdminRole(s.principal.User.Role) {
		return true, nil
	}
	key := message.UserKey()
	if key == s.principal.User.Key() {
		blocked, err := s.isSenderBlockedCached(ctx, message.Sender, message.CreatedAt)
		if err != nil {
			return false, err
		}
		return !blocked, nil
	}
	role, err := s.http.messageTargetRole(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	switch role {
	case store.RoleBroadcast:
		return true, nil
	case store.RoleChannel:
		return s.isSubscribedToChannelCached(ctx, key)
	default:
		return false, nil
	}
}

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

func (s *clientWSSession) isSubscribedToChannelCached(ctx context.Context, channel store.UserKey) (bool, error) {
	s.persistentMu.Lock()
	if entry, ok := s.subscriptionCache[channel]; ok && time.Now().Before(entry.expiresAt) {
		s.persistentMu.Unlock()
		return entry.value, nil
	}
	s.persistentMu.Unlock()

	subscribed, err := s.http.service.IsSubscribedToChannel(ctx, s.principal.User.Key(), channel)
	if err != nil {
		return false, err
	}

	s.persistentMu.Lock()
	s.subscriptionCache[channel] = clientBoolCacheEntry{
		value:     subscribed,
		expiresAt: time.Now().Add(clientWSVisibilityCacheTTL),
	}
	s.persistentMu.Unlock()
	return subscribed, nil
}

func (s *clientWSSession) invalidateBlacklistCache(sender store.UserKey) {
	if s == nil {
		return
	}
	s.persistentMu.Lock()
	delete(s.blacklistCache, sender)
	s.persistentMu.Unlock()
}

func (s *clientWSSession) invalidateChannelSubscriptionCache(channel store.UserKey) {
	if s == nil {
		return
	}
	s.persistentMu.Lock()
	delete(s.subscriptionCache, channel)
	s.persistentMu.Unlock()
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
