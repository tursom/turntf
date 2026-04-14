package api

import (
	"context"
	"errors"
	"time"

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
