package api

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tursom/turntf/internal/store"
)

const clientWSVisibilityCacheTTL = 5 * time.Second

type clientBoolCacheEntry struct {
	value     bool
	expiresAt time.Time
}

type clientRoleCacheEntry struct {
	role      string
	expiresAt time.Time
}

type queuedPersistentMessage struct {
	eventSequence int64
	message       store.Message
}

func (h *HTTP) startPersistentDispatcher() {
	if h == nil || h.service == nil {
		return
	}
	h.dispatcherMu.Lock()
	defer h.dispatcherMu.Unlock()
	if h.dispatcherCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.dispatcherCancel = cancel

	afterSequence := int64(0)
	if seq, err := h.service.LastEventSequence(ctx); err == nil {
		afterSequence = seq
	} else {
		log.Warn().
			Err(err).
			Str("component", "api").
			Str("event", "client_persistent_dispatcher_watermark_failed").
			Msg("client persistent dispatcher failed to load initial watermark")
	}

	go h.runPersistentDispatcher(ctx, afterSequence)
}

func (h *HTTP) runPersistentDispatcher(ctx context.Context, afterSequence int64) {
	ticker := time.NewTicker(clientWSPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !h.hasPersistentSessions() {
			continue
		}

		events, err := h.service.ListEvents(ctx, afterSequence, clientWSPollBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn().
				Err(err).
				Str("component", "api").
				Str("event", "client_persistent_dispatcher_list_events_failed").
				Msg("client persistent dispatcher failed to list events")
			continue
		}

		for _, event := range events {
			if event.Sequence > afterSequence {
				afterSequence = event.Sequence
			}
			message, ok, err := messageFromClientPushEvent(event)
			if err != nil {
				log.Warn().
					Err(err).
					Str("component", "api").
					Str("event", "client_persistent_dispatcher_decode_failed").
					Int64("event_sequence", event.Sequence).
					Msg("client persistent dispatcher failed to decode event")
				continue
			}
			if !ok {
				continue
			}
			h.dispatchPersistentMessage(ctx, event.Sequence, message)
		}
	}
}

func (h *HTTP) dispatchPersistentMessage(ctx context.Context, eventSequence int64, message store.Message) {
	if h == nil {
		return
	}

	candidates, err := h.persistentCandidatesForMessage(ctx, message)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return
		}
		log.Warn().
			Err(err).
			Str("component", "api").
			Str("event", "client_persistent_dispatcher_target_lookup_failed").
			Int64("event_sequence", eventSequence).
			Int64("recipient_node_id", message.Recipient.NodeID).
			Int64("recipient_user_id", message.Recipient.UserID).
			Msg("client persistent dispatcher failed to resolve message target")
		return
	}

	for _, sess := range candidates {
		if sess == nil || sess.shouldSkipPersistentEvent(eventSequence) {
			continue
		}

		visible, err := sess.canSeeMessage(ctx, message)
		if err != nil {
			_ = sess.handlePersistentEvent(eventSequence, nil)
			sess.logWarn("client_persistent_authorize_failed", err).
				Int64("event_sequence", eventSequence).
				Msg("client persistent dispatcher failed to authorize message")
			continue
		}
		if visible {
			if err := sess.handlePersistentEvent(eventSequence, &message); err != nil {
				sess.handlePersistentDispatchFailure(err)
			}
			continue
		}
		_ = sess.handlePersistentEvent(eventSequence, nil)
	}
}

func (h *HTTP) hasPersistentSessions() bool {
	if h == nil {
		return false
	}
	h.persistentMu.RLock()
	defer h.persistentMu.RUnlock()
	return len(h.persistent) > 0
}

func (h *HTTP) registerPersistentSession(sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	h.startPersistentDispatcher()
	h.persistentMu.Lock()
	h.persistent[sess] = struct{}{}
	if sess.principal != nil && isAdminRole(sess.principal.User.Role) {
		h.persistentAdmin[sess] = struct{}{}
	}
	h.persistentMu.Unlock()
}

func (h *HTTP) unregisterPersistentSession(sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	h.persistentMu.Lock()
	delete(h.persistent, sess)
	delete(h.persistentAdmin, sess)
	h.persistentMu.Unlock()
}

func (h *HTTP) persistentCandidatesForMessage(ctx context.Context, message store.Message) ([]*clientWSSession, error) {
	role, err := h.messageTargetRole(ctx, message.UserKey())
	if err != nil {
		return nil, err
	}
	switch role {
	case store.RoleBroadcast, store.RoleChannel:
		return h.clonePersistentSessions(), nil
	default:
		return h.clonePersistentDirectRecipients(message.UserKey()), nil
	}
}

func (h *HTTP) clonePersistentSessions() []*clientWSSession {
	if h == nil {
		return nil
	}
	h.persistentMu.RLock()
	sessions := make([]*clientWSSession, 0, len(h.persistent))
	for sess := range h.persistent {
		sessions = append(sessions, sess)
	}
	h.persistentMu.RUnlock()
	return sessions
}

func (h *HTTP) clonePersistentDirectRecipients(recipient store.UserKey) []*clientWSSession {
	if h == nil {
		return nil
	}

	dedup := make(map[*clientWSSession]struct{})
	sessions := make([]*clientWSSession, 0, 4)

	shard := h.sessionShard(recipient)
	if shard != nil {
		shard.mu.RLock()
		if bucket := shard.sessions[recipient]; bucket != nil {
			for _, sess := range bucket.snapshot {
				if sess == nil || !sess.requiresPersistentPush() {
					continue
				}
				dedup[sess] = struct{}{}
				sessions = append(sessions, sess)
			}
		}
		shard.mu.RUnlock()
	}

	h.persistentMu.RLock()
	for sess := range h.persistentAdmin {
		if _, exists := dedup[sess]; exists {
			continue
		}
		dedup[sess] = struct{}{}
		sessions = append(sessions, sess)
	}
	h.persistentMu.RUnlock()
	return sessions
}

func (h *HTTP) messageTargetRole(ctx context.Context, key store.UserKey) (string, error) {
	now := time.Now()

	h.targetRoleMu.Lock()
	if entry, ok := h.targetRoleCache[key]; ok {
		if now.Before(entry.expiresAt) {
			h.targetRoleMu.Unlock()
			return entry.role, nil
		}
		delete(h.targetRoleCache, key)
	}
	h.targetRoleMu.Unlock()

	user, err := h.service.GetUser(ctx, key)
	if err != nil {
		return "", err
	}

	h.targetRoleMu.Lock()
	h.targetRoleCache[key] = clientRoleCacheEntry{
		role:      user.Role,
		expiresAt: now.Add(clientWSVisibilityCacheTTL),
	}
	h.targetRoleMu.Unlock()
	return user.Role, nil
}

func (h *HTTP) invalidateTargetRoleCache(key store.UserKey) {
	if h == nil {
		return
	}
	h.targetRoleMu.Lock()
	delete(h.targetRoleCache, key)
	h.targetRoleMu.Unlock()
}

func (h *HTTP) invalidateUserChannelSubscriptionCache(subscriber, channel store.UserKey) {
	for _, sess := range h.cloneSessionsForUser(subscriber) {
		sess.invalidateChannelSubscriptionCache(channel)
	}
}

func (h *HTTP) invalidateUserBlacklistCache(owner, blocked store.UserKey) {
	for _, sess := range h.cloneSessionsForUser(owner) {
		sess.invalidateBlacklistCache(blocked)
	}
}

func (h *HTTP) cloneSessionsForUser(key store.UserKey) []*clientWSSession {
	shard := h.sessionShard(key)
	if shard == nil {
		return nil
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	bucket := shard.sessions[key]
	if bucket == nil || len(bucket.snapshot) == 0 {
		return nil
	}
	sessions := make([]*clientWSSession, len(bucket.snapshot))
	copy(sessions, bucket.snapshot)
	return sessions
}
