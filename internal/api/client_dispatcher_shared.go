package api

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tursom/turntf/internal/store"
)

// clientWSVisibilityCacheTTL 客户端可见性缓存（黑名单、用户角色）的 TTL。
const clientWSVisibilityCacheTTL = 5 * time.Second

// clientBoolCacheEntry 布尔值缓存条目（用于黑名单）。
type clientBoolCacheEntry struct {
	value     bool
	expiresAt time.Time
}

// clientRoleCacheEntry 用户角色缓存条目。
type clientRoleCacheEntry struct {
	role      string
	expiresAt time.Time
}

// queuedPersistentMessage 暂存的持久化消息（在会话推送就绪前排队的消息）。
type queuedPersistentMessage struct {
	eventSequence int64
	message       *encodedPersistentMessage
}

type persistentDispatchPlan struct {
	role       string
	candidates []*clientWSSession
}

// startPersistentDispatcher 启动持久化事件分发器的后台 goroutine（仅启动一次）。
// 从当前最新事件序列号开始等待消息提交或兜底轮询，将新事件推送给持久化客户端会话。
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

	wake, unsubscribe := h.service.subscribeMessageCommits()
	go func() {
		defer unsubscribe()
		h.runPersistentDispatcher(ctx, afterSequence, wake)
	}()
}

// runPersistentDispatcher 持久化事件分发器的生产循环。消息提交事件负责立即唤醒，
// 每秒 ticker 仅用于在通知缺失时兜底追平事件日志。
func (h *HTTP) runPersistentDispatcher(ctx context.Context, afterSequence int64, wake <-chan struct{}) {
	ticker := time.NewTicker(clientWSPollInterval)
	defer ticker.Stop()
	h.runPersistentDispatcherWithFallback(ctx, afterSequence, wake, ticker.C)
}

// runPersistentDispatcherWithFallback 等待消息提交或兜底触发，并在每次触发后连续拉取，
// 直到事件日志返回不足一个批次。fallback 可为 nil，便于测试事件唤醒路径。
func (h *HTTP) runPersistentDispatcherWithFallback(
	ctx context.Context,
	afterSequence int64,
	wake <-chan struct{},
	fallback <-chan time.Time,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-fallback:
		}

		if !h.hasPersistentSessions() {
			continue
		}

		for {
			if ctx.Err() != nil {
				return
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
				break
			}

			retryBatch := false
			for _, event := range events {
				message, ok, err := messageFromClientPushEvent(event)
				if err != nil {
					log.Warn().
						Err(err).
						Str("component", "api").
						Str("event", "client_persistent_dispatcher_decode_failed").
						Int64("event_sequence", event.Sequence).
						Msg("client persistent dispatcher failed to decode event")
					afterSequence = event.Sequence
					continue
				}
				if !ok {
					afterSequence = event.Sequence
					continue
				}
				if err := h.dispatchPersistentMessage(ctx, event.Sequence, message); err != nil {
					retryBatch = true
					break
				}
				afterSequence = event.Sequence
			}

			if retryBatch || len(events) < clientWSPollBatchSize {
				break
			}
		}
	}
}

// dispatchPersistentMessage 将一条持久化消息分发给所有符合条件的候选会话。
// 先解析消息目标角色并获取候选会话；只有 direct 消息需要逐会话检查黑名单。
func (h *HTTP) dispatchPersistentMessage(ctx context.Context, eventSequence int64, message store.Message) error {
	if h == nil {
		return nil
	}

	plan, err := h.persistentCandidatesForMessage(ctx, message)
	if err != nil {
		log.Warn().
			Err(err).
			Str("component", "api").
			Str("event", "client_persistent_dispatcher_target_lookup_failed").
			Int64("event_sequence", eventSequence).
			Int64("recipient_node_id", message.Recipient.NodeID).
			Int64("recipient_user_id", message.Recipient.UserID).
			Msg("client persistent dispatcher failed to resolve message target")
		return err
	}
	h.persistentCandidateCount.Add(int64(len(plan.candidates)))
	encoded, encodeErr := encodePersistentMessage(message)

	for _, sess := range plan.candidates {
		if sess == nil || sess.shouldSkipPersistentEvent(eventSequence) {
			continue
		}

		visible := true
		if plan.role != store.RoleBroadcast && plan.role != store.RoleChannel {
			visible, err = sess.canReceiveDirectMessage(ctx, message)
			if err != nil {
				_ = sess.handlePersistentEvent(eventSequence, nil)
				sess.logWarn("client_persistent_authorize_failed", err).
					Int64("event_sequence", eventSequence).
					Msg("client persistent dispatcher failed to authorize message")
				continue
			}
		}
		if visible {
			if encodeErr != nil {
				_ = sess.handlePersistentEvent(eventSequence, nil)
				sess.handlePersistentDispatchFailure(encodeErr)
				continue
			}
			if err := sess.handlePersistentEvent(eventSequence, encoded); err != nil {
				sess.handlePersistentDispatchFailure(err)
			}
			continue
		}
		_ = sess.handlePersistentEvent(eventSequence, nil)
	}
	return nil
}

// hasPersistentSessions 检查是否有任何需要持久化推送的客户端会话。
func (h *HTTP) hasPersistentSessions() bool {
	if h == nil {
		return false
	}
	return h.persistentSessions != nil && h.persistentSessions.HasSessions()
}

// registerPersistentSession 注册一个需要持久化推送的客户端会话。会自动启动分发器（如果尚未启动）。
func (h *HTTP) registerPersistentSession(ctx context.Context, sess *clientWSSession) error {
	if h == nil || sess == nil || h.persistentSessions == nil {
		return nil
	}
	if err := h.persistentSessions.Register(ctx, sess); err != nil {
		return err
	}
	h.startPersistentDispatcher()
	return nil
}

// unregisterPersistentSession 注销一个持久化推送会话。
func (h *HTTP) unregisterPersistentSession(sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	if h.persistentSessions != nil {
		h.persistentSessions.Unregister(sess)
	}
}

// persistentCandidatesForMessage 确定消息的候选推送会话：
//   - 广播消息 → 所有持久化会话
//   - 频道消息 → 在线订阅会话 + 所有管理员会话
//   - 普通用户消息 → 该用户的直接接收者会话 + 所有管理员会话
func (h *HTTP) persistentCandidatesForMessage(ctx context.Context, message store.Message) (persistentDispatchPlan, error) {
	role, err := h.messageTargetRole(ctx, message.UserKey())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return persistentDispatchPlan{}, nil
		}
		return persistentDispatchPlan{}, err
	}
	switch role {
	case store.RoleBroadcast:
		return persistentDispatchPlan{role: role, candidates: h.persistentSessions.AllCandidates()}, nil
	case store.RoleChannel:
		candidates, err := h.persistentSessions.ChannelCandidates(ctx, message.UserKey())
		return persistentDispatchPlan{role: role, candidates: candidates}, err
	default:
		return persistentDispatchPlan{role: role, candidates: h.persistentSessions.DirectCandidates(message.UserKey())}, nil
	}
}

// messageTargetRole 带缓存查询消息目标用户的角色（用于确定推送策略：广播/频道/直接）。
// 缓存 TTL 为 clientWSVisibilityCacheTTL。
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

// invalidateTargetRoleCache 使指定用户的角色缓存失效（当用户角色变更时调用）。
func (h *HTTP) invalidateTargetRoleCache(key store.UserKey) {
	if h == nil {
		return
	}
	h.targetRoleMu.Lock()
	delete(h.targetRoleCache, key)
	h.targetRoleMu.Unlock()
}

// invalidateUserBlacklistCache 使指定用户的黑名单缓存失效（遍历该用户的所有会话）。
func (h *HTTP) invalidateUserBlacklistCache(owner, blocked store.UserKey) {
	for _, sess := range h.cloneSessionsForUser(owner) {
		sess.invalidateBlacklistCache(blocked)
	}
}

// cloneSessionsForUser 返回指定用户所有会话的快照副本（从会话 shard 中查找）。
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
