package cluster

import (
	"fmt"
	"time"
)

// clockState 表示节点或对等节点的时钟信任状态。
// 时钟状态机用于保护集群免受时钟偏差导致的脑裂和数据损坏。
type clockState string

// 时钟状态机的七个状态：
//
//	probing → trusted → observing → rejected
//	                ↓           ↓
//	           unsynced     degraded → unwritable
//
// trusted是正常工作状态；probing是初始探测状态；
// observing是过渡状态（同步刚过期或样本不完美）；
// rejected是对等节点被拒绝；degraded/unwritable是节点级的降级状态。
const (
	// clockStateProbing 是初始状态，等待首次可信的时钟同步样本。
	clockStateProbing clockState = "probing"
	// clockStateTrusted 表示时钟同步可信，事件应用和快照流量正常放行。
	clockStateTrusted clockState = "trusted"
	// clockStateObserving 表示可信同步已过期或样本接近偏差阈值，需要继续观察。
	clockStateObserving clockState = "observing"
	// clockStateRejected 表示该对等节点的时钟已被拒绝，不允许事件应用。
	clockStateRejected clockState = "rejected"
	// clockStateUnsynced 表示节点从未与任何对等节点完成可信的时钟同步。
	clockStateUnsynced clockState = "unsynced"
	// clockStateDegraded 表示同步已过期超过观察容忍期，但仍允许快照流量。
	clockStateDegraded clockState = "degraded"
	// clockStateUnwritable 是最终降级状态，事件应用被阻止，仅允许快照同步。
	clockStateUnwritable clockState = "unwritable"
)

const (
	// peerClockSampleWindowSize 是每个对等节点保留的时钟样本数量。
	peerClockSampleWindowSize = 8
	// peerClockSampleTTL 是单个时钟样本的有效期。
	peerClockSampleTTL = 5 * time.Minute
)

// clockStateTransitionKey 记录一次状态转移，用于可观测性统计。
type clockStateTransitionKey struct {
	FromState string
	ToState   string
	Reason    string
}

// clockSyncTimeout 返回时钟同步请求的超时时间。
func (m *Manager) clockSyncTimeout() time.Duration {
	if m.cfg.ClockSyncTimeoutMs <= 0 {
		return timeSyncTimeout
	}
	return time.Duration(m.cfg.ClockSyncTimeoutMs) * time.Millisecond
}

// clockTrustedFreshWindow 返回可信同步的"新鲜"窗口。
// 最近的同步在此窗口内 → 状态为trusted。
func (m *Manager) clockTrustedFreshWindow() time.Duration {
	return time.Duration(m.cfg.ClockTrustedFreshMs) * time.Millisecond
}

// clockObserveGraceWindow 返回观察容忍窗口。
// 同步过期但在此窗口内 → 状态为observing。
func (m *Manager) clockObserveGraceWindow() time.Duration {
	return time.Duration(m.cfg.ClockObserveGraceMs) * time.Millisecond
}

// clockWriteGateGraceWindow 返回写门控容忍窗口。
// 同步过期超过观察窗口但在此窗口内 → 状态为degraded。
func (m *Manager) clockWriteGateGraceWindow() time.Duration {
	return time.Duration(m.cfg.ClockWriteGateGraceMs) * time.Millisecond
}

// isSingleNodeClockModeLocked 检查是否处于单节点模式（无任何对等节点）。
// 单节点模式下始终返回trusted状态。
func (m *Manager) isSingleNodeClockModeLocked() bool {
	return len(m.configuredPeers) == 0 && len(m.peers) == 0
}

// nodeClockStateLocked 计算当前节点的全局时钟状态。
//
// 状态判定逻辑：
//   - 单节点模式 → trusted（无需时钟同步）
//   - 存在可信对等节点且最近同步在新鲜窗口内 → trusted
//   - 同步过期但在观察容忍窗口内 → observing
//   - 同步过期但在写门控容忍窗口内 → degraded
//   - 同步完全过期 → unwritable
//   - 从未完成可信同步 → unsynced
func (m *Manager) nodeClockStateLocked() (clockState, string) {
	if m.isSingleNodeClockModeLocked() {
		return clockStateTrusted, "single_node_mode"
	}

	now := time.Now().UTC()
	hasTrustedPeer := false
	latestTrusted := m.lastTrustedClockSync
	// 遍历所有对等节点，找到最近的可信同步时间
	for _, peer := range m.peers {
		if peer != nil && peer.trustedSession != nil {
			hasTrustedPeer = true
			candidate := peer.lastCredibleClockSync
			if candidate.IsZero() {
				candidate = peer.lastClockSync
			}
			if latestTrusted.IsZero() || (!candidate.IsZero() && candidate.After(latestTrusted)) {
				latestTrusted = candidate
			}
		}
	}
	// 有可信对等节点且同步新鲜 → trusted
	if hasTrustedPeer && !latestTrusted.IsZero() && now.Sub(latestTrusted) <= m.clockTrustedFreshWindow() {
		return clockStateTrusted, "trusted_peer_available"
	}
	// 同步过期，按时间窗逐步降级
	if !latestTrusted.IsZero() {
		age := now.Sub(latestTrusted)
		if age <= m.clockObserveGraceWindow() {
			return clockStateObserving, "trusted_sync_recently_expired"
		}
		if age <= m.clockWriteGateGraceWindow() {
			return clockStateDegraded, "trusted_sync_stale"
		}
		return clockStateUnwritable, "trusted_sync_expired"
	}
	return clockStateUnsynced, "trusted_sync_unavailable"
}

// refreshNodeClockStateLocked 更新节点的时钟状态并记录状态转移。
func (m *Manager) refreshNodeClockStateLocked() {
	nextState, nextReason := m.nodeClockStateLocked()
	if m.clockState == nextState && m.clockReason == nextReason {
		return
	}
	// 记录状态转移统计
	if m.clockState != "" && nextState != "" {
		key := clockStateTransitionKey{
			FromState: string(m.clockState),
			ToState:   string(nextState),
			Reason:    nextReason,
		}
		m.clockStateTransitions[key]++
	}
	m.clockState = nextState
	m.clockReason = nextReason
}

// peerClockStateLocked 返回指定对等节点的时钟状态及其原因。
//
// 针对每个对等节点，检查其当前的clockState、可信会话和最近的同步样本：
//   - trusted且有可信会话且同步新鲜 → trusted
//   - trusted但会话或样本过期 → observing
//   - observing且无错误信息 → 继续观察
//   - rejected → 保持拒绝
//   - 其他 → probing
func (m *Manager) peerClockStateLocked(peerID int64) (clockState, string) {
	peer := m.peers[peerID]
	if peer == nil {
		return clockStateProbing, "peer_untracked"
	}
	switch peer.clockState {
	case clockStateTrusted:
		if peer.trustedSession != nil {
			lastTrusted := peer.lastCredibleClockSync
			if lastTrusted.IsZero() {
				lastTrusted = peer.lastClockSync
			}
			if !lastTrusted.IsZero() && time.Now().UTC().Sub(lastTrusted) <= m.clockTrustedFreshWindow() {
				return clockStateTrusted, "trusted_sample_available"
			}
			return clockStateObserving, "trusted_sample_stale"
		}
		return clockStateObserving, "trusted_session_missing"
	case clockStateObserving:
		if peer.clockLastError != "" {
			return clockStateObserving, peer.clockLastError
		}
		return clockStateObserving, "clock_under_observation"
	case clockStateRejected:
		if peer.clockLastError != "" {
			return clockStateRejected, peer.clockLastError
		}
		return clockStateRejected, "clock_rejected"
	case clockStateProbing:
		return clockStateProbing, "awaiting_credible_sample"
	default:
		return clockStateProbing, "awaiting_credible_sample"
	}
}

// setPeerClockStateLocked 更新对等节点的时钟状态并级联刷新节点状态。
func (m *Manager) setPeerClockStateLocked(sess *session, peer *peerState, nextState clockState, reason string) {
	if peer == nil {
		return
	}
	peer.clockState = nextState
	if nextState == clockStateTrusted {
		peer.clockLastError = ""
	} else {
		peer.clockLastError = reason
	}
	// 维护可信会话引用：trusted状态时建立，其他状态时清除
	if nextState == clockStateTrusted {
		peer.trustedSession = sess
		m.lastTrustedClockSync = time.Now().UTC()
	} else if peer.trustedSession == sess {
		peer.trustedSession = nil
	}
	// 级联刷新：对等节点状态变化可能影响全局节点状态
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
}

// recordTimeSyncSample 记录一次时钟同步样本并评估对等节点时钟状态。
//
// 判定逻辑：
//   - 不可信样本（RTT过高）→ observing
//   - 可信样本，偏移量下界超过MaxClockSkewMs → 偏差确认，累计后可能→rejected
//   - 可信样本，偏移量上界超过MaxClockSkewMs → 偏差接近阈值→observing
//   - 可信样本，在阈值内 → 健康计数+1，可能从observing恢复至trusted
func (m *Manager) recordTimeSyncSample(sess *session, sample timeSyncSample) (clockState, string) {
	now := time.Now().UTC()
	if sample.sampledAt.IsZero() {
		sample.sampledAt = now
	}
	if sample.uncertaintyMs == 0 {
		// 不确定性默认为 RTT/2 + 50ms
		sample.uncertaintyMs = maxInt64(sample.rttMs/2, 0) + 50
	}
	if !sample.credible {
		sample.credible = sample.rttMs <= m.cfg.ClockCredibleRttMs
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	peer := m.peers[sess.peerID]
	if peer == nil {
		return clockStateProbing, "peer_untracked"
	}
	peer.lastClockSync = now
	peer.clockSamples = append(peer.clockSamples, sample)
	peer.clockSamples = pruneTimeSyncSamples(peer.clockSamples, now)
	peer.clockFailureStreak = 0
	if sample.credible {
		peer.lastCredibleClockSync = now
	}

	reason := "trusted_sample_available"
	nextState := clockStateTrusted
	if !sample.credible {
		// RTT过高：降为observing
		peer.clockHealthyStreak = 0
		peer.clockSkewViolationStreak = 0
		reason = "slow_time_sync_sample"
		nextState = clockStateObserving
	} else {
		peer.clockOffsetMs = sample.offsetMs
		peer.clockUncertaintyMs = sample.uncertaintyMs
		// 使用不确定区间评估时钟偏差风险
		lowerBound := absInt64(sample.offsetMs) - sample.uncertaintyMs
		upperBound := absInt64(sample.offsetMs) + sample.uncertaintyMs
		switch {
		case m.cfg.MaxClockSkewMs > 0 && lowerBound > m.cfg.MaxClockSkewMs:
			// 偏差下界已超过阈值：确认存在严重时钟偏差
			peer.clockHealthyStreak = 0
			peer.clockSkewViolationStreak++
			reason = "clock_skew_confirmed"
			nextState = clockStateObserving
			// 连续多次确认偏差 → 拒绝该对等节点
			if peer.clockSkewViolationStreak >= m.cfg.ClockRejectAfterSkewSamples {
				reason = "clock_skew_rejected"
				nextState = clockStateRejected
			}
		case m.cfg.MaxClockSkewMs > 0 && upperBound > m.cfg.MaxClockSkewMs:
			// 偏差上界超过阈值但下界仍在范围内：接近偏差阈值
			peer.clockHealthyStreak = 0
			peer.clockSkewViolationStreak = 0
			reason = "clock_skew_near_limit"
			nextState = clockStateObserving
		default:
			// 样本在安全范围内
			peer.clockSkewViolationStreak = 0
			peer.clockHealthyStreak++
			// 从observing恢复需要连续多个健康样本
			if peer.clockState == clockStateObserving && peer.clockHealthyStreak < m.cfg.ClockRecoverAfterHealthySamples {
				reason = "clock_recovery_pending"
				nextState = clockStateObserving
			}
		}
	}

	m.setPeerClockStateLocked(sess, peer, nextState, reason)
	return nextState, reason
}

// recordTimeSyncFailure 记录一次时钟同步失败。
//
// 连续失败次数超过ClockRejectAfterFailures时，对等节点状态变为rejected。
func (m *Manager) recordTimeSyncFailure(sess *session, err error) (clockState, string) {
	now := time.Now().UTC()
	reason := "time_sync_failed"
	if err != nil {
		reason = err.Error()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	peer := m.peers[sess.peerID]
	if peer == nil {
		return clockStateProbing, reason
	}
	peer.lastClockSync = now
	peer.clockFailures++
	peer.clockFailureStreak++
	peer.clockHealthyStreak = 0
	peer.clockSkewViolationStreak = 0
	nextState := peer.clockState
	if nextState == "" {
		nextState = clockStateProbing
	}
	// trusted状态出现失败 → 立即降为observing
	if nextState == clockStateTrusted {
		nextState = clockStateObserving
	}
	// 连续失败达到阈值 → 拒绝
	if peer.clockFailureStreak >= m.cfg.ClockRejectAfterFailures {
		nextState = clockStateRejected
		reason = "time_sync_failures_exceeded"
	}
	m.setPeerClockStateLocked(sess, peer, nextState, reason)
	return nextState, reason
}

// pruneTimeSyncSamples 清理过期的时钟同步样本，仅保留peerClockSampleWindowSize个最新样本。
func pruneTimeSyncSamples(samples []timeSyncSample, now time.Time) []timeSyncSample {
	filtered := make([]timeSyncSample, 0, len(samples))
	for _, sample := range samples {
		if sample.sampledAt.IsZero() || now.Sub(sample.sampledAt) > peerClockSampleTTL {
			continue
		}
		filtered = append(filtered, sample)
	}
	if len(filtered) > peerClockSampleWindowSize {
		filtered = filtered[len(filtered)-peerClockSampleWindowSize:]
	}
	return filtered
}

// allowEventApplyLocked 检查是否允许来自指定对等节点的事件应用。
// 当节点处于unwritable状态或对等节点处于rejected状态时拒绝。
//
// 这是写门控的核心入口：在所有事件写入路径上调用，确保时钟保护生效。
func (m *Manager) allowEventApplyLocked(peerID int64) error {
	if m.isSingleNodeClockModeLocked() {
		return nil
	}
	nodeState, reason := m.nodeClockStateLocked()
	if nodeState == clockStateUnwritable {
		return fmt.Errorf("%w: event apply disabled while node clock state=%s reason=%s", errClockProtectionRejected, nodeState, reason)
	}
	peerState, peerReason := m.peerClockStateLocked(peerID)
	if peerState == clockStateRejected {
		return fmt.Errorf("%w: event apply disabled for peer=%d state=%s reason=%s", errClockProtectionRejected, peerID, peerState, peerReason)
	}
	return nil
}

// allowEventApplyForSessionLocked 检查是否允许来自指定会话对等节点的事件应用。
// 合成的mesh会话（没有实际传输连接）永远放行，因为它们继承了经过验证的路由路径。
func (m *Manager) allowEventApplyForSessionLocked(sess *session) error {
	if sess == nil {
		return nil
	}
	// 合成的mesh会话没有传统的时钟同步通道，因此旧的写门控仅适用于基于传输的会话。
	if sess.conn == nil {
		return nil
	}
	return m.allowEventApplyLocked(sess.peerID)
}

// allowSnapshotTrafficLocked 检查是否允许与指定对等节点进行快照流量交换。
// 仅当节点状态为trusted或observing时才允许。rejected的对等节点也不能参与快照交换。
func (m *Manager) allowSnapshotTrafficLocked(peerID int64) error {
	if m.isSingleNodeClockModeLocked() {
		return nil
	}
	nodeState, reason := m.nodeClockStateLocked()
	if nodeState != clockStateTrusted && nodeState != clockStateObserving {
		return fmt.Errorf("%w: snapshot traffic disabled while node clock state=%s reason=%s", errClockProtectionRejected, nodeState, reason)
	}
	peerState, peerReason := m.peerClockStateLocked(peerID)
	if peerState == clockStateRejected {
		return fmt.Errorf("%w: snapshot traffic disabled for peer=%d state=%s reason=%s", errClockProtectionRejected, peerID, peerState, peerReason)
	}
	return nil
}

// allowSnapshotTrafficForSessionLocked 检查是否允许与指定会话的对等节点进行快照流量交换。
func (m *Manager) allowSnapshotTrafficForSessionLocked(sess *session) error {
	if sess == nil {
		return nil
	}
	if sess.conn == nil {
		return nil
	}
	return m.allowSnapshotTrafficLocked(sess.peerID)
}
