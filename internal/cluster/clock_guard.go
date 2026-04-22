package cluster

import (
	"fmt"
	"time"
)

type clockState string

const (
	clockStateProbing    clockState = "probing"
	clockStateTrusted    clockState = "trusted"
	clockStateObserving  clockState = "observing"
	clockStateRejected   clockState = "rejected"
	clockStateUnsynced   clockState = "unsynced"
	clockStateDegraded   clockState = "degraded"
	clockStateUnwritable clockState = "unwritable"
)

const (
	peerClockSampleWindowSize = 8
	peerClockSampleTTL        = 5 * time.Minute
)

type clockStateTransitionKey struct {
	FromState string
	ToState   string
	Reason    string
}

func (m *Manager) clockSyncTimeout() time.Duration {
	if m.cfg.ClockSyncTimeoutMs <= 0 {
		return timeSyncTimeout
	}
	return time.Duration(m.cfg.ClockSyncTimeoutMs) * time.Millisecond
}

func (m *Manager) clockTrustedFreshWindow() time.Duration {
	return time.Duration(m.cfg.ClockTrustedFreshMs) * time.Millisecond
}

func (m *Manager) clockObserveGraceWindow() time.Duration {
	return time.Duration(m.cfg.ClockObserveGraceMs) * time.Millisecond
}

func (m *Manager) clockWriteGateGraceWindow() time.Duration {
	return time.Duration(m.cfg.ClockWriteGateGraceMs) * time.Millisecond
}

func (m *Manager) isSingleNodeClockModeLocked() bool {
	return len(m.configuredPeers) == 0 && len(m.peers) == 0
}

func (m *Manager) nodeClockStateLocked() (clockState, string) {
	if m.isSingleNodeClockModeLocked() {
		return clockStateTrusted, "single_node_mode"
	}

	now := time.Now().UTC()
	hasTrustedPeer := false
	latestTrusted := m.lastTrustedClockSync
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
	if hasTrustedPeer && !latestTrusted.IsZero() && now.Sub(latestTrusted) <= m.clockTrustedFreshWindow() {
		return clockStateTrusted, "trusted_peer_available"
	}
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

func (m *Manager) refreshNodeClockStateLocked() {
	nextState, nextReason := m.nodeClockStateLocked()
	if m.clockState == nextState && m.clockReason == nextReason {
		return
	}
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
	if nextState == clockStateTrusted {
		peer.trustedSession = sess
		m.lastTrustedClockSync = time.Now().UTC()
	} else if peer.trustedSession == sess {
		peer.trustedSession = nil
	}
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
}

func (m *Manager) recordTimeSyncSample(sess *session, sample timeSyncSample) (clockState, string) {
	now := time.Now().UTC()
	if sample.sampledAt.IsZero() {
		sample.sampledAt = now
	}
	if sample.uncertaintyMs == 0 {
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
		peer.clockHealthyStreak = 0
		peer.clockSkewViolationStreak = 0
		reason = "slow_time_sync_sample"
		nextState = clockStateObserving
	} else {
		peer.clockOffsetMs = sample.offsetMs
		peer.clockUncertaintyMs = sample.uncertaintyMs
		lowerBound := absInt64(sample.offsetMs) - sample.uncertaintyMs
		upperBound := absInt64(sample.offsetMs) + sample.uncertaintyMs
		switch {
		case m.cfg.MaxClockSkewMs > 0 && lowerBound > m.cfg.MaxClockSkewMs:
			peer.clockHealthyStreak = 0
			peer.clockSkewViolationStreak++
			reason = "clock_skew_confirmed"
			nextState = clockStateObserving
			if peer.clockSkewViolationStreak >= m.cfg.ClockRejectAfterSkewSamples {
				reason = "clock_skew_rejected"
				nextState = clockStateRejected
			}
		case m.cfg.MaxClockSkewMs > 0 && upperBound > m.cfg.MaxClockSkewMs:
			peer.clockHealthyStreak = 0
			peer.clockSkewViolationStreak = 0
			reason = "clock_skew_near_limit"
			nextState = clockStateObserving
		default:
			peer.clockSkewViolationStreak = 0
			peer.clockHealthyStreak++
			if peer.clockState == clockStateObserving && peer.clockHealthyStreak < m.cfg.ClockRecoverAfterHealthySamples {
				reason = "clock_recovery_pending"
				nextState = clockStateObserving
			}
		}
	}

	m.setPeerClockStateLocked(sess, peer, nextState, reason)
	return nextState, reason
}

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
	if nextState == clockStateTrusted {
		nextState = clockStateObserving
	}
	if peer.clockFailureStreak >= m.cfg.ClockRejectAfterFailures {
		nextState = clockStateRejected
		reason = "time_sync_failures_exceeded"
	}
	m.setPeerClockStateLocked(sess, peer, nextState, reason)
	return nextState, reason
}

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
