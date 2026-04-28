package cluster

import (
	"strings"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func (m *Manager) disconnectSuspicionGrace() time.Duration {
	if m == nil {
		return time.Duration(DefaultDisconnectSuspicionGraceMs) * time.Millisecond
	}
	graceMs := m.cfg.DisconnectSuspicionGraceMs
	if graceMs <= 0 {
		graceMs = DefaultDisconnectSuspicionGraceMs
	}
	return time.Duration(graceMs) * time.Millisecond
}

func (m *Manager) currentRuntimeEpochForNodeLocked(nodeID int64) uint64 {
	if m == nil || nodeID <= 0 {
		return 0
	}
	if nodeID == m.cfg.NodeID {
		return m.localRuntimeEpoch
	}
	current := m.remoteRuntimeEpochs[nodeID]
	if applied := m.onlinePresenceEpochs[nodeID]; applied > current {
		current = applied
	}
	return current
}

func (m *Manager) rememberRemoteRuntimeEpochLocked(nodeID int64, runtimeEpoch uint64) bool {
	if m == nil || nodeID <= 0 || nodeID == m.cfg.NodeID || runtimeEpoch == 0 {
		return false
	}
	if m.remoteRuntimeEpochs == nil {
		m.remoteRuntimeEpochs = make(map[int64]uint64)
	}
	current := m.remoteRuntimeEpochs[nodeID]
	if runtimeEpoch <= current {
		return false
	}
	m.remoteRuntimeEpochs[nodeID] = runtimeEpoch
	return true
}

func (m *Manager) rumorObservedAt(rumor *internalproto.NodeConnectivityRumor, fallback time.Time) time.Time {
	if rumor == nil || rumor.GetObservedAtMs() <= 0 {
		return fallback
	}
	return time.UnixMilli(rumor.GetObservedAtMs()).UTC()
}

func (m *Manager) markConnectivityRumorSeenLocked(rumor *internalproto.NodeConnectivityRumor, now time.Time) bool {
	if m == nil || rumor == nil {
		return false
	}
	if m.seenConnectivityRumors == nil {
		m.seenConnectivityRumors = make(map[connectivityRumorKey]time.Time)
	}
	key := connectivityRumorKey{
		targetNodeID:         rumor.GetTargetNodeId(),
		targetRuntimeEpoch:   rumor.GetTargetRuntimeEpoch(),
		reporterNodeID:       rumor.GetReporterNodeId(),
		reporterRuntimeEpoch: rumor.GetReporterRuntimeEpoch(),
		observedAtMs:         rumor.GetObservedAtMs(),
	}
	if _, ok := m.seenConnectivityRumors[key]; ok {
		return false
	}
	m.seenConnectivityRumors[key] = now
	return true
}

func (m *Manager) pruneSeenConnectivityRumorsLocked(now time.Time) {
	if m == nil || len(m.seenConnectivityRumors) == 0 {
		return
	}
	cutoff := now.Add(-2 * m.disconnectSuspicionGrace())
	for key, seenAt := range m.seenConnectivityRumors {
		if seenAt.Before(cutoff) {
			delete(m.seenConnectivityRumors, key)
		}
	}
}

func (m *Manager) noteDisconnectSuspicionLocked(rumor *internalproto.NodeConnectivityRumor, now time.Time) {
	if m == nil || rumor == nil || rumor.GetTargetNodeId() <= 0 || rumor.GetTargetRuntimeEpoch() == 0 {
		return
	}
	if m.disconnectSuspicions == nil {
		m.disconnectSuspicions = make(map[disconnectSuspicionKey]disconnectSuspicionState)
	}
	key := disconnectSuspicionKey{
		targetNodeID: rumor.GetTargetNodeId(),
		runtimeEpoch: rumor.GetTargetRuntimeEpoch(),
	}
	state := m.disconnectSuspicions[key]
	state.deadline = now.Add(m.disconnectSuspicionGrace())
	state.observedAt = m.rumorObservedAt(rumor, now)
	if state.reason == "" {
		state.reason = strings.TrimSpace(rumor.GetReason())
	}
	if state.reason == "" {
		state.reason = "connectivity_rumor"
	}
	if state.reporters == nil {
		state.reporters = make(map[int64]uint64)
	}
	if rumor.GetReporterNodeId() > 0 {
		state.reporters[rumor.GetReporterNodeId()] = rumor.GetReporterRuntimeEpoch()
	}
	m.disconnectSuspicions[key] = state
}

func (m *Manager) clearDisconnectSuspicionsForNodeLocked(nodeID int64, runtimeEpoch uint64) {
	if m == nil || nodeID <= 0 || len(m.disconnectSuspicions) == 0 {
		return
	}
	for key := range m.disconnectSuspicions {
		if key.targetNodeID != nodeID {
			continue
		}
		if runtimeEpoch == 0 || key.runtimeEpoch <= runtimeEpoch {
			delete(m.disconnectSuspicions, key)
		}
	}
}

func (m *Manager) clearEphemeralStateForNode(nodeID int64, runtimeEpoch uint64, reason string) {
	if m == nil || nodeID <= 0 || nodeID == m.cfg.NodeID {
		return
	}
	m.mu.Lock()
	m.clearEphemeralStateForNodeLocked(nodeID, runtimeEpoch)
	m.mu.Unlock()

	m.logInfo("ephemeral_state_cleared").
		Int64("target_node_id", nodeID).
		Uint64("runtime_epoch", runtimeEpoch).
		Str("reason", strings.TrimSpace(reason)).
		Msg("cleared remote ephemeral state")
}

func (m *Manager) clearEphemeralStateForNodeLocked(nodeID int64, runtimeEpoch uint64) {
	if m == nil || nodeID <= 0 || nodeID == m.cfg.NodeID {
		return
	}
	current := m.currentRuntimeEpochForNodeLocked(nodeID)
	if runtimeEpoch > 0 && current > 0 && current != runtimeEpoch {
		return
	}
	clearPresenceForNodeLocked(m.onlinePresenceByUser, nodeID)
	delete(m.loggedInUsersByNode, nodeID)
}

func (m *Manager) expireDisconnectSuspicions(now time.Time) {
	if m == nil {
		return
	}
	type clearAction struct {
		nodeID       int64
		runtimeEpoch uint64
		reason       string
	}
	actions := make([]clearAction, 0)

	m.mu.Lock()
	m.pruneSeenConnectivityRumorsLocked(now)
	for key, state := range m.disconnectSuspicions {
		if now.Before(state.deadline) {
			continue
		}
		if m.directAdjacencyCounts[key.targetNodeID] > 0 {
			delete(m.disconnectSuspicions, key)
			continue
		}
		current := m.currentRuntimeEpochForNodeLocked(key.targetNodeID)
		if current > 0 && current != key.runtimeEpoch {
			delete(m.disconnectSuspicions, key)
			continue
		}
		m.clearEphemeralStateForNodeLocked(key.targetNodeID, key.runtimeEpoch)
		delete(m.disconnectSuspicions, key)
		actions = append(actions, clearAction{
			nodeID:       key.targetNodeID,
			runtimeEpoch: key.runtimeEpoch,
			reason:       state.reason,
		})
	}
	m.mu.Unlock()

	for _, action := range actions {
		m.logInfo("disconnect_suspicion_expired").
			Int64("target_node_id", action.nodeID).
			Uint64("runtime_epoch", action.runtimeEpoch).
			Str("reason", action.reason).
			Msg("disconnect suspicion expired; remote ephemeral state cleared")
	}
}
