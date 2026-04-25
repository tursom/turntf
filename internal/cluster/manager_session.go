package cluster

import (
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func (m *Manager) activateSession(sess *session) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return false
	}

	if peer.active == nil {
		peer.active = sess
		if peer.sessions == nil {
			peer.sessions = make(map[uint64]*session)
		}
		peer.sessions[sess.connectionID] = sess
		eventName := "peer_joined"
		message := "peer joined"
		if peer.joinedLogged {
			eventName = "peer_reconnected"
			message = "peer reconnected"
		} else {
			peer.joinedLogged = true
		}
		m.logSessionEvent(eventName, sess).
			Msg(message)
		return true
	}
	if peer.active == sess {
		if peer.sessions == nil {
			peer.sessions = make(map[uint64]*session)
		}
		peer.sessions[sess.connectionID] = sess
		return true
	}

	preferOutbound := m.cfg.NodeID < sess.peerID
	shouldKeepNew := sess.outbound == preferOutbound
	if !shouldKeepNew {
		return false
	}

	old := peer.active
	peer.active = sess
	if peer.sessions == nil {
		peer.sessions = make(map[uint64]*session)
	}
	peer.sessions[sess.connectionID] = sess
	go old.close()
	return true
}

func sessionDirection(sess *session) string {
	if sess != nil && sess.outbound {
		return "outbound"
	}
	return "inbound"
}

func (m *Manager) deactivateSession(sess *session) {
	if sess.peerID == 0 {
		return
	}

	wasActive := false
	wasTrusted := false
	m.mu.Lock()
	peer, ok := m.peers[sess.peerID]
	if ok && peer.active == sess {
		peer.active = nil
		wasActive = true
	}
	if ok && peer.sessions != nil {
		delete(peer.sessions, sess.connectionID)
	}
	if ok && peer.trustedSession == sess {
		if peer.active != nil && peer.active != sess {
			peer.trustedSession = peer.active
			peer.clockOffsetMs = peer.active.clockOffset()
			peer.clockState = clockStateTrusted
		} else {
			peer.trustedSession = nil
			peer.clockOffsetMs = 0
			peer.clockState = clockStateObserving
			wasTrusted = true
		}
	}
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
	m.mu.Unlock()
	m.logSessionEvent("peer_session_closed", sess).
		Bool("was_active", wasActive).
		Bool("was_trusted", wasTrusted).
		Msg("peer session closed")
}

func (s *session) enqueue(envelope *internalproto.Envelope) {
	defer func() {
		_ = recover()
	}()

	if s.manager == nil || s.manager.ctx == nil {
		select {
		case s.send <- envelope:
		default:
		}
		return
	}

	select {
	case s.send <- envelope:
	case <-s.manager.ctx.Done():
	}
}

func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	pending := make([]chan timeSyncResult, 0, len(s.pendingTimeSync))
	for requestID, ch := range s.pendingTimeSync {
		delete(s.pendingTimeSync, requestID)
		pending = append(pending, ch)
	}
	close(s.send)
	s.mu.Unlock()

	for _, ch := range pending {
		select {
		case ch <- timeSyncResult{err: errSessionClosed}:
		default:
		}
		close(ch)
	}

	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *session) noteRemoteOriginProgress(progress []*internalproto.OriginProgress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remoteOriginProgress == nil {
		s.remoteOriginProgress = make(map[int64]uint64)
	}
	for _, item := range progress {
		if item == nil || item.OriginNodeId <= 0 {
			continue
		}
		if item.LastEventId > s.remoteOriginProgress[item.OriginNodeId] {
			s.remoteOriginProgress[item.OriginNodeId] = item.LastEventId
		}
	}
}

func (s *session) noteRemoteOriginEvent(originNodeID int64, eventID uint64) {
	if originNodeID <= 0 || eventID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remoteOriginProgress == nil {
		s.remoteOriginProgress = make(map[int64]uint64)
	}
	if eventID > s.remoteOriginProgress[originNodeID] {
		s.remoteOriginProgress[originNodeID] = eventID
	}
}

func (s *session) remoteOriginEventID(originNodeID int64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteOriginProgress[originNodeID]
}

func (s *session) remoteOriginProgressSnapshot() map[int64]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	progress := make(map[int64]uint64, len(s.remoteOriginProgress))
	for originNodeID, eventID := range s.remoteOriginProgress {
		progress[originNodeID] = eventID
	}
	return progress
}

func (s *session) beginPendingPull(originNodeID int64, afterEventID uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingPulls == nil {
		s.pendingPulls = make(map[int64]pendingPullState)
	}
	if _, ok := s.pendingPulls[originNodeID]; ok {
		return 0, false
	}
	s.nextPullRequestID++
	requestID := s.nextPullRequestID
	s.pendingPulls[originNodeID] = pendingPullState{
		RequestID:    requestID,
		AfterEventID: afterEventID,
	}
	return requestID, true
}

func (s *session) hasPendingPull(originNodeID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pendingPulls[originNodeID]
	return ok
}

func (s *session) hasPendingPulls() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingPulls) > 0
}

func (s *session) cancelPendingPull(originNodeID int64, requestID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.pendingPulls[originNodeID]
	if ok && state.RequestID == requestID {
		delete(s.pendingPulls, originNodeID)
	}
}

func (s *session) completePendingPull(originNodeID int64, requestID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.pendingPulls[originNodeID]
	if !ok || state.RequestID != requestID {
		return false
	}
	delete(s.pendingPulls, originNodeID)
	return true
}

func (s *session) beginBootstrap() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bootstrapStarted {
		return false
	}
	s.bootstrapStarted = true
	return true
}

func (s *session) markReplicationReady() {
	var manager *Manager
	var peerID int64
	s.mu.Lock()
	if !s.replicationReady {
		manager = s.manager
		peerID = s.peerID
	}
	s.replicationReady = true
	s.mu.Unlock()
	if manager != nil && peerID > 0 {
		manager.markSnapshotDigestDirty(peerID, false)
	}
}

func (s *session) isReplicationReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicationReady
}

func (s *session) startSyncLoop(run func()) {
	s.mu.Lock()
	if s.syncLoopStarted {
		s.mu.Unlock()
		return
	}
	s.syncLoopStarted = true
	s.mu.Unlock()

	go run()
}

func (s *session) beginTimeSync() (uint64, chan timeSyncResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingTimeSync == nil {
		s.pendingTimeSync = make(map[uint64]chan timeSyncResult)
	}
	s.nextTimeSyncID++
	requestID := s.nextTimeSyncID
	ch := make(chan timeSyncResult, 1)
	s.pendingTimeSync[requestID] = ch
	return requestID, ch
}

func (s *session) cancelTimeSync(requestID uint64, err error) {
	s.mu.Lock()
	ch, ok := s.pendingTimeSync[requestID]
	if ok {
		delete(s.pendingTimeSync, requestID)
	}
	s.mu.Unlock()

	if !ok {
		return
	}
	select {
	case ch <- timeSyncResult{err: err}:
	default:
	}
	close(ch)
}

func (s *session) resolveTimeSync(requestID uint64, result timeSyncResult) bool {
	s.mu.Lock()
	ch, ok := s.pendingTimeSync[requestID]
	if ok {
		delete(s.pendingTimeSync, requestID)
	}
	s.mu.Unlock()

	if !ok {
		return false
	}
	ch <- result
	close(ch)
	return true
}

func (s *session) setClockOffset(offsetMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clockOffsetMs = offsetMs
}

func (s *session) clockOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clockOffsetMs
}

func (s *session) observeRTT(rttMs int64) {
	if rttMs < 0 {
		rttMs = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.smoothedRTTMs == 0 {
		s.smoothedRTTMs = rttMs
		s.jitterPenaltyMs = 0
		s.lastRTTUpdate = time.Now().UTC()
		return
	}
	diff := absInt64(rttMs - s.smoothedRTTMs)
	s.jitterPenaltyMs = (3*s.jitterPenaltyMs + diff) / 4
	s.smoothedRTTMs = (7*s.smoothedRTTMs + rttMs) / 8
	s.lastRTTUpdate = time.Now().UTC()
}

func (s *session) beginSnapshotRequest(partition string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingSnapshotParts == nil {
		s.pendingSnapshotParts = make(map[string]struct{})
	}
	if _, ok := s.pendingSnapshotParts[partition]; ok {
		return false
	}
	s.pendingSnapshotParts[partition] = struct{}{}
	return true
}

func (s *session) completeSnapshotRequest(partition string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pendingSnapshotParts, partition)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
