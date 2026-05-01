package cluster

import (
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// activateSession 尝试将指定会话设为其对等节点的活跃会话。
//
// 当节点间存在双向连接时，需要选择其中一个作为活跃会话。
// 选择规则：以较小的nodeID优先选择出站连接。
// 换言之，nodeID较小的节点主动发起的（出站）连接成为活跃会话。
// 如果现有会话与新会话方向相同且新会话不满足优先条件，则拒绝新会话。
func (m *Manager) activateSession(sess *session) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return false
	}

	// 无活跃会话时直接接受
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
	// 同一会话再次激活：更新sessions映射
	if peer.active == sess {
		if peer.sessions == nil {
			peer.sessions = make(map[uint64]*session)
		}
		peer.sessions[sess.connectionID] = sess
		return true
	}

	// 双向连接冲突：较小的nodeID优先选择出站连接
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

// sessionDirection 返回会话的方向字符串。
func sessionDirection(sess *session) string {
	if sess != nil && sess.outbound {
		return "outbound"
	}
	return "inbound"
}

// deactivateSession 取消指定会话的活跃状态。
// 如果该会话是trustedSession，则尝试切换到当前活跃会话；
// 否则清除时钟信任状态。
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
	// 如果该会话是可信会话，尝试转移到活跃会话
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

// enqueue 将信封放入会话的发送队列。
// 会优雅处理panic（向已关闭的通道发送）和Manager关闭的情况。
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

// close 关闭会话：标记为已关闭、通知所有等待中的时间同步请求、
// 关闭发送通道和底层传输连接。
func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	// 通知所有等待中的时间同步请求
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

// isClosed 返回会话是否已关闭。
func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// noteRemoteOriginProgress 记录对等节点报告的各原始节点事件进度。
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

// noteRemoteOriginEvent 记录单一对等节点报告的单个原始节点事件ID。
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

// remoteOriginEventID 返回对等节点报告的指定原始节点的最新事件ID。
func (s *session) remoteOriginEventID(originNodeID int64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteOriginProgress[originNodeID]
}

// remoteOriginProgressSnapshot 返回对等节点报告的原始节点进度的快照副本。
func (s *session) remoteOriginProgressSnapshot() map[int64]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	progress := make(map[int64]uint64, len(s.remoteOriginProgress))
	for originNodeID, eventID := range s.remoteOriginProgress {
		progress[originNodeID] = eventID
	}
	return progress
}

// beginPendingPull 为指定原始节点开始一个PullEvents请求。
// 如果已有等待中的Pull请求则返回false。
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

// hasPendingPull 返回是否有指定原始节点的等待中Pull请求。
func (s *session) hasPendingPull(originNodeID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pendingPulls[originNodeID]
	return ok
}

// hasPendingPulls 返回是否有任何等待中的Pull请求。
func (s *session) hasPendingPulls() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingPulls) > 0
}

// cancelPendingPull 取消指定请求ID的等待中Pull请求。
func (s *session) cancelPendingPull(originNodeID int64, requestID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.pendingPulls[originNodeID]
	if ok && state.RequestID == requestID {
		delete(s.pendingPulls, originNodeID)
	}
}

// completePendingPull 标记Pull请求已完成，返回是否匹配。
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

// beginBootstrap 标记引导流程已开始，返回是否是首次引导。
func (s *session) beginBootstrap() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bootstrapStarted {
		return false
	}
	s.bootstrapStarted = true
	return true
}

// markReplicationReady 标记会话已准备好接收复制事件。
// 首次就绪时会触发快照摘要标记。
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

// isReplicationReady 返回会话是否已准备好接收复制事件。
func (s *session) isReplicationReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicationReady
}

// startSyncLoop 启动会话的同步循环（仅一次）。
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

// beginTimeSync 为新的时间同步请求分配请求ID和响应通道。
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

// cancelTimeSync 取消等待中的时间同步请求，向其发送错误。
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

// resolveTimeSync 完成等待中的时间同步请求，向其发送结果。
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

// setClockOffset 设置此会话的时钟偏移。
func (s *session) setClockOffset(offsetMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clockOffsetMs = offsetMs
}

// clockOffset 返回此会话的时钟偏移。
func (s *session) clockOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clockOffsetMs
}

// observeRTT 使用指数加权移动平均（EWMA）更新平滑RTT和抖动值。
// smoothedRTTMs = 7/8 * old + 1/8 * new (权重8)
// jitterPenaltyMs = 3/4 * old + 1/4 * |new - old| (权重4)
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

// beginSnapshotRequest 标记快照分区请求已开始，返回是否重复请求。
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

// completeSnapshotRequest 标记快照分区请求已完成。
func (s *session) completeSnapshotRequest(partition string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pendingSnapshotParts, partition)
}

// minInt 返回两个整数中的较小值。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// absInt64 返回int64的绝对值。
func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
