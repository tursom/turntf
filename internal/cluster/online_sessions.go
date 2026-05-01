package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// RegisterLocalSession 注册一个本地在线会话，并广播在线状态更新。
func (m *Manager) RegisterLocalSession(session store.OnlineSession) {
	if m == nil || !session.SessionRef.Valid() {
		return
	}
	if session.SessionRef.ServingNodeID == 0 {
		session.SessionRef.ServingNodeID = m.cfg.NodeID
	}
	if session.SessionRef.ServingNodeID != m.cfg.NodeID || session.User.Validate() != nil {
		return
	}

	m.mu.Lock()
	bucket := m.localOnlineSessions[session.User]
	if bucket == nil {
		bucket = make(map[string]store.OnlineSession)
		m.localOnlineSessions[session.User] = bucket
	}
	current, exists := bucket[session.SessionRef.SessionID]
	if exists && current == session {
		m.mu.Unlock()
		return
	}
	bucket[session.SessionRef.SessionID] = session
	m.refreshLocalPresenceLocked(session.User)
	m.mu.Unlock()

	m.broadcastOnlinePresence()
}

// UnregisterLocalSession 注销一个本地在线会话，并广播在线状态更新。
func (m *Manager) UnregisterLocalSession(user store.UserKey, sessionRef store.SessionRef) {
	if m == nil || !sessionRef.Valid() || user.Validate() != nil {
		return
	}

	m.mu.Lock()
	bucket := m.localOnlineSessions[user]
	if bucket == nil {
		m.mu.Unlock()
		return
	}
	if _, ok := bucket[sessionRef.SessionID]; !ok {
		m.mu.Unlock()
		return
	}
	delete(bucket, sessionRef.SessionID)
	if len(bucket) == 0 {
		delete(m.localOnlineSessions, user)
	}
	m.refreshLocalPresenceLocked(user)
	m.mu.Unlock()

	m.broadcastOnlinePresence()
}

// QueryOnlineUserPresence 查询指定用户在集群中的在线状态。
// 返回所有已知的服务节点上该用户的存在信息。
func (m *Manager) QueryOnlineUserPresence(_ context.Context, user store.UserKey) ([]store.OnlineNodePresence, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: cluster manager is not configured", app.ErrServiceUnavailable)
	}
	if err := user.Validate(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]store.OnlineNodePresence, 0)
	for _, presence := range m.onlinePresenceByUser[user] {
		items = append(items, presence)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ServingNodeID < items[j].ServingNodeID
	})
	return items, nil
}

// ResolveUserSessions 跨集群解析指定用户的所有在线会话。
//
// 解析策略：
//  1. 首先查询存在信息中已知的候选节点
//  2. 如果没有找到结果，则回退到查询所有已知节点
func (m *Manager) ResolveUserSessions(ctx context.Context, user store.UserKey) ([]store.OnlineSession, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: cluster manager is not configured", app.ErrServiceUnavailable)
	}
	if err := user.Validate(); err != nil {
		return nil, err
	}

	candidates := m.presenceCandidateNodeIDs(user)
	results, lastErr, queried := m.resolveUserSessionsAcrossNodes(ctx, user, candidates, nil)
	if len(results) == 0 {
		results, lastErr, _ = m.resolveUserSessionsAcrossNodes(ctx, user, m.allKnownNodeIDs(), queried)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].SessionRef.ServingNodeID != results[j].SessionRef.ServingNodeID {
			return results[i].SessionRef.ServingNodeID < results[j].SessionRef.ServingNodeID
		}
		return results[i].SessionRef.SessionID < results[j].SessionRef.SessionID
	})
	if len(results) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return results, nil
}

// resolveUserSessionsAcrossNodes 在指定的候选节点列表中解析用户会话。
// skip参数包含已查询过的节点。
func (m *Manager) resolveUserSessionsAcrossNodes(ctx context.Context, user store.UserKey, candidates []int64, skip map[int64]struct{}) ([]store.OnlineSession, error, map[int64]struct{}) {
	seenNodes := make(map[int64]struct{}, len(candidates))
	for nodeID := range skip {
		seenNodes[nodeID] = struct{}{}
	}
	seenSessions := make(map[string]struct{})
	results := make([]store.OnlineSession, 0)
	var lastErr error
	for _, nodeID := range candidates {
		if nodeID <= 0 {
			continue
		}
		if _, ok := seenNodes[nodeID]; ok {
			continue
		}
		seenNodes[nodeID] = struct{}{}
		items, err := m.resolveUserSessionsAtNode(ctx, nodeID, user)
		if err != nil {
			lastErr = err
			continue
		}
		for _, item := range items {
			key := fmt.Sprintf("%d:%s", item.SessionRef.ServingNodeID, item.SessionRef.SessionID)
			if _, ok := seenSessions[key]; ok {
				continue
			}
			seenSessions[key] = struct{}{}
			results = append(results, item)
		}
	}
	return results, lastErr, seenNodes
}

// resolveUserSessionsAtNode 在指定节点上解析用户会话。
// 本地节点直接返回本地会话；远程节点通过网格查询。
func (m *Manager) resolveUserSessionsAtNode(ctx context.Context, nodeID int64, user store.UserKey) ([]store.OnlineSession, error) {
	if nodeID == m.cfg.NodeID {
		return m.localUserSessions(user), nil
	}
	requestID, resultCh := m.beginResolveUserSessionsQuery()
	req := &internalproto.QueryResolveUserSessionsRequest{
		RequestId:     requestID,
		TargetNodeId:  nodeID,
		OriginNodeId:  m.cfg.NodeID,
		RemainingHops: defaultLoggedInUsersQueryMaxHops,
		User:          &internalproto.ClusterUserRef{NodeId: user.NodeID, UserId: user.UserID},
	}
	if m.MeshRuntime() == nil {
		m.cancelResolveUserSessionsQuery(requestID, meshNoRouteError(nodeID))
		return nil, meshNoRouteError(nodeID)
	}
	if err := m.routeMeshResolveUserSessionsRequest(ctx, req); err != nil {
		m.cancelResolveUserSessionsQuery(requestID, err)
		return nil, fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
	}

	timeoutCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, queryLoggedInUsersTimeout)
		defer cancel()
	}

	select {
	case <-timeoutCtx.Done():
		m.cancelResolveUserSessionsQuery(requestID, timeoutCtx.Err())
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: timed out resolving user sessions on node %d", app.ErrServiceUnavailable, nodeID)
		}
		return nil, timeoutCtx.Err()
	case result := <-resultCh:
		if result.err != nil {
			if errors.Is(result.err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("%w: timed out resolving user sessions on node %d", app.ErrServiceUnavailable, nodeID)
			}
			if errors.Is(result.err, errSessionClosed) {
				return nil, fmt.Errorf("%w: peer session closed while resolving user sessions on node %d", app.ErrServiceUnavailable, nodeID)
			}
			return nil, result.err
		}
		if err := clusterQueryErrorCode(result.response.GetErrorCode(), result.response.GetErrorMessage()); err != nil {
			return nil, err
		}
		return storeOnlineSessionsFromCluster(result.response.GetUser(), result.response.GetItems()), nil
	}
}

// beginResolveUserSessionsQuery 分配新的查询请求ID和响应通道。
func (m *Manager) beginResolveUserSessionsQuery() (uint64, chan resolveUserSessionsQueryResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextResolveSessionsQueryID++
	requestID := m.nextResolveSessionsQueryID
	ch := make(chan resolveUserSessionsQueryResult, 1)
	m.pendingResolveSessions[requestID] = ch
	return requestID, ch
}

// cancelResolveUserSessionsQuery 取消等待中的查询。
func (m *Manager) cancelResolveUserSessionsQuery(requestID uint64, err error) {
	m.mu.Lock()
	ch, ok := m.pendingResolveSessions[requestID]
	if ok {
		delete(m.pendingResolveSessions, requestID)
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- resolveUserSessionsQueryResult{err: err}:
	default:
	}
	close(ch)
}

// resolveResolveUserSessionsQuery 完成等待中的查询，发送结果。
func (m *Manager) resolveResolveUserSessionsQuery(requestID uint64, result resolveUserSessionsQueryResult) bool {
	m.mu.Lock()
	ch, ok := m.pendingResolveSessions[requestID]
	if ok {
		delete(m.pendingResolveSessions, requestID)
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	ch <- result
	close(ch)
	return true
}

// presenceCandidateNodeIDs 返回在在线存在信息中已知的候选节点ID列表。
func (m *Manager) presenceCandidateNodeIDs(user store.UserKey) []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return presenceNodeIDsLocked(m.onlinePresenceByUser[user])
}

// allKnownNodeIDs 返回所有已知节点ID的列表。
func (m *Manager) allKnownNodeIDs() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := map[int64]struct{}{m.cfg.NodeID: {}}
	for nodeID := range m.peers {
		if nodeID > 0 {
			seen[nodeID] = struct{}{}
		}
	}
	for _, peer := range m.configuredPeers {
		if peer != nil && peer.nodeID > 0 {
			seen[peer.nodeID] = struct{}{}
		}
	}
	for _, peer := range m.discoveredPeers {
		if peer != nil && peer.nodeID > 0 {
			seen[peer.nodeID] = struct{}{}
		}
	}
	out := make([]int64, 0, len(seen))
	for nodeID := range seen {
		out = append(out, nodeID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// localUserSessions 返回本地指定用户的所有在线会话。
func (m *Manager) localUserSessions(user store.UserKey) []store.OnlineSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneLocalSessionsLocked(m.localOnlineSessions[user])
}

// localPresenceSessions 返回所有活跃的本地会话（用于广播在线状态）。
func (m *Manager) localPresenceSessions() []*session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*session, 0)
	for _, peer := range m.peers {
		if peer == nil {
			continue
		}
		for _, sess := range peer.sessions {
			if sess != nil && !sess.isClosed() {
				out = append(out, sess)
			}
		}
	}
	return out
}

// broadcastOnlinePresence 构建并广播当前的在线状态快照。
func (m *Manager) broadcastOnlinePresence() {
	if m == nil || m.ctx == nil {
		return
	}
	snapshot := m.buildOnlinePresenceSnapshot()
	if snapshot == nil {
		return
	}
	m.forwardOnlinePresence(snapshot, 0)
}

// presenceLoop 是在线状态广播和断开连接怀疑清理的后台循环。
func (m *Manager) presenceLoop() {
	defer m.wg.Done()

	antiEntropyTicker := time.NewTicker(membershipUpdateInterval)
	defer antiEntropyTicker.Stop()
	suspicionTicker := time.NewTicker(disconnectSuspicionSweepInterval)
	defer suspicionTicker.Stop()

	m.broadcastOnlinePresence()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-antiEntropyTicker.C:
			m.broadcastOnlinePresence()
		case <-suspicionTicker.C:
			m.expireDisconnectSuspicions(time.Now().UTC())
		}
	}
}

// forwardOnlinePresence 将在线状态快照转发给所有本地活跃会话。
func (m *Manager) forwardOnlinePresence(snapshot *internalproto.OnlinePresenceSnapshot, excludePeerNodeID int64) {
	if m == nil || snapshot == nil {
		return
	}
	for _, sess := range m.localPresenceSessions() {
		if sess == nil || sess.peerID == excludePeerNodeID {
			continue
		}
		m.sendOnlinePresence(sess, snapshot)
	}
}

// sendOnlinePresence 向单个会话发送在线状态快照。
func (m *Manager) sendOnlinePresence(sess *session, snapshot *internalproto.OnlinePresenceSnapshot) {
	if m == nil || sess == nil || snapshot == nil || sess.isClosed() {
		return
	}
	if m.MeshRuntime() == nil {
		return
	}
	if err := m.routeMeshPresenceUpdate(context.Background(), sess.peerID, snapshot); err != nil {
		m.logMeshForwardFailure("mesh_online_presence_forward_failed", sess, err, "failed to forward online presence snapshot over mesh")
		if errors.Is(err, mesh.ErrNoRoute) {
			m.retryOnlinePresenceForward(sess.peerID, snapshot)
		}
	}
}

// retryOnlinePresenceForward 延迟重试在线状态快照的转发。
func (m *Manager) retryOnlinePresenceForward(targetNodeID int64, snapshot *internalproto.OnlinePresenceSnapshot) {
	if m == nil || targetNodeID <= 0 || snapshot == nil || m.ctx == nil {
		return
	}
	cloned := cloneOnlinePresenceSnapshot(snapshot)
	if cloned == nil {
		return
	}
	time.AfterFunc(100*time.Millisecond, func() {
		if m == nil || m.ctx == nil || m.ctx.Err() != nil {
			return
		}
		if err := m.routeMeshPresenceUpdate(context.Background(), targetNodeID, cloned); err != nil && !errors.Is(err, mesh.ErrNoRoute) {
			m.logDebug("mesh_online_presence_retry_failed").
				Int64("target_node_id", targetNodeID).
				Err(err).
				Msg("delayed retry for online presence snapshot failed")
		}
	})
}

// buildOnlinePresenceSnapshot 构建包含本地在线状态和已登录用户的快照。
func (m *Manager) buildOnlinePresenceSnapshot() *internalproto.OnlinePresenceSnapshot {
	if m == nil {
		return nil
	}
	loggedInUsers, err := m.snapshotLocalLoggedInUsers()
	if err != nil {
		m.logWarn("online_presence_snapshot_logged_in_users_unavailable", err).
			Msg("skipping authoritative online presence snapshot because logged-in users snapshot could not be built")
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.onlinePresenceGeneration++
	items := make([]*internalproto.ClusterOnlineNodePresence, 0, len(m.localOnlineSessions))
	for user, bucket := range m.localOnlineSessions {
		if len(bucket) == 0 {
			continue
		}
		items = append(items, &internalproto.ClusterOnlineNodePresence{
			User:          &internalproto.ClusterUserRef{NodeId: user.NodeID, UserId: user.UserID},
			ServingNodeId: m.cfg.NodeID,
			SessionCount:  int32(len(bucket)),
			TransportHint: localTransportHint(bucket),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].GetUser().GetNodeId() != items[j].GetUser().GetNodeId() {
			return items[i].GetUser().GetNodeId() < items[j].GetUser().GetNodeId()
		}
		return items[i].GetUser().GetUserId() < items[j].GetUser().GetUserId()
	})
	return &internalproto.OnlinePresenceSnapshot{
		OriginNodeId:  m.cfg.NodeID,
		Generation:    m.onlinePresenceGeneration,
		RuntimeEpoch:  m.localRuntimeEpoch,
		Items:         items,
		LoggedInUsers: clusterLoggedInUsers(loggedInUsers),
	}
}

// applyOnlinePresenceSnapshot 应用从远程节点接收的在线状态快照。
//
// 纪元（epoch）检查确保不会用过期或陈旧的数据覆盖当前状态：
//   - 如果纪元小于当前已知纪元，拒绝更新
//   - 如果纪元变化，清除该节点的临时状态
//   - 如果generation不更新，跳过重复快照
func (m *Manager) applyOnlinePresenceSnapshot(snapshot *internalproto.OnlinePresenceSnapshot) bool {
	if m == nil || snapshot == nil || snapshot.GetOriginNodeId() <= 0 || snapshot.GetRuntimeEpoch() == 0 {
		return false
	}
	if snapshot.GetOriginNodeId() == m.cfg.NodeID {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	originNodeID := snapshot.GetOriginNodeId()
	runtimeEpoch := snapshot.GetRuntimeEpoch()
	currentEpoch := m.currentRuntimeEpochForNodeLocked(originNodeID)
	if currentEpoch > 0 && runtimeEpoch < currentEpoch {
		return false
	}
	appliedEpoch := m.onlinePresenceEpochs[originNodeID]
	if appliedEpoch > 0 && runtimeEpoch < appliedEpoch {
		return false
	}
	if runtimeEpoch == appliedEpoch && m.onlinePresenceOrigins[originNodeID] >= snapshot.GetGeneration() {
		return false
	}
	if runtimeEpoch > currentEpoch {
		m.remoteRuntimeEpochs[originNodeID] = runtimeEpoch
	}
	if appliedEpoch > 0 && runtimeEpoch != appliedEpoch {
		m.clearEphemeralStateForNodeLocked(originNodeID, 0)
	}
	clearPresenceForNodeLocked(m.onlinePresenceByUser, originNodeID)
	delete(m.loggedInUsersByNode, originNodeID)
	for _, item := range snapshot.GetItems() {
		if item == nil || item.GetUser() == nil {
			continue
		}
		user := store.UserKey{NodeID: item.GetUser().GetNodeId(), UserID: item.GetUser().GetUserId()}
		if user.Validate() != nil {
			continue
		}
		servingNodeID := item.GetServingNodeId()
		if servingNodeID <= 0 {
			servingNodeID = snapshot.GetOriginNodeId()
		}
		if servingNodeID != snapshot.GetOriginNodeId() || item.GetSessionCount() <= 0 {
			continue
		}
		bucket := m.onlinePresenceByUser[user]
		if bucket == nil {
			bucket = make(map[int64]store.OnlineNodePresence)
			m.onlinePresenceByUser[user] = bucket
		}
		bucket[servingNodeID] = store.OnlineNodePresence{
			User:          user,
			ServingNodeID: servingNodeID,
			SessionCount:  item.GetSessionCount(),
			TransportHint: strings.TrimSpace(item.GetTransportHint()),
		}
	}
	m.loggedInUsersByNode[originNodeID] = loggedInUsersFromCluster(snapshot.GetLoggedInUsers())
	m.onlinePresenceEpochs[originNodeID] = runtimeEpoch
	m.onlinePresenceOrigins[originNodeID] = snapshot.GetGeneration()
	m.clearDisconnectSuspicionsForNodeLocked(originNodeID, runtimeEpoch)
	return true
}

// refreshLocalPresenceLocked 根据本地会话状态更新在线存在信息。
func (m *Manager) refreshLocalPresenceLocked(user store.UserKey) {
	if m == nil {
		return
	}
	bucket := m.onlinePresenceByUser[user]
	localSessions := m.localOnlineSessions[user]
	if len(localSessions) == 0 {
		if bucket != nil {
			delete(bucket, m.cfg.NodeID)
			if len(bucket) == 0 {
				delete(m.onlinePresenceByUser, user)
			}
		}
		return
	}
	if bucket == nil {
		bucket = make(map[int64]store.OnlineNodePresence)
		m.onlinePresenceByUser[user] = bucket
	}
	bucket[m.cfg.NodeID] = store.OnlineNodePresence{
		User:          user,
		ServingNodeID: m.cfg.NodeID,
		SessionCount:  int32(len(localSessions)),
		TransportHint: localTransportHint(localSessions),
	}
}

// presenceNodeIDsLocked 从存在信息桶中提取节点ID列表。
func presenceNodeIDsLocked(bucket map[int64]store.OnlineNodePresence) []int64 {
	if len(bucket) == 0 {
		return nil
	}
	out := make([]int64, 0, len(bucket))
	for nodeID := range bucket {
		out = append(out, nodeID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// cloneLocalSessionsLocked 克隆本地会话桶。
func cloneLocalSessionsLocked(bucket map[string]store.OnlineSession) []store.OnlineSession {
	if len(bucket) == 0 {
		return nil
	}
	items := make([]store.OnlineSession, 0, len(bucket))
	for _, session := range bucket {
		items = append(items, session)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].SessionRef.SessionID < items[j].SessionRef.SessionID
	})
	return items
}

// localTransportHint 返回本地会话桶的传输提示。
// 如果所有会话使用同一传输则返回该传输名，否则返回"mixed"。
func localTransportHint(bucket map[string]store.OnlineSession) string {
	hint := ""
	for _, session := range bucket {
		if hint == "" {
			hint = session.Transport
			continue
		}
		if hint != session.Transport {
			return "mixed"
		}
	}
	return hint
}

// clearPresenceForNodeLocked 从所有用户的存在信息中移除指定服务节点的条目。
func clearPresenceForNodeLocked(presenceByUser map[store.UserKey]map[int64]store.OnlineNodePresence, servingNodeID int64) {
	for user, bucket := range presenceByUser {
		delete(bucket, servingNodeID)
		if len(bucket) == 0 {
			delete(presenceByUser, user)
		}
	}
}

// cloneOnlinePresenceSnapshot 创建在线状态快照的深拷贝。
func cloneOnlinePresenceSnapshot(snapshot *internalproto.OnlinePresenceSnapshot) *internalproto.OnlinePresenceSnapshot {
	if snapshot == nil {
		return nil
	}
	items := make([]*internalproto.ClusterOnlineNodePresence, 0, len(snapshot.GetItems()))
	for _, item := range snapshot.GetItems() {
		if item == nil || item.GetUser() == nil {
			continue
		}
		items = append(items, &internalproto.ClusterOnlineNodePresence{
			User: &internalproto.ClusterUserRef{
				NodeId: item.GetUser().GetNodeId(),
				UserId: item.GetUser().GetUserId(),
			},
			ServingNodeId: item.GetServingNodeId(),
			SessionCount:  item.GetSessionCount(),
			TransportHint: item.GetTransportHint(),
		})
	}
	return &internalproto.OnlinePresenceSnapshot{
		OriginNodeId:  snapshot.GetOriginNodeId(),
		Generation:    snapshot.GetGeneration(),
		RuntimeEpoch:  snapshot.GetRuntimeEpoch(),
		Items:         items,
		LoggedInUsers: clusterLoggedInUsers(loggedInUsersFromCluster(snapshot.GetLoggedInUsers())),
	}
}

// clusterQueryErrorCode 将protobuf错误码转换为Go error。
func clusterQueryErrorCode(code, message string) error {
	if strings.TrimSpace(code) == "" {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		message = "cluster query failed"
	}
	switch code {
	case "invalid_request":
		return fmt.Errorf("%w: %s", store.ErrInvalidInput, message)
	case "not_found":
		return fmt.Errorf("%w: %s", store.ErrNotFound, message)
	case "forbidden":
		return fmt.Errorf("%w: %s", store.ErrForbidden, message)
	default:
		return fmt.Errorf("%w: %s", app.ErrServiceUnavailable, message)
	}
}

// storeOnlineSessionsFromCluster 将protobuf格式的集群会话引用转换为存储层在线会话。
func storeOnlineSessionsFromCluster(user *internalproto.ClusterUserRef, items []*internalproto.ClusterSessionRef) []store.OnlineSession {
	if user == nil || len(items) == 0 {
		return nil
	}
	userKey := store.UserKey{NodeID: user.GetNodeId(), UserID: user.GetUserId()}
	out := make([]store.OnlineSession, 0, len(items))
	for _, item := range items {
		if item == nil || item.GetServingNodeId() <= 0 || strings.TrimSpace(item.GetSessionId()) == "" {
			continue
		}
		out = append(out, store.OnlineSession{
			User: userKey,
			SessionRef: store.SessionRef{
				ServingNodeID: item.GetServingNodeId(),
				SessionID:     item.GetSessionId(),
			},
			Transport:        item.GetTransport(),
			TransientCapable: item.GetTransientCapable(),
		})
	}
	return out
}

// meshNoRouteError 返回无路由错误。
func meshNoRouteError(nodeID int64) error {
	return fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
}
