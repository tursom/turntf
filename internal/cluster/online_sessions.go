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
	"google.golang.org/protobuf/proto"
)

const (
	onlinePresenceShardCount         = 16
	onlinePresenceDeltaFlushInterval = 50 * time.Millisecond
	onlinePresenceRepairInterval     = 500 * time.Millisecond
	onlinePresenceDeltaBatchSize     = 256
)

type onlinePresenceShardKey struct {
	originNodeID int64
	shardIndex   uint32
}

func newOnlinePresenceShardUserSets() [onlinePresenceShardCount]map[store.UserKey]struct{} {
	var shards [onlinePresenceShardCount]map[store.UserKey]struct{}
	for index := range shards {
		shards[index] = make(map[store.UserKey]struct{})
	}
	return shards
}

func onlinePresenceShardIndex(user store.UserKey) int {
	hash := uint64(user.NodeID)*11400714819323198485 ^ (uint64(user.UserID) + 0x9e3779b97f4a7c15)
	return int(hash % onlinePresenceShardCount)
}

// RegisterLocalSession 注册一个本地在线会话，并排队广播在线状态增量。
func (m *Manager) RegisterLocalSession(session store.OnlineSession, loggedInUser app.LoggedInUserSummary) {
	if m == nil || !session.SessionRef.Valid() {
		return
	}
	if session.SessionRef.ServingNodeID == 0 {
		session.SessionRef.ServingNodeID = m.cfg.NodeID
	}
	if session.SessionRef.ServingNodeID != m.cfg.NodeID || session.User.Validate() != nil ||
		loggedInUser.NodeID != session.User.NodeID || loggedInUser.UserID != session.User.UserID {
		return
	}
	loggedInUser.Username = strings.TrimSpace(loggedInUser.Username)
	loggedInUser.LoginName = strings.TrimSpace(loggedInUser.LoginName)
	session.Transport = strings.TrimSpace(session.Transport)
	if loggedInUser.Username == "" || session.Transport == "" {
		return
	}

	m.mu.Lock()
	bucket := m.localOnlineSessions[session.User]
	if bucket == nil {
		bucket = make(map[string]store.OnlineSession)
		m.localOnlineSessions[session.User] = bucket
	}
	current, exists := bucket[session.SessionRef.SessionID]
	if exists && current == session && m.localLoggedInUsers[session.User] == loggedInUser {
		m.mu.Unlock()
		return
	}
	bucket[session.SessionRef.SessionID] = session
	m.localLoggedInUsers[session.User] = loggedInUser
	shardIndex := onlinePresenceShardIndex(session.User)
	m.localPresenceUsersByShard[shardIndex][session.User] = struct{}{}
	m.dirtyPresenceUsersByShard[shardIndex][session.User] = struct{}{}
	m.refreshLocalPresenceLocked(session.User)
	m.mu.Unlock()

	m.wakePresenceLoop()
}

// UnregisterLocalSession 注销一个本地在线会话，并排队广播在线状态增量。
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
	shardIndex := onlinePresenceShardIndex(user)
	if len(bucket) == 0 {
		delete(m.localOnlineSessions, user)
		delete(m.localLoggedInUsers, user)
		delete(m.localPresenceUsersByShard[shardIndex], user)
	}
	m.dirtyPresenceUsersByShard[shardIndex][user] = struct{}{}
	m.refreshLocalPresenceLocked(user)
	m.mu.Unlock()

	m.wakePresenceLoop()
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

// wakePresenceLoop 通知后台循环刷新合并后的在线状态变化。
func (m *Manager) wakePresenceLoop() {
	if m == nil || m.presenceWake == nil {
		return
	}
	select {
	case m.presenceWake <- struct{}{}:
	default:
	}
}

// broadcastOnlinePresence 立即广播一个权威分片，用于邻接建立和连接怀疑自证。
// 其余分片仍由 500ms 轮转任务在 8 秒内补齐。
func (m *Manager) broadcastOnlinePresence() {
	m.broadcastAuthoritativePresenceShard(0)
}

// presenceLoop 是在线状态增量、权威分片校验和断开连接怀疑清理的后台循环。
func (m *Manager) presenceLoop() {
	defer m.wg.Done()

	repairTicker := time.NewTicker(onlinePresenceRepairInterval)
	defer repairTicker.Stop()
	suspicionTicker := time.NewTicker(disconnectSuspicionSweepInterval)
	defer suspicionTicker.Stop()
	var flushTimer *time.Timer
	var flushTimerC <-chan time.Time
	nextRepairShard := 0

	for {
		select {
		case <-m.ctx.Done():
			if flushTimer != nil {
				flushTimer.Stop()
			}
			return
		case <-m.presenceWake:
			if flushTimer == nil {
				flushTimer = time.NewTimer(onlinePresenceDeltaFlushInterval)
				flushTimerC = flushTimer.C
			}
		case <-flushTimerC:
			flushTimer = nil
			flushTimerC = nil
			m.flushOnlinePresenceDeltas()
		case <-repairTicker.C:
			m.broadcastAuthoritativePresenceShard(nextRepairShard)
			nextRepairShard = (nextRepairShard + 1) % onlinePresenceShardCount
		case <-suspicionTicker.C:
			m.expireDisconnectSuspicions(time.Now().UTC())
		}
	}
}

// flushOnlinePresenceDeltas 构建并广播所有待处理的用户增量。
func (m *Manager) flushOnlinePresenceDeltas() {
	for shardIndex := 0; shardIndex < onlinePresenceShardCount; shardIndex++ {
		for {
			update := m.takeOnlinePresenceDelta(shardIndex)
			if update == nil {
				break
			}
			m.forwardOnlinePresence(update)
		}
	}
}

// takeOnlinePresenceDelta 从指定分片取出一批合并后的用户变化。
func (m *Manager) takeOnlinePresenceDelta(shardIndex int) *internalproto.OnlinePresenceUpdate {
	if m == nil || shardIndex < 0 || shardIndex >= onlinePresenceShardCount {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dirty := m.dirtyPresenceUsersByShard[shardIndex]
	if len(dirty) == 0 {
		return nil
	}
	capacity := len(dirty)
	if capacity > onlinePresenceDeltaBatchSize {
		capacity = onlinePresenceDeltaBatchSize
	}
	users := make([]store.UserKey, 0, capacity)
	for user := range dirty {
		users = append(users, user)
		delete(dirty, user)
		if len(users) == onlinePresenceDeltaBatchSize {
			break
		}
	}
	sortUserKeys(users)
	return m.buildOnlinePresenceUpdateLocked(
		internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA,
		shardIndex,
		users,
	)
}

// broadcastAuthoritativePresenceShard 广播一个本地权威在线状态分片。
func (m *Manager) broadcastAuthoritativePresenceShard(shardIndex int) {
	if m == nil || m.ctx == nil || shardIndex < 0 || shardIndex >= onlinePresenceShardCount {
		return
	}
	update := m.takeAuthoritativePresenceShard(shardIndex)
	m.forwardOnlinePresence(update)
}

// takeAuthoritativePresenceShard 从本地索引构建一个权威在线状态分片。
func (m *Manager) takeAuthoritativePresenceShard(shardIndex int) *internalproto.OnlinePresenceUpdate {
	if m == nil || shardIndex < 0 || shardIndex >= onlinePresenceShardCount {
		return nil
	}
	m.mu.Lock()
	users := make([]store.UserKey, 0, len(m.localPresenceUsersByShard[shardIndex]))
	for user := range m.localPresenceUsersByShard[shardIndex] {
		users = append(users, user)
	}
	sortUserKeys(users)
	for user := range m.dirtyPresenceUsersByShard[shardIndex] {
		delete(m.dirtyPresenceUsersByShard[shardIndex], user)
	}
	update := m.buildOnlinePresenceUpdateLocked(
		internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD,
		shardIndex,
		users,
	)
	m.mu.Unlock()
	return update
}

// buildOnlinePresenceUpdateLocked 从当前本地状态构建指定用户集合的更新。
func (m *Manager) buildOnlinePresenceUpdateLocked(mode internalproto.OnlinePresenceUpdateMode, shardIndex int, users []store.UserKey) *internalproto.OnlinePresenceUpdate {
	m.localPresenceGenerations[shardIndex]++
	update := &internalproto.OnlinePresenceUpdate{
		OriginNodeId:  m.cfg.NodeID,
		RuntimeEpoch:  m.localRuntimeEpoch,
		Mode:          mode,
		ShardIndex:    uint32(shardIndex),
		ShardCount:    onlinePresenceShardCount,
		Generation:    m.localPresenceGenerations[shardIndex],
		Items:         make([]*internalproto.ClusterOnlineNodePresence, 0, len(users)),
		LoggedInUsers: make([]*internalproto.ClusterLoggedInUser, 0, len(users)),
	}
	for _, user := range users {
		bucket := m.localOnlineSessions[user]
		loggedInUser, online := m.localLoggedInUsers[user]
		if len(bucket) == 0 || !online {
			if mode == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA {
				update.RemovedUsers = append(update.RemovedUsers, clusterUserRef(user))
			}
			continue
		}
		update.Items = append(update.Items, &internalproto.ClusterOnlineNodePresence{
			User:          clusterUserRef(user),
			ServingNodeId: m.cfg.NodeID,
			SessionCount:  int32(len(bucket)),
			TransportHint: localTransportHint(bucket),
		})
		update.LoggedInUsers = append(update.LoggedInUsers, &internalproto.ClusterLoggedInUser{
			NodeId:    loggedInUser.NodeID,
			UserId:    loggedInUser.UserID,
			Username:  loggedInUser.Username,
			LoginName: loggedInUser.LoginName,
		})
	}
	return update
}

// forwardOnlinePresence 将在线状态更新分别路由到所有已知节点。
// 每个目标由 mesh 自身完成多跳转发，保证包来源始终是更新的 origin。
func (m *Manager) forwardOnlinePresence(update *internalproto.OnlinePresenceUpdate) {
	if m == nil || update == nil {
		return
	}
	binding := m.MeshRuntime()
	if binding == nil {
		return
	}
	for nodeID := range binding.TopologyStore().Snapshot().Nodes {
		if nodeID <= 0 || nodeID == m.cfg.NodeID {
			continue
		}
		m.sendOnlinePresence(nodeID, update)
	}
}

// sendOnlinePresence 向单个目标节点发送在线状态更新。
func (m *Manager) sendOnlinePresence(targetNodeID int64, update *internalproto.OnlinePresenceUpdate) {
	if m == nil || targetNodeID <= 0 || update == nil {
		return
	}
	if m.MeshRuntime() == nil {
		return
	}
	if err := m.routeMeshPresenceUpdate(context.Background(), targetNodeID, update); err != nil {
		m.logDebug("mesh_online_presence_forward_failed").
			Int64("target_node_id", targetNodeID).
			Err(err).
			Msg("failed to forward online presence update over mesh")
		if errors.Is(err, mesh.ErrNoRoute) {
			m.retryOnlinePresenceForward(targetNodeID, update)
		}
	}
}

// retryOnlinePresenceForward 延迟重试在线状态更新的转发。
func (m *Manager) retryOnlinePresenceForward(targetNodeID int64, update *internalproto.OnlinePresenceUpdate) {
	if m == nil || targetNodeID <= 0 || update == nil || m.ctx == nil {
		return
	}
	cloned := cloneOnlinePresenceUpdate(update)
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

// applyOnlinePresenceUpdate 校验并原子应用从远程节点接收的在线状态更新。
func (m *Manager) applyOnlinePresenceUpdate(update *internalproto.OnlinePresenceUpdate) (bool, error) {
	if m == nil {
		return false, nil
	}
	loggedInUsers, err := validateOnlinePresenceUpdate(update)
	if err != nil {
		return false, err
	}
	if update.GetOriginNodeId() == m.cfg.NodeID {
		return false, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	originNodeID := update.GetOriginNodeId()
	runtimeEpoch := update.GetRuntimeEpoch()
	currentEpoch := m.currentRuntimeEpochForNodeLocked(originNodeID)
	if currentEpoch > 0 && runtimeEpoch < currentEpoch {
		return false, nil
	}
	appliedEpoch := m.onlinePresenceEpochs[originNodeID]
	if appliedEpoch > 0 && runtimeEpoch < appliedEpoch {
		return false, nil
	}
	if runtimeEpoch > currentEpoch {
		m.remoteRuntimeEpochs[originNodeID] = runtimeEpoch
	}
	if appliedEpoch > 0 && runtimeEpoch != appliedEpoch {
		m.clearEphemeralStateForNodeLocked(originNodeID, 0)
	}
	shardKey := onlinePresenceShardKey{originNodeID: originNodeID, shardIndex: update.GetShardIndex()}
	if runtimeEpoch == appliedEpoch && m.onlinePresenceGenerations[shardKey] >= update.GetGeneration() {
		return false, nil
	}
	if update.GetMode() == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD {
		m.clearOnlinePresenceShardLocked(shardKey)
		if m.loggedInUsersByNode[originNodeID] == nil {
			m.loggedInUsersByNode[originNodeID] = make(map[store.UserKey]app.LoggedInUserSummary)
		}
	}
	for _, removed := range update.GetRemovedUsers() {
		m.removeRemotePresenceUserLocked(shardKey, store.UserKey{NodeID: removed.GetNodeId(), UserID: removed.GetUserId()})
	}
	for _, item := range update.GetItems() {
		user := store.UserKey{NodeID: item.GetUser().GetNodeId(), UserID: item.GetUser().GetUserId()}
		bucket := m.onlinePresenceByUser[user]
		if bucket == nil {
			bucket = make(map[int64]store.OnlineNodePresence)
			m.onlinePresenceByUser[user] = bucket
		}
		bucket[originNodeID] = store.OnlineNodePresence{
			User:          user,
			ServingNodeID: originNodeID,
			SessionCount:  item.GetSessionCount(),
			TransportHint: strings.TrimSpace(item.GetTransportHint()),
		}
		remoteUsers := m.loggedInUsersByNode[originNodeID]
		if remoteUsers == nil {
			remoteUsers = make(map[store.UserKey]app.LoggedInUserSummary)
			m.loggedInUsersByNode[originNodeID] = remoteUsers
		}
		remoteUsers[user] = loggedInUsers[user]
		usersInShard := m.onlinePresenceUsersByShard[shardKey]
		if usersInShard == nil {
			usersInShard = make(map[store.UserKey]struct{})
			m.onlinePresenceUsersByShard[shardKey] = usersInShard
		}
		usersInShard[user] = struct{}{}
	}
	m.onlinePresenceEpochs[originNodeID] = runtimeEpoch
	m.onlinePresenceGenerations[shardKey] = update.GetGeneration()
	m.clearDisconnectSuspicionsForNodeLocked(originNodeID, runtimeEpoch)
	return true, nil
}

func validateOnlinePresenceUpdate(update *internalproto.OnlinePresenceUpdate) (map[store.UserKey]app.LoggedInUserSummary, error) {
	if update == nil || update.GetOriginNodeId() <= 0 || update.GetRuntimeEpoch() == 0 || update.GetGeneration() == 0 {
		return nil, errors.New("online presence update identity is invalid")
	}
	if update.GetShardCount() != onlinePresenceShardCount || update.GetShardIndex() >= onlinePresenceShardCount {
		return nil, errors.New("online presence update shard is invalid")
	}
	mode := update.GetMode()
	if mode != internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA &&
		mode != internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD {
		return nil, errors.New("online presence update mode is invalid")
	}
	if mode == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD && len(update.GetRemovedUsers()) != 0 {
		return nil, errors.New("authoritative online presence shard cannot contain removals")
	}
	items := make(map[store.UserKey]struct{}, len(update.GetItems()))
	for _, item := range update.GetItems() {
		if item == nil || item.GetUser() == nil || item.GetServingNodeId() != update.GetOriginNodeId() ||
			item.GetSessionCount() <= 0 || strings.TrimSpace(item.GetTransportHint()) == "" {
			return nil, errors.New("online presence item is invalid")
		}
		user := store.UserKey{NodeID: item.GetUser().GetNodeId(), UserID: item.GetUser().GetUserId()}
		if user.Validate() != nil || onlinePresenceShardIndex(user) != int(update.GetShardIndex()) {
			return nil, errors.New("online presence item belongs to an invalid shard")
		}
		if _, exists := items[user]; exists {
			return nil, errors.New("online presence update contains duplicate items")
		}
		items[user] = struct{}{}
	}
	loggedInUsers := make(map[store.UserKey]app.LoggedInUserSummary, len(update.GetLoggedInUsers()))
	for _, item := range update.GetLoggedInUsers() {
		if item == nil || strings.TrimSpace(item.GetUsername()) == "" {
			return nil, errors.New("logged-in user summary is invalid")
		}
		user := store.UserKey{NodeID: item.GetNodeId(), UserID: item.GetUserId()}
		if user.Validate() != nil || onlinePresenceShardIndex(user) != int(update.GetShardIndex()) {
			return nil, errors.New("logged-in user summary belongs to an invalid shard")
		}
		if _, exists := loggedInUsers[user]; exists {
			return nil, errors.New("online presence update contains duplicate logged-in users")
		}
		loggedInUsers[user] = app.LoggedInUserSummary{
			NodeID:    user.NodeID,
			UserID:    user.UserID,
			Username:  strings.TrimSpace(item.GetUsername()),
			LoginName: strings.TrimSpace(item.GetLoginName()),
		}
	}
	if len(items) != len(loggedInUsers) {
		return nil, errors.New("online presence items and logged-in users do not match")
	}
	for user := range items {
		if _, exists := loggedInUsers[user]; !exists {
			return nil, errors.New("online presence item is missing its logged-in user summary")
		}
	}
	removedUsers := make(map[store.UserKey]struct{}, len(update.GetRemovedUsers()))
	for _, item := range update.GetRemovedUsers() {
		if item == nil {
			return nil, errors.New("removed online presence user is invalid")
		}
		user := store.UserKey{NodeID: item.GetNodeId(), UserID: item.GetUserId()}
		if user.Validate() != nil || onlinePresenceShardIndex(user) != int(update.GetShardIndex()) {
			return nil, errors.New("removed online presence user belongs to an invalid shard")
		}
		if _, exists := removedUsers[user]; exists {
			return nil, errors.New("online presence update contains duplicate removals")
		}
		if _, exists := items[user]; exists {
			return nil, errors.New("online presence update both upserts and removes a user")
		}
		removedUsers[user] = struct{}{}
	}
	return loggedInUsers, nil
}

func (m *Manager) clearOnlinePresenceShardLocked(shardKey onlinePresenceShardKey) {
	for user := range m.onlinePresenceUsersByShard[shardKey] {
		m.removeRemotePresenceUserLocked(shardKey, user)
	}
	delete(m.onlinePresenceUsersByShard, shardKey)
}

func (m *Manager) removeRemotePresenceUserLocked(shardKey onlinePresenceShardKey, user store.UserKey) {
	if bucket := m.onlinePresenceByUser[user]; bucket != nil {
		delete(bucket, shardKey.originNodeID)
		if len(bucket) == 0 {
			delete(m.onlinePresenceByUser, user)
		}
	}
	if users := m.loggedInUsersByNode[shardKey.originNodeID]; users != nil {
		delete(users, user)
	}
	if users := m.onlinePresenceUsersByShard[shardKey]; users != nil {
		delete(users, user)
	}
}

func sortUserKeys(users []store.UserKey) {
	sort.Slice(users, func(i, j int) bool {
		if users[i].NodeID != users[j].NodeID {
			return users[i].NodeID < users[j].NodeID
		}
		return users[i].UserID < users[j].UserID
	})
}

func clusterUserRef(user store.UserKey) *internalproto.ClusterUserRef {
	return &internalproto.ClusterUserRef{NodeId: user.NodeID, UserId: user.UserID}
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

// cloneOnlinePresenceUpdate 创建在线状态更新的深拷贝。
func cloneOnlinePresenceUpdate(update *internalproto.OnlinePresenceUpdate) *internalproto.OnlinePresenceUpdate {
	if update == nil {
		return nil
	}
	cloned, _ := proto.Clone(update).(*internalproto.OnlinePresenceUpdate)
	return cloned
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
