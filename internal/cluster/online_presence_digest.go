package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"google.golang.org/protobuf/proto"
)

type presenceRepairKey struct {
	nodeID int64
	full   bool
}

func (m *Manager) recordPresenceSync(update *internalproto.OnlinePresenceUpdate, sent bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &m.presenceSync
	mode := update.GetMode()
	if sent {
		s.SentPayloadBytes += uint64(proto.Size(update))
		switch mode {
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA:
			s.DeltaSent++
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD:
			s.FullSent++
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST:
			s.DigestSent++
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_REPAIR_REQUEST:
			s.RepairRequestsSent++
		}
	} else {
		s.ReceivedPayloadBytes += uint64(proto.Size(update))
		switch mode {
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DELTA:
			s.DeltaReceived++
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD:
			s.FullReceived++
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST:
			s.DigestReceived++
		case internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_REPAIR_REQUEST:
			s.RepairRequestsReceived++
		}
	}
}

// presenceDigestLocked 比较实际内容，不依赖 generation：丢失一条增量后
// 收到更新代数的另一条增量，也仍能检测内容缺失。字段按用户键排序，
// 本地和远端缓存构建相同的 canonical protobuf，不包含会话引用或时间戳。
func (m *Manager) presenceDigestLocked(nodeID int64) []byte {
	state := &internalproto.OnlinePresenceUpdate{OriginNodeId: nodeID, RuntimeEpoch: m.onlinePresenceEpochs[nodeID]}
	var users []store.UserKey
	if nodeID == m.cfg.NodeID {
		state.RuntimeEpoch = m.localRuntimeEpoch
		for user := range m.localOnlineSessions {
			users = append(users, user)
		}
	} else {
		for user, bucket := range m.onlinePresenceByUser {
			if _, ok := bucket[nodeID]; ok {
				users = append(users, user)
			}
		}
	}
	sortUserKeys(users)
	for _, user := range users {
		if nodeID == m.cfg.NodeID {
			bucket := m.localOnlineSessions[user]
			u := m.localLoggedInUsers[user]
			state.Items = append(state.Items, &internalproto.ClusterOnlineNodePresence{User: clusterUserRef(user), ServingNodeId: nodeID, SessionCount: int32(len(bucket)), TransportHint: localTransportHint(bucket)})
			state.LoggedInUsers = append(state.LoggedInUsers, &internalproto.ClusterLoggedInUser{NodeId: u.NodeID, UserId: u.UserID, Username: u.Username, LoginName: u.LoginName})
		} else {
			p := m.onlinePresenceByUser[user][nodeID]
			u := m.loggedInUsersByNode[nodeID][user]
			state.Items = append(state.Items, &internalproto.ClusterOnlineNodePresence{User: clusterUserRef(user), ServingNodeId: nodeID, SessionCount: p.SessionCount, TransportHint: p.TransportHint})
			state.LoggedInUsers = append(state.LoggedInUsers, &internalproto.ClusterLoggedInUser{NodeId: u.NodeID, UserId: u.UserID, Username: u.Username, LoginName: u.LoginName})
		}
	}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(state)
	hash := sha256.Sum256(encoded)
	return hash[:]
}

func (m *Manager) presenceControlLocked(mode internalproto.OnlinePresenceUpdateMode) *internalproto.OnlinePresenceUpdate {
	return &internalproto.OnlinePresenceUpdate{OriginNodeId: m.cfg.NodeID, RuntimeEpoch: m.localRuntimeEpoch, Generation: 1, ShardCount: onlinePresenceShardCount, Mode: mode}
}

// snapshotPresenceShardLocked 不清除 dirty 标记。定向修复或旧邻居的快照
// 不能吞掉尚未发给其他邻居的50ms增量。
func (m *Manager) snapshotPresenceShardLocked(index int) *internalproto.OnlinePresenceUpdate {
	users := make([]store.UserKey, 0, len(m.localPresenceUsersByShard[index]))
	for user := range m.localPresenceUsersByShard[index] {
		users = append(users, user)
	}
	sortUserKeys(users)
	return m.buildOnlinePresenceUpdateLocked(internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_AUTHORITATIVE_SHARD, index, users)
}

func (m *Manager) repairOnlinePresenceShard(index int, fullRound bool) {
	binding := m.MeshRuntime()
	if binding == nil {
		return
	}
	m.mu.Lock()
	full := m.snapshotPresenceShardLocked(index)
	digest := m.presenceControlLocked(internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST)
	if index == 0 {
		digest.StateDigest = m.presenceDigestLocked(m.cfg.NodeID)
	}
	m.mu.Unlock()
	for nodeID := range binding.TopologyStore().Snapshot().Nodes {
		if nodeID <= 0 || nodeID == m.cfg.NodeID {
			continue
		}
		if !fullRound && binding.runtime.SupportsPresenceDigest(nodeID) {
			if index == 0 {
				m.sendOnlinePresence(nodeID, digest)
			}
		} else {
			m.sendOnlinePresence(nodeID, full)
		}
	}
}

func (m *Manager) handlePresenceControl(update *internalproto.OnlinePresenceUpdate) error {
	mode := update.GetMode()
	if update.OriginNodeId <= 0 || update.RuntimeEpoch == 0 || update.Generation != 1 ||
		update.ShardCount != onlinePresenceShardCount || update.ShardIndex != 0 ||
		len(update.Items)+len(update.LoggedInUsers)+len(update.RemovedUsers) != 0 ||
		(mode == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST && len(update.StateDigest) != sha256.Size) ||
		(mode == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_REPAIR_REQUEST && len(update.StateDigest) != 0) {
		return errors.New("invalid online presence control")
	}
	m.mu.Lock()
	if update.OriginNodeId == m.cfg.NodeID || update.RuntimeEpoch < m.currentRuntimeEpochForNodeLocked(update.OriginNodeId) {
		m.mu.Unlock()
		return nil
	}
	if mode == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_DIGEST && bytes.Equal(update.StateDigest, m.presenceDigestLocked(update.OriginNodeId)) {
		m.mu.Unlock()
		return nil
	}
	if m.pendingPresenceRepairs == nil {
		m.pendingPresenceRepairs = make(map[presenceRepairKey]struct{})
	}
	key := presenceRepairKey{nodeID: update.OriginNodeId, full: mode == internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_REPAIR_REQUEST}
	m.pendingPresenceRepairs[key] = struct{}{}
	m.mu.Unlock()
	m.wakePresenceLoop()
	return nil
}

// 读取任务只入有界邮箱，回复在 presenceLoop 中发送，避免拥塞 socket
// 阻塞集群入站消息分发。相同节点/任务在一次 flush 中只处理一次。
func (m *Manager) flushPresenceRepairs() {
	m.mu.Lock()
	pending := m.pendingPresenceRepairs
	m.pendingPresenceRepairs = nil
	m.mu.Unlock()
	for key := range pending {
		if key.full {
			m.startFullPresenceRepair(key.nodeID)
		} else {
			m.mu.Lock()
			update := m.presenceControlLocked(internalproto.OnlinePresenceUpdateMode_ONLINE_PRESENCE_UPDATE_MODE_REPAIR_REQUEST)
			m.mu.Unlock()
			m.sendOnlinePresence(key.nodeID, update)
		}
	}
}

// 完整修复独立于50ms增量循环；拥塞邻居不能让16次发送逐次耗尽超时，
// 阻塞其他节点的在线变化。每目标最多一个任务，全部分片共用10秒预算，
// 失败后等待下一次摘要再校验，不累积旧快照或额外重试队列。
func (m *Manager) startFullPresenceRepair(nodeID int64) {
	m.mu.Lock()
	if m.ctx == nil || m.ctx.Err() != nil || m.presenceFullSending[nodeID] {
		m.mu.Unlock()
		return
	}
	if m.presenceFullSending == nil {
		m.presenceFullSending = make(map[int64]bool)
	}
	m.presenceFullSending[nodeID] = true
	ctx, cancel := context.WithTimeout(m.ctx, writeWait)
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		defer cancel()
		defer func() {
			m.mu.Lock()
			delete(m.presenceFullSending, nodeID)
			m.mu.Unlock()
		}()
		for index := 0; index < onlinePresenceShardCount && ctx.Err() == nil; index++ {
			m.mu.Lock()
			update := m.snapshotPresenceShardLocked(index)
			m.mu.Unlock()
			if err := m.routeMeshPresenceUpdate(ctx, nodeID, update); err != nil {
				return
			}
			m.recordPresenceSync(update, true)
		}
	}()
}
