package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// queueTransientPacket 将瞬时数据包放入重试队列，等待投递或重试。
// 如果已在队列中则保留原有的尝试次数和时间信息。
func (m *Manager) queueTransientPacket(packet store.TransientPacket) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := packetCacheKey(packet.SourceNodeID, packet.PacketID)
	now := time.Now().UTC()
	item := queuedPacket{
		packet:      cloneTransientPacket(packet),
		queuedAt:    now,
		nextAttempt: now.Add(routeRetryInterval),
	}
	if current, ok := m.retryQueue[key]; ok {
		item.attempts = current.attempts
		item.queuedAt = current.queuedAt
		item.nextAttempt = current.nextAttempt
	}
	m.retryQueue[key] = item
	addPacketLogFields(m.logInfo("transient_packet_queued"), packet).
		Int("attempt", item.attempts).
		Msg("queued transient packet for retry")
}

// removeQueuedTransientPacket 从重试队列中移除数据包。
func (m *Manager) removeQueuedTransientPacket(packet store.TransientPacket) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.retryQueue, packetCacheKey(packet.SourceNodeID, packet.PacketID))
}

// retryTransientPackets 重试所有到期的队列数据包。
//
// 淘汰策略：
//   - TTL过期（routeRetryTTL=3秒）：丢弃
//   - 重试次数达到10次：丢弃
//   - 下一次尝试时间尚未到达：跳过
func (m *Manager) retryTransientPackets() {
	m.mu.Lock()
	if len(m.retryQueue) == 0 {
		m.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	items := make([]queuedPacket, 0, len(m.retryQueue))
	for key, item := range m.retryQueue {
		if now.Sub(item.queuedAt) > routeRetryTTL || item.attempts >= 10 {
			reason := "retry_limit"
			if now.Sub(item.queuedAt) > routeRetryTTL {
				reason = "retry_ttl_expired"
			}
			addPacketLogFields(m.logWarn("transient_packet_dropped", nil), item.packet).
				Int("attempt", item.attempts).
				Str("reason", reason).
				Msg("dropping transient packet from retry queue")
			delete(m.retryQueue, key)
			continue
		}
		if item.nextAttempt.After(now) {
			continue
		}
		item.attempts++
		item.nextAttempt = now.Add(routeRetryInterval)
		m.retryQueue[key] = item
		items = append(items, item)
	}
	m.mu.Unlock()
	for _, item := range items {
		addPacketLogFields(m.logDebug("transient_packet_retrying"), item.packet).
			Int("attempt", item.attempts).
			Msg("retrying transient packet route")
		m.routeOrQueueTransientPacket(m.ctx, item.packet)
	}
}

// deliverTransientLocal 将瞬时数据包投递给本节点的瞬态处理器。
func (m *Manager) deliverTransientLocal(packet store.TransientPacket) bool {
	m.mu.Lock()
	handler := m.transientHandler
	m.mu.Unlock()
	if handler == nil {
		addPacketLogFields(m.logWarn("transient_packet_dropped", nil), packet).
			Str("reason", "no_local_handler").
			Msg("dropping transient packet without local handler")
		return false
	}
	delivered := handler(packet)
	event := m.logInfo("transient_packet_delivered")
	if !delivered {
		event = m.logWarn("transient_packet_delivery_missed", nil)
	}
	addPacketLogFields(event, packet).
		Bool("delivered", delivered).
		Msg("processed local transient packet")
	return delivered
}

// ensurePeerLocked 确保peers映射中存在指定对等节点的peerState。
// 如果不存在则创建新的peerState。
func (m *Manager) ensurePeerLocked(peerID int64) *peerState {
	peer := m.peers[peerID]
	if peer == nil {
		peer = &peerState{
			sessions: make(map[uint64]*session),
		}
		m.peers[peerID] = peer
	}
	if peer.sessions == nil {
		peer.sessions = make(map[uint64]*session)
	}
	return peer
}

// transientPacketProto 将存储层的瞬时数据包转换为protobuf格式。
func transientPacketProto(packet store.TransientPacket) *internalproto.TransientPacket {
	return &internalproto.TransientPacket{
		PacketId:      packet.PacketID,
		SourceNodeId:  packet.SourceNodeID,
		TargetNodeId:  packet.TargetNodeID,
		Recipient:     &internalproto.ClusterUserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		Sender:        &internalproto.ClusterUserRef{NodeId: packet.Sender.NodeID, UserId: packet.Sender.UserID},
		Body:          packet.Body,
		DeliveryMode:  storeDeliveryModeToCluster(packet.DeliveryMode),
		TtlHops:       uint32(packet.TTLHops),
		TargetSession: storeSessionRefToCluster(packet.TargetSession),
	}
}

// transientPacketFromProto 将protobuf的瞬时数据包（及可选的转发信息）转换为存储层格式。
func transientPacketFromProto(packet *internalproto.TransientPacket, forwarded *mesh.ForwardedPacket) (store.TransientPacket, error) {
	if packet == nil {
		return store.TransientPacket{}, fmt.Errorf("transient packet cannot be empty")
	}
	if packet.Recipient == nil {
		return store.TransientPacket{}, fmt.Errorf("transient packet recipient cannot be empty")
	}
	if packet.Sender == nil {
		return store.TransientPacket{}, fmt.Errorf("transient packet sender cannot be empty")
	}
	transient := store.TransientPacket{
		PacketID:      packet.GetPacketId(),
		SourceNodeID:  packet.GetSourceNodeId(),
		TargetNodeID:  packet.GetTargetNodeId(),
		Recipient:     store.UserKey{NodeID: packet.Recipient.NodeId, UserID: packet.Recipient.UserId},
		Sender:        store.UserKey{NodeID: packet.Sender.NodeId, UserID: packet.Sender.UserId},
		Body:          packet.GetBody(),
		DeliveryMode:  clusterDeliveryModeToStore(packet.GetDeliveryMode()),
		TTLHops:       int32(packet.GetTtlHops()),
		TargetSession: clusterSessionRefToStore(packet.GetTargetSession()),
	}
	// 如果存在转发信息，使用转发后的路由元数据覆盖原始值
	if forwarded == nil {
		return transient, nil
	}
	transient.PacketID = forwarded.GetPacketId()
	transient.SourceNodeID = forwarded.GetSourceNodeId()
	transient.TargetNodeID = forwarded.GetTargetNodeId()
	transient.TTLHops = int32(forwarded.GetTtlHops())
	return transient, nil
}

// cloneTransientPacket 创建瞬时数据包的深拷贝（复制Body切片）。
func cloneTransientPacket(packet store.TransientPacket) store.TransientPacket {
	packet.Body = append([]byte(nil), packet.Body...)
	return packet
}

// storeSessionRefToCluster 将存储层的会话引用转换为protobuf格式。
func storeSessionRefToCluster(ref store.SessionRef) *internalproto.ClusterSessionRef {
	if !ref.Valid() {
		return nil
	}
	return &internalproto.ClusterSessionRef{
		ServingNodeId: ref.ServingNodeID,
		SessionId:     ref.SessionID,
	}
}

// clusterSessionRefToStore 将protobuf的会话引用转换为存储层格式。
func clusterSessionRefToStore(ref *internalproto.ClusterSessionRef) store.SessionRef {
	if ref == nil {
		return store.SessionRef{}
	}
	return store.SessionRef{
		ServingNodeID: ref.GetServingNodeId(),
		SessionID:     ref.GetSessionId(),
	}
}

// storeDeliveryModeToCluster 将存储层的投递模式转换为protobuf格式。
func storeDeliveryModeToCluster(mode store.DeliveryMode) internalproto.ClusterDeliveryMode {
	switch mode {
	case store.DeliveryModeRouteRetry:
		return internalproto.ClusterDeliveryMode_CLUSTER_DELIVERY_MODE_ROUTE_RETRY
	default:
		return internalproto.ClusterDeliveryMode_CLUSTER_DELIVERY_MODE_BEST_EFFORT
	}
}

// clusterDeliveryModeToStore 将protobuf的投递模式转换为存储层格式。
func clusterDeliveryModeToStore(mode internalproto.ClusterDeliveryMode) store.DeliveryMode {
	switch mode {
	case internalproto.ClusterDeliveryMode_CLUSTER_DELIVERY_MODE_ROUTE_RETRY:
		return store.DeliveryModeRouteRetry
	default:
		return store.DeliveryModeBestEffort
	}
}

// packetCacheKey 为数据包生成唯一的队列键：(sourceNodeID:packetID)。
func packetCacheKey(sourceNodeID int64, packetID uint64) string {
	return fmt.Sprintf("%d:%d", sourceNodeID, packetID)
}

// maxInt64 返回两个int64中的较大值。
func maxInt64(v, fallback int64) int64 {
	if v < fallback {
		return fallback
	}
	return v
}

// 编译时检查Manager实现了RouteTransientPacket方法。
var _ interface {
	RouteTransientPacket(context.Context, store.TransientPacket) error
} = (*Manager)(nil)
