package cluster

import (
	"context"
	"fmt"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

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

func (m *Manager) removeQueuedTransientPacket(packet store.TransientPacket) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.retryQueue, packetCacheKey(packet.SourceNodeID, packet.PacketID))
}

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

func cloneTransientPacket(packet store.TransientPacket) store.TransientPacket {
	packet.Body = append([]byte(nil), packet.Body...)
	return packet
}

func storeSessionRefToCluster(ref store.SessionRef) *internalproto.ClusterSessionRef {
	if !ref.Valid() {
		return nil
	}
	return &internalproto.ClusterSessionRef{
		ServingNodeId: ref.ServingNodeID,
		SessionId:     ref.SessionID,
	}
}

func clusterSessionRefToStore(ref *internalproto.ClusterSessionRef) store.SessionRef {
	if ref == nil {
		return store.SessionRef{}
	}
	return store.SessionRef{
		ServingNodeID: ref.GetServingNodeId(),
		SessionID:     ref.GetSessionId(),
	}
}

func storeDeliveryModeToCluster(mode store.DeliveryMode) internalproto.ClusterDeliveryMode {
	switch mode {
	case store.DeliveryModeRouteRetry:
		return internalproto.ClusterDeliveryMode_CLUSTER_DELIVERY_MODE_ROUTE_RETRY
	default:
		return internalproto.ClusterDeliveryMode_CLUSTER_DELIVERY_MODE_BEST_EFFORT
	}
}

func clusterDeliveryModeToStore(mode internalproto.ClusterDeliveryMode) store.DeliveryMode {
	switch mode {
	case internalproto.ClusterDeliveryMode_CLUSTER_DELIVERY_MODE_ROUTE_RETRY:
		return store.DeliveryModeRouteRetry
	default:
		return store.DeliveryModeBestEffort
	}
}

func packetCacheKey(sourceNodeID int64, packetID uint64) string {
	return fmt.Sprintf("%d:%d", sourceNodeID, packetID)
}

func maxInt64(v, fallback int64) int64 {
	if v < fallback {
		return fallback
	}
	return v
}

var _ interface {
	RouteTransientPacket(context.Context, store.TransientPacket) error
} = (*Manager)(nil)
