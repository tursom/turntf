package cluster

import (
	"context"
	"fmt"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) handleRoutingUpdate(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	update := envelope.GetRoutingUpdate()
	if update == nil {
		return fmt.Errorf("routing update body cannot be empty")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	peer := m.ensurePeerLocked(sess.peerID)
	if peer.routeAdverts == nil {
		peer.routeAdverts = make(map[int64]routeAdvertisement)
	}
	peer.routeAdverts = make(map[int64]routeAdvertisement, len(update.Routes))
	for _, item := range update.Routes {
		if item == nil || item.DestinationNodeId <= 0 {
			continue
		}
		peer.routeAdverts[item.DestinationNodeId] = routeAdvertisement{
			destinationNodeID: item.DestinationNodeId,
			reachable:         item.Reachable,
			totalCostMs:       int64(item.TotalCostMs),
			residualCostMs:    int64(item.ResidualCostMs),
			residualJitterMs:  int64(item.ResidualJitterMs),
		}
	}
	m.recomputeRoutesLocked()
	m.logSessionDebug("routing_update_received", sess).
		Int("route_count", len(update.Routes)).
		Uint64("generation", update.Generation).
		Msg("routing update received")
	return nil
}

func (m *Manager) handleTransientPacket(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	body := envelope.GetTransientPacket()
	if body == nil {
		return fmt.Errorf("transient packet body cannot be empty")
	}
	if body.Recipient == nil {
		return fmt.Errorf("transient packet recipient cannot be empty")
	}
	if body.Sender == nil {
		return fmt.Errorf("transient packet sender cannot be empty")
	}
	packet := store.TransientPacket{
		PacketID:     body.PacketId,
		SourceNodeID: body.SourceNodeId,
		TargetNodeID: body.TargetNodeId,
		Recipient:    store.UserKey{NodeID: body.Recipient.NodeId, UserID: body.Recipient.UserId},
		Sender:       store.UserKey{NodeID: body.Sender.NodeId, UserID: body.Sender.UserId},
		Body:         append([]byte(nil), body.Body...),
		DeliveryMode: clusterDeliveryModeToStore(body.DeliveryMode),
		TTLHops:      int32(body.TtlHops),
	}
	m.logSessionDebug("transient_packet_received", sess).
		Uint64("packet_id", packet.PacketID).
		Int64("source_node_id", packet.SourceNodeID).
		Int64("target_node_id", packet.TargetNodeID).
		Int32("ttl_hops", packet.TTLHops).
		Msg("transient packet received")
	m.routeIncomingTransient(packet)
	return nil
}

func (m *Manager) routeIncomingTransient(packet store.TransientPacket) {
	if packet.PacketID == 0 || packet.SourceNodeID <= 0 || packet.TargetNodeID <= 0 {
		return
	}
	if packet.TTLHops <= 0 {
		addPacketLogFields(m.logWarn("transient_packet_dropped", nil), packet).
			Str("reason", "ttl_exhausted").
			Msg("dropping transient packet")
		return
	}
	if !m.markPacketSeen(packet) {
		addPacketLogFields(m.logDebug("transient_packet_deduplicated"), packet).
			Msg("ignoring duplicate transient packet")
		return
	}
	if packet.TargetNodeID == m.cfg.NodeID {
		m.deliverTransientLocal(packet)
		return
	}
	packet.TTLHops--
	if packet.TTLHops <= 0 {
		addPacketLogFields(m.logWarn("transient_packet_dropped", nil), packet).
			Str("reason", "ttl_exhausted").
			Msg("dropping transient packet")
		return
	}
	m.routeOrQueueTransient(packet)
}

func (m *Manager) routeOrQueueTransient(packet store.TransientPacket) {
	if packet.TargetNodeID == m.cfg.NodeID {
		m.deliverTransientLocal(packet)
		return
	}
	if sess := m.bestRouteSession(packet.TargetNodeID); sess != nil {
		addPacketLogFields(m.logSessionEvent("transient_packet_forwarded", sess), packet).
			Int64("next_hop_peer_node_id", sess.peerID).
			Msg("forwarding transient packet")
		sess.enqueue(&internalproto.Envelope{
			NodeId: m.cfg.NodeID,
			Body: &internalproto.Envelope_TransientPacket{
				TransientPacket: transientPacketProto(packet),
			},
		})
		return
	}
	if packet.DeliveryMode != store.DeliveryModeRouteRetry {
		addPacketLogFields(m.logWarn("transient_packet_dropped", nil), packet).
			Str("reason", "no_route").
			Msg("dropping transient packet without route")
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := packetCacheKey(packet.SourceNodeID, packet.PacketID)
	now := time.Now().UTC()
	item := queuedPacket{
		packet:      packet,
		queuedAt:    now,
		nextAttempt: now.Add(routeRetryInterval),
	}
	if current, ok := m.retryQueue[key]; ok {
		item.attempts = current.attempts + 1
		item.queuedAt = current.queuedAt
	}
	m.retryQueue[key] = item
	addPacketLogFields(m.logInfo("transient_packet_queued"), packet).
		Int("attempt", item.attempts).
		Msg("queued transient packet for retry")
}

func (m *Manager) retryTransientPackets() {
	m.mu.Lock()
	if len(m.retryQueue) == 0 {
		m.gcPacketSeenLocked()
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
	m.gcPacketSeenLocked()
	m.mu.Unlock()
	for _, item := range items {
		addPacketLogFields(m.logDebug("transient_packet_retrying"), item.packet).
			Int("attempt", item.attempts).
			Msg("retrying transient packet route")
		m.routeOrQueueTransient(item.packet)
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

func (m *Manager) bestRouteSession(targetNodeID int64) *session {
	m.mu.Lock()
	defer m.mu.Unlock()
	route, ok := m.routingTable[targetNodeID]
	if !ok {
		return nil
	}
	peer := m.peers[route.nextHopPeer]
	if peer == nil {
		return nil
	}
	if peer.sessions != nil {
		if sess := peer.sessions[route.selectedConnectionID]; sess != nil && !sess.isClosed() {
			return sess
		}
	}
	return m.bestPeerSessionLocked(route.nextHopPeer)
}

func (m *Manager) broadcastRoutingUpdate() {
	if !m.routingSupported() {
		return
	}

	m.mu.Lock()
	sessions := make([]*session, 0)
	for _, peer := range m.peers {
		for _, sess := range peer.sessions {
			if sess != nil && sess.supportsRouting && !sess.isClosed() {
				sessions = append(sessions, sess)
			}
		}
	}
	m.mu.Unlock()
	for _, sess := range sessions {
		m.logSessionDebug("routing_update_sent", sess).
			Msg("routing update sent")
		sess.enqueue(m.buildRoutingEnvelope(sess.peerID))
	}
}

func (m *Manager) buildRoutingEnvelope(forPeer int64) *internalproto.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routingGeneration++
	routes := make([]*internalproto.RouteAdvertisement, 0, len(m.routingTable)+1)
	routes = append(routes, &internalproto.RouteAdvertisement{
		DestinationNodeId: m.cfg.NodeID,
		Reachable:         true,
		TotalCostMs:       0,
		ResidualCostMs:    0,
		ResidualJitterMs:  0,
	})
	for _, route := range m.routingTable {
		reachable := true
		totalCost := route.totalCostMs
		residualCost := route.totalCostMs
		residualJitter := route.residualJitterMs
		if route.nextHopPeer == forPeer {
			reachable = false
			totalCost = 0
			residualCost = 0
			residualJitter = 0
		}
		routes = append(routes, &internalproto.RouteAdvertisement{
			DestinationNodeId: route.destinationNodeID,
			Reachable:         reachable,
			TotalCostMs:       uint32(maxInt64(totalCost, 0)),
			ResidualCostMs:    uint32(maxInt64(residualCost, 0)),
			ResidualJitterMs:  uint32(maxInt64(residualJitter, 0)),
		})
	}
	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_RoutingUpdate{
			RoutingUpdate: &internalproto.RoutingUpdate{
				OriginNodeId: m.cfg.NodeID,
				Generation:   m.routingGeneration,
				Routes:       routes,
			},
		},
	}
}

func (m *Manager) recomputeRoutesLocked() {
	nextTable := make(map[int64]routeEntry)
	now := time.Now().UTC()
	for peerID := range m.peers {
		sess := m.bestPeerSessionLocked(peerID)
		if sess == nil {
			continue
		}
		linkCost, jitter := sess.routingCost()
		direct := routeEntry{
			destinationNodeID:    peerID,
			nextHopPeer:          peerID,
			selectedConnectionID: sess.connectionID,
			totalCostMs:          linkCost,
			residualCostMs:       0,
			residualJitterMs:     jitter,
			learnedFromPeer:      peerID,
			generation:           m.routingGeneration,
			updatedAt:            now,
		}
		nextTable[peerID] = direct
		peer := m.peers[peerID]
		for dest, advert := range peer.routeAdverts {
			if !advert.reachable || dest <= 0 || dest == m.cfg.NodeID {
				continue
			}
			candidate := routeEntry{
				destinationNodeID:    dest,
				nextHopPeer:          peerID,
				selectedConnectionID: sess.connectionID,
				totalCostMs:          linkCost + advert.totalCostMs,
				residualCostMs:       advert.totalCostMs,
				residualJitterMs:     advert.residualJitterMs + jitter,
				learnedFromPeer:      peerID,
				generation:           m.routingGeneration,
				updatedAt:            now,
			}
			current, ok := nextTable[dest]
			if !ok || betterRoute(current, candidate) {
				nextTable[dest] = candidate
			}
		}
	}
	for dest, current := range m.routingTable {
		candidate, ok := nextTable[dest]
		if !ok {
			continue
		}
		if !shouldSwitchRoute(current, candidate) {
			nextTable[dest] = current
		}
	}
	m.routingTable = nextTable
}

func (m *Manager) ensurePeerLocked(peerID int64) *peerState {
	peer := m.peers[peerID]
	if peer == nil {
		peer = &peerState{
			sessions:     make(map[uint64]*session),
			routeAdverts: make(map[int64]routeAdvertisement),
		}
		m.peers[peerID] = peer
	}
	if peer.sessions == nil {
		peer.sessions = make(map[uint64]*session)
	}
	if peer.routeAdverts == nil {
		peer.routeAdverts = make(map[int64]routeAdvertisement)
	}
	return peer
}

func (m *Manager) bestPeerSessionLocked(peerID int64) *session {
	peer := m.peers[peerID]
	if peer == nil {
		return nil
	}
	var best *session
	bestCost := int64(0)
	bestJitter := int64(0)
	for _, sess := range peer.sessions {
		if sess == nil || sess.isClosed() {
			continue
		}
		if peer.trustedSession == nil && !sess.supportsRouting {
			continue
		}
		cost, jitter := sess.routingCost()
		if best == nil || cost < bestCost || (cost == bestCost && jitter < bestJitter) {
			best = sess
			bestCost = cost
			bestJitter = jitter
		}
	}
	return best
}

func (s *session) routingCost() (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cost := s.smoothedRTTMs + s.jitterPenaltyMs
	if cost <= 0 {
		cost = 1
	}
	return cost, s.jitterPenaltyMs
}

func (m *Manager) markPacketSeen(packet store.TransientPacket) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcPacketSeenLocked()
	key := packetCacheKey(packet.SourceNodeID, packet.PacketID)
	if _, ok := m.seenPackets[key]; ok {
		return false
	}
	m.seenPackets[key] = time.Now().UTC()
	return true
}

func (m *Manager) gcPacketSeenLocked() {
	now := time.Now().UTC()
	for key, ts := range m.seenPackets {
		if now.Sub(ts) > packetSeenTTL {
			delete(m.seenPackets, key)
		}
	}
}

func betterRoute(current, candidate routeEntry) bool {
	if candidate.totalCostMs != current.totalCostMs {
		return candidate.totalCostMs < current.totalCostMs
	}
	if candidate.residualJitterMs != current.residualJitterMs {
		return candidate.residualJitterMs < current.residualJitterMs
	}
	return candidate.nextHopPeer < current.nextHopPeer
}

func shouldSwitchRoute(current, candidate routeEntry) bool {
	if candidate.nextHopPeer == current.nextHopPeer && candidate.selectedConnectionID == current.selectedConnectionID {
		return true
	}
	if current.totalCostMs-candidate.totalCostMs >= routingSwitchThresholdMs {
		return true
	}
	return false
}

func transientPacketProto(packet store.TransientPacket) *internalproto.TransientPacket {
	return &internalproto.TransientPacket{
		PacketId:     packet.PacketID,
		SourceNodeId: packet.SourceNodeID,
		TargetNodeId: packet.TargetNodeID,
		Recipient:    &internalproto.ClusterUserRef{NodeId: packet.Recipient.NodeID, UserId: packet.Recipient.UserID},
		Sender:       &internalproto.ClusterUserRef{NodeId: packet.Sender.NodeID, UserId: packet.Sender.UserID},
		Body:         append([]byte(nil), packet.Body...),
		DeliveryMode: storeDeliveryModeToCluster(packet.DeliveryMode),
		TtlHops:      uint32(packet.TTLHops),
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
