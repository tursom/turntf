package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	peerSourceStatic     = "static"
	peerSourceDiscovered = "discovered"
	peerSourceInbound    = "inbound"

	discoveryStateCandidate = "candidate"
	discoveryStateDialing   = "dialing"
	discoveryStateConnected = "connected"
	discoveryStateFailed    = "failed"
	discoveryStateExpired   = "expired"
)

func (m *Manager) loadDiscoveredPeers(ctx context.Context) error {
	if m == nil || m.store == nil {
		return nil
	}
	peers, err := m.store.ListDiscoveredPeers(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, peer := range peers {
		normalized, err := normalizePeerURL(peer.URL)
		if err != nil {
			continue
		}
		if peer.NodeID == m.cfg.NodeID {
			m.selfKnownURLs[normalized] = peer.Generation
			continue
		}
		m.discoveredPeers[normalized] = &discoveredPeerState{
			nodeID:           peer.NodeID,
			url:              normalized,
			sourcePeerNodeID: peer.SourcePeerNodeID,
			state:            peer.State,
			firstSeenAt:      timestampWallTime(peer.FirstSeenAt.WallTimeMs),
			lastSeenAt:       timestampWallTime(peer.LastSeenAt.WallTimeMs),
			lastError:        peer.LastError,
			generation:       peer.Generation,
		}
		if peer.LastConnectedAt != nil {
			m.discoveredPeers[normalized].lastConnectedAt = timestampWallTime(peer.LastConnectedAt.WallTimeMs)
		}
	}
	return nil
}

func (m *Manager) discoveryLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(membershipUpdateInterval)
	defer ticker.Stop()

	m.reconcileDiscoveredDialers()
	m.broadcastMembershipUpdate()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.expireDiscoveredCandidates()
			m.reconcileDiscoveredDialers()
			m.broadcastMembershipUpdate()
		}
	}
}

func (m *Manager) handleMembershipUpdate(sess *session, envelope *internalproto.Envelope) error {
	if m == nil || m.cfg.DiscoveryDisabled {
		return nil
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	update := envelope.GetMembershipUpdate()
	if update == nil {
		return errors.New("membership update body cannot be empty")
	}
	if update.OriginNodeId != sess.peerID {
		return fmt.Errorf("membership update origin mismatch: got %d want %d", update.OriginNodeId, sess.peerID)
	}

	accepted := 0
	for _, item := range update.Peers {
		if item == nil {
			continue
		}
		if err := m.observePeerAdvertisement(sess.peerID, item); err != nil {
			m.recordDiscoveryReject()
			m.logSessionDebug("membership_advertisement_ignored", sess).
				Int64("advertised_node_id", item.NodeId).
				Str("advertised_url", item.Url).
				Str("reason", err.Error()).
				Msg("ignored membership advertisement")
			continue
		}
		accepted++
	}

	m.mu.Lock()
	m.membershipUpdatesRecv++
	m.mu.Unlock()
	m.logSessionDebug("membership_update_received", sess).
		Uint64("generation", update.Generation).
		Int("advertisement_count", len(update.Peers)).
		Int("accepted_count", accepted).
		Msg("membership update received")
	return nil
}

func (m *Manager) observePeerAdvertisement(sourcePeerNodeID int64, item *internalproto.PeerAdvertisement) error {
	if item == nil {
		return errors.New("advertisement cannot be empty")
	}
	if item.NodeId <= 0 {
		return errors.New("advertisement node id cannot be empty")
	}
	normalized, err := normalizePeerURL(item.Url)
	if err != nil {
		return err
	}

	if item.NodeId == m.cfg.NodeID {
		m.mu.Lock()
		m.selfKnownURLs[normalized] = item.Generation
		m.mu.Unlock()
		return nil
	}
	return m.recordDiscoveredCandidate(item.NodeId, normalized, sourcePeerNodeID, item.Generation)
}

func (m *Manager) recordDiscoveredCandidate(nodeID int64, normalizedURL string, sourcePeerNodeID int64, generation uint64) error {
	if nodeID <= 0 || nodeID == m.cfg.NodeID {
		return nil
	}
	if normalizedURL == "" {
		return errors.New("discovered peer url cannot be empty")
	}

	now := time.Now().UTC()
	var snapshot discoveredPeerState
	m.mu.Lock()
	peer := m.discoveredPeers[normalizedURL]
	if peer == nil {
		peer = &discoveredPeerState{
			nodeID:      nodeID,
			url:         normalizedURL,
			state:       discoveryStateCandidate,
			firstSeenAt: now,
		}
		m.discoveredPeers[normalizedURL] = peer
	} else if peer.nodeID > 0 && peer.nodeID != nodeID {
		m.mu.Unlock()
		return fmt.Errorf("discovered peer url %q already belongs to node %d", normalizedURL, peer.nodeID)
	}
	peer.nodeID = nodeID
	peer.sourcePeerNodeID = sourcePeerNodeID
	if peer.state == "" || peer.state == discoveryStateExpired || peer.state == discoveryStateFailed {
		peer.state = discoveryStateCandidate
	}
	peer.lastSeenAt = now
	if generation > peer.generation {
		peer.generation = generation
	}
	snapshot = *peer
	m.mu.Unlock()

	m.persistDiscoveredPeer(snapshot, false)
	return nil
}

func (m *Manager) recordConfiguredPeerDialing(peer *configuredPeer) {
	if m == nil || m.cfg.DiscoveryDisabled || peer == nil || !peer.dynamic {
		return
	}
	m.recordDiscoveredState(peer.nodeID, peer.URL, discoveryStateDialing, "", false)
}

func (m *Manager) recordConfiguredPeerDialFailure(peer *configuredPeer, err error) {
	if m == nil || m.cfg.DiscoveryDisabled || peer == nil || !peer.dynamic {
		return
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	m.recordDiscoveredState(peer.nodeID, peer.URL, discoveryStateFailed, message, false)
}

func (m *Manager) recordConfiguredPeerSessionClosed(peer *configuredPeer, sess *session) {
	if m == nil || m.cfg.DiscoveryDisabled || peer == nil || !peer.dynamic || m.ctx == nil {
		return
	}
	select {
	case <-m.ctx.Done():
		return
	default:
	}
	nodeID := peer.nodeID
	if nodeID <= 0 && sess != nil {
		nodeID = sess.peerID
	}
	if nodeID <= 0 {
		return
	}
	m.recordDiscoveredState(nodeID, peer.URL, discoveryStateFailed, "session closed", false)
}

func (m *Manager) recordSessionDiscoveryConnected(sess *session) {
	if m == nil || m.cfg.DiscoveryDisabled || sess == nil || sess.configuredPeer == nil {
		return
	}
	source := peerSourceStatic
	if sess.configuredPeer.dynamic {
		source = peerSourceDiscovered
	}
	m.recordDiscoveredState(sess.peerID, sess.configuredPeer.URL, discoveryStateConnected, "", true)
	m.logSessionDebug("peer_discovery_recorded", sess).
		Str("source", source).
		Msg("recorded peer discovery state")
}

func (m *Manager) recordDiscoveredState(nodeID int64, rawURL, state, lastError string, connected bool) {
	normalized, err := normalizePeerURL(rawURL)
	if err != nil || nodeID <= 0 || nodeID == m.cfg.NodeID {
		return
	}
	now := time.Now().UTC()
	var snapshot discoveredPeerState
	m.mu.Lock()
	peer := m.discoveredPeers[normalized]
	if peer == nil {
		peer = &discoveredPeerState{
			nodeID:      nodeID,
			url:         normalized,
			firstSeenAt: now,
		}
		m.discoveredPeers[normalized] = peer
	}
	peer.nodeID = nodeID
	peer.url = normalized
	if state != "" {
		peer.state = state
	}
	peer.lastSeenAt = now
	peer.lastError = strings.TrimSpace(lastError)
	if connected {
		peer.lastConnectedAt = now
	}
	snapshot = *peer
	m.mu.Unlock()

	m.persistDiscoveredPeer(snapshot, connected)
}

func (m *Manager) expireDiscoveredCandidates() {
	now := time.Now().UTC()
	expired := make([]discoveredPeerState, 0)
	m.mu.Lock()
	for _, peer := range m.discoveredPeers {
		if peer == nil || peer.state == discoveryStateConnected || peer.state == discoveryStateExpired {
			continue
		}
		if !peer.lastSeenAt.IsZero() && now.Sub(peer.lastSeenAt) > discoveryCandidateTTL {
			peer.state = discoveryStateExpired
			peer.lastError = "candidate expired"
			expired = append(expired, *peer)
		}
	}
	m.mu.Unlock()
	for _, peer := range expired {
		m.persistDiscoveredPeer(peer, false)
	}
}

func (m *Manager) reconcileDiscoveredDialers() {
	type candidate struct {
		peer discoveredPeerState
	}
	candidates := make([]candidate, 0)
	m.mu.Lock()
	dynamicCount := len(m.dynamicPeers)
	staticURLs := make(map[string]struct{}, len(m.configuredPeers))
	for _, peer := range m.configuredPeers {
		if normalized, err := normalizePeerURL(peer.URL); err == nil {
			staticURLs[normalized] = struct{}{}
		}
	}
	for url, peer := range m.discoveredPeers {
		if peer == nil || peer.nodeID <= 0 || peer.nodeID == m.cfg.NodeID {
			continue
		}
		if _, ok := staticURLs[url]; ok {
			continue
		}
		if _, ok := m.dynamicPeers[url]; ok || peer.dialing {
			continue
		}
		if peer.state == discoveryStateExpired {
			continue
		}
		if active := m.peers[peer.nodeID]; active != nil && active.active != nil {
			continue
		}
		candidates = append(candidates, candidate{peer: *peer})
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].peer
		right := candidates[j].peer
		if left.lastConnectedAt.IsZero() != right.lastConnectedAt.IsZero() {
			return !left.lastConnectedAt.IsZero()
		}
		if !left.lastConnectedAt.Equal(right.lastConnectedAt) {
			return left.lastConnectedAt.After(right.lastConnectedAt)
		}
		if !left.lastSeenAt.Equal(right.lastSeenAt) {
			return left.lastSeenAt.After(right.lastSeenAt)
		}
		return left.url < right.url
	})
	toStart := make([]*configuredPeer, 0)
	for _, item := range candidates {
		if dynamicCount >= maxDynamicDiscoveredPeers {
			break
		}
		peer := m.discoveredPeers[item.peer.url]
		if peer == nil {
			continue
		}
		peer.dialing = true
		peer.state = discoveryStateDialing
		configured := &configuredPeer{
			URL:     peer.url,
			nodeID:  peer.nodeID,
			dynamic: true,
			source:  peerSourceDiscovered,
		}
		m.dynamicPeers[peer.url] = configured
		toStart = append(toStart, configured)
		dynamicCount++
	}
	m.mu.Unlock()

	for _, peer := range toStart {
		peer := peer
		m.wg.Add(1)
		go m.dialLoop(peer)
	}
}

func (m *Manager) broadcastMembershipUpdate() {
	if m == nil || m.cfg.DiscoveryDisabled {
		return
	}
	sessions := m.membershipSessions()
	for _, sess := range sessions {
		m.sendMembershipUpdate(sess)
	}
}

func (m *Manager) sendMembershipUpdate(sess *session) {
	if m == nil || m.cfg.DiscoveryDisabled || sess == nil || !sess.supportsMembership || sess.isClosed() {
		return
	}
	envelope := m.buildMembershipEnvelope()
	if envelope == nil {
		return
	}
	sess.enqueue(envelope)
	m.mu.Lock()
	m.membershipUpdatesSent++
	m.mu.Unlock()
}

func (m *Manager) membershipSessions() []*session {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions := make([]*session, 0)
	for _, peer := range m.peers {
		if peer == nil {
			continue
		}
		for _, sess := range peer.sessions {
			if sess != nil && sess.supportsMembership && !sess.isClosed() {
				sessions = append(sessions, sess)
			}
		}
	}
	return sessions
}

func (m *Manager) buildMembershipEnvelope() *internalproto.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.membershipGeneration++
	generation := m.membershipGeneration
	items := make([]*internalproto.PeerAdvertisement, 0, len(m.configuredPeers)+len(m.discoveredPeers)+len(m.selfKnownURLs))
	seen := make(map[string]struct{})
	add := func(nodeID int64, rawURL string, itemGeneration uint64) {
		if nodeID <= 0 || strings.TrimSpace(rawURL) == "" {
			return
		}
		normalized, err := normalizePeerURL(rawURL)
		if err != nil {
			return
		}
		key := fmt.Sprintf("%d\x00%s", nodeID, normalized)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		items = append(items, &internalproto.PeerAdvertisement{
			NodeId:           nodeID,
			Url:              normalized,
			Generation:       maxUint64(generation, itemGeneration),
			ObservedAtUnixMs: time.Now().UTC().UnixMilli(),
		})
	}

	for _, peer := range m.configuredPeers {
		if peer.nodeID > 0 {
			add(peer.nodeID, peer.URL, generation)
		}
	}
	for _, peer := range m.discoveredPeers {
		if peer == nil || peer.nodeID <= 0 || peer.state != discoveryStateConnected {
			continue
		}
		add(peer.nodeID, peer.url, peer.generation)
	}
	for rawURL, itemGeneration := range m.selfKnownURLs {
		add(m.cfg.NodeID, rawURL, itemGeneration)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].NodeId != items[j].NodeId {
			return items[i].NodeId < items[j].NodeId
		}
		return items[i].Url < items[j].Url
	})
	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_MembershipUpdate{
			MembershipUpdate: &internalproto.MembershipUpdate{
				OriginNodeId: m.cfg.NodeID,
				Generation:   generation,
				Peers:        items,
			},
		},
	}
}

func (m *Manager) persistDiscoveredPeer(peer discoveredPeerState, connected bool) {
	if m == nil || m.store == nil || peer.nodeID <= 0 || peer.url == "" {
		return
	}
	var lastConnected *time.Time
	if connected || !peer.lastConnectedAt.IsZero() {
		lastConnected = &peer.lastConnectedAt
	}
	err := m.store.UpsertDiscoveredPeer(context.Background(), store.DiscoveredPeer{
		NodeID:           peer.nodeID,
		URL:              peer.url,
		SourcePeerNodeID: peer.sourcePeerNodeID,
		State:            peer.state,
		LastError:        peer.lastError,
		Generation:       peer.generation,
		LastConnectedAt:  clockTimestampPointer(m.clock, lastConnected),
	})
	if err != nil {
		m.recordDiscoveryPersistFailure()
		m.logWarn("discovered_peer_persist_failed", err).
			Int64("peer_node_id", peer.nodeID).
			Str("peer_url", peer.url).
			Str("state", peer.state).
			Msg("failed to persist discovered peer")
	}
}

func (m *Manager) discoveryStateByURLLocked(rawURL string) discoveredPeerState {
	normalized, err := normalizePeerURL(rawURL)
	if err != nil {
		return discoveredPeerState{}
	}
	if peer := m.discoveredPeers[normalized]; peer != nil {
		return *peer
	}
	return discoveredPeerState{}
}

func (m *Manager) discoveryStateByNodeIDLocked(nodeID int64) discoveredPeerState {
	var best discoveredPeerState
	for _, peer := range m.discoveredPeers {
		if peer == nil || peer.nodeID != nodeID {
			continue
		}
		if best.url == "" ||
			(!peer.lastConnectedAt.IsZero() && best.lastConnectedAt.IsZero()) ||
			peer.lastConnectedAt.After(best.lastConnectedAt) ||
			(peer.lastConnectedAt.Equal(best.lastConnectedAt) && peer.lastSeenAt.After(best.lastSeenAt)) ||
			(peer.lastConnectedAt.Equal(best.lastConnectedAt) && peer.lastSeenAt.Equal(best.lastSeenAt) && peer.url < best.url) {
			best = *peer
		}
	}
	return best
}

func (m *Manager) recordDiscoveryReject() {
	m.mu.Lock()
	m.discoveryRejects++
	m.mu.Unlock()
}

func (m *Manager) recordDiscoveryPersistFailure() {
	m.mu.Lock()
	m.discoveryPersistFailures++
	m.mu.Unlock()
}

func normalizePeerURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("peer url cannot be empty")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse peer url: %w", err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "ws" && scheme != "wss" {
		return "", fmt.Errorf("peer url scheme must be ws or wss")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", errors.New("peer url host cannot be empty")
	}
	if parsed.Fragment != "" {
		return "", errors.New("peer url fragment is not allowed")
	}
	parsed.Scheme = scheme
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String(), nil
}

func timestampWallTime(wallTimeMs int64) time.Time {
	if wallTimeMs <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(wallTimeMs).UTC()
}

func clockTimestampPointer(clockRef *clock.Clock, value *time.Time) *clock.Timestamp {
	if value == nil || value.IsZero() {
		return nil
	}
	ts := clockRef.Now()
	ts.WallTimeMs = value.UTC().UnixMilli()
	return &ts
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
