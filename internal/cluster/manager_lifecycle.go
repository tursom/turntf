package cluster

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) validateConfiguredTransports() error {
	if m == nil {
		return nil
	}
	needsZeroMQDial := m.cfg.zeroMQDialEnabled()
	if !needsZeroMQDial {
		for _, peer := range m.configuredPeers {
			if peer != nil && isZeroMQPeerURL(peer.URL) {
				needsZeroMQDial = true
				break
			}
		}
	}
	needsZeroMQListener := m.cfg.zeroMQListenerEnabled()
	if (needsZeroMQDial || needsZeroMQListener) && !zeroMQEnabled() {
		return errZeroMQNotBuilt
	}
	return nil
}

func (m *Manager) transportForPeerURL(peerURL string) (string, error) {
	transport := transportForPeerURL(peerURL)
	if transport == "" {
		return "", fmt.Errorf("unsupported peer transport for %q", peerURL)
	}
	return transport, nil
}

func (m *Manager) canDialPeerURL(peerURL string) bool {
	switch transportForPeerURL(peerURL) {
	case transportWebSocket:
		return true
	case transportZeroMQ:
		return m != nil && m.cfg.zeroMQDialEnabled()
	default:
		return false
	}
}

func (m *Manager) canDialDiscoveredPeer(peer *discoveredPeerState) bool {
	if peer == nil {
		return false
	}
	if !m.canDialPeerURL(peer.url) {
		return false
	}
	if isZeroMQPeerURL(peer.url) && m.cfg.zeroMQCurveEnabled() {
		return strings.TrimSpace(peer.zeroMQCurveServerPublicKey) != ""
	}
	return true
}

func (m *Manager) zeroMQCurveServerKeyForPeer(peerURL string) string {
	if m == nil || !m.cfg.zeroMQCurveEnabled() {
		return ""
	}
	normalized, err := normalizePeerURL(peerURL)
	if err != nil {
		normalized = peerURL
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, peer := range m.configuredPeers {
		if peer == nil {
			continue
		}
		if peer.URL == normalized {
			return strings.TrimSpace(peer.zeroMQCurveServerPublicKey)
		}
	}
	if peer := m.discoveredPeers[normalized]; peer != nil {
		return strings.TrimSpace(peer.zeroMQCurveServerPublicKey)
	}
	return ""
}

func (m *Manager) dialerForPeerURL(peerURL string) (Dialer, error) {
	transport, err := m.transportForPeerURL(peerURL)
	if err != nil {
		return nil, err
	}
	dialer := m.dialers[transport]
	if dialer == nil {
		return nil, fmt.Errorf("%s dialer is not configured", transport)
	}
	return dialer, nil
}

func (m *Manager) Start(parent context.Context) error {
	m.startOnce.Do(func() {
		m.ctx, m.cancel = context.WithCancel(parent)

		if err := m.validateConfiguredTransports(); err != nil {
			m.startErr = err
			m.cancel()
			return
		}

		m.wg.Add(1)
		go m.publishLoop()
		m.wg.Add(1)
		go m.routingLoop()
		if !m.cfg.DiscoveryDisabled {
			m.wg.Add(1)
			go m.discoveryLoop()
		}

		for _, peer := range m.configuredPeers {
			peer := peer
			m.wg.Add(1)
			go m.dialLoop(peer)
		}
	})
	return m.startErr
}

func (m *Manager) AcceptZeroMQConn(conn TransportConn) {
	if conn == nil {
		return
	}
	if m.ctx == nil || m.ctx.Err() != nil {
		closeTransport(conn, "shutdown")
		return
	}
	m.logInfo("peer_inbound_accepted").
		Str("direction", "inbound").
		Str("transport", conn.Transport()).
		Str("remote_addr", conn.RemoteAddr()).
		Str("bind_url", m.cfg.ZeroMQ.BindURL).
		Msg("accepted inbound peer zeromq connection")

	sess := m.newSession(conn, false, nil)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runSession(sess)
	}()
}

func (m *Manager) SetZeroMQListenerRunning(running bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.zeroMQListenerRunning = running
}

func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}

		m.mu.Lock()
		sessions := make([]*session, 0, len(m.peers))
		for _, peer := range m.peers {
			if peer.active != nil {
				sessions = append(sessions, peer.active)
			}
		}
		m.mu.Unlock()

		for _, sess := range sessions {
			sess.close()
		}
		m.wg.Wait()
	})
	return nil
}

func (m *Manager) Publish(event store.Event) {
	if m == nil || m.ctx == nil {
		return
	}

	select {
	case m.publishCh <- event:
	case <-m.ctx.Done():
	default:
		go func() {
			select {
			case m.publishCh <- event:
			case <-m.ctx.Done():
			}
		}()
	}
}

func (m *Manager) publishLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case event := <-m.publishCh:
			m.broadcastEvent(event)
		}
	}
}

func (m *Manager) routingLoop() {
	defer m.wg.Done()

	updateTicker := time.NewTicker(routingUpdateInterval)
	retryTicker := time.NewTicker(routeRetryInterval)
	defer updateTicker.Stop()
	defer retryTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-updateTicker.C:
			m.broadcastRoutingUpdate()
		case <-retryTicker.C:
			m.retryTransientPackets()
		}
	}
}

func (m *Manager) activeSessions() []*session {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions := make([]*session, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.active != nil {
			sessions = append(sessions, peer.active)
		}
	}
	return sessions
}

func (m *Manager) dialLoop(peer *configuredPeer) {
	defer m.wg.Done()

	backoff := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	attempt := 0

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		if peerID := m.configuredPeerNodeID(peer); peerID > 0 && m.cfg.NodeID > peerID && m.hasActivePeer(peerID) {
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		m.recordConfiguredPeerDialing(peer)
		m.logInfo("peer_dial_started").
			Str("direction", "outbound").
			Str("peer_url", peer.URL).
			Msg("starting outbound peer dial")
		dialer, err := m.dialerForPeerURL(peer.URL)
		if err != nil {
			m.recordConfiguredPeerDialFailure(peer, err)
			wait := backoff[minInt(attempt, len(backoff)-1)]
			m.logWarn("peer_dial_failed", err).
				Str("direction", "outbound").
				Str("peer_url", peer.URL).
				Dur("retry_in", wait).
				Msg("outbound peer dial failed")
			attempt++
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}
		conn, err := dialer.Dial(m.ctx, peer.URL)
		if err != nil {
			m.recordConfiguredPeerDialFailure(peer, err)
			wait := backoff[minInt(attempt, len(backoff)-1)]
			m.logWarn("peer_dial_failed", err).
				Str("direction", "outbound").
				Str("peer_url", peer.URL).
				Dur("retry_in", wait).
				Msg("outbound peer dial failed")
			attempt++
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}

		attempt = 0
		sess := m.newSession(conn, true, peer)
		m.logSessionEvent("peer_dial_succeeded", sess).
			Msg("outbound peer dial succeeded")
		m.runSession(sess)
		m.recordConfiguredPeerSessionClosed(peer, sess)

		select {
		case <-m.ctx.Done():
			return
		default:
		}
	}
}

func (m *Manager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if m.ctx == nil || m.ctx.Err() != nil {
		http.Error(w, "cluster manager not started", http.StatusServiceUnavailable)
		return
	}

	conn, err := m.websocket.Upgrade(w, r)
	if err != nil {
		return
	}
	if m.ctx.Err() != nil {
		closeTransport(conn, "shutdown")
		return
	}
	m.logInfo("peer_inbound_accepted").
		Str("direction", "inbound").
		Str("transport", conn.Transport()).
		Str("remote_addr", r.RemoteAddr).
		Str("path", r.URL.Path).
		Msg("accepted inbound peer websocket")

	sess := m.newSession(conn, false, nil)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runSession(sess)
	}()
}

func (m *Manager) newSession(conn TransportConn, outbound bool, configuredPeer *configuredPeer) *session {
	m.mu.Lock()
	m.routingGeneration++
	connectionID := m.routingGeneration
	m.mu.Unlock()
	return &session{
		manager:              m,
		conn:                 conn,
		outbound:             outbound,
		configuredPeer:       configuredPeer,
		connectionID:         connectionID,
		send:                 make(chan *internalproto.Envelope, outboundQueueSize),
		remoteOriginProgress: make(map[int64]uint64),
		pendingPulls:         make(map[int64]pendingPullState),
		pendingTimeSync:      make(map[uint64]chan timeSyncResult),
		supportsRouting:      true,
		supportsMembership:   !m.cfg.DiscoveryDisabled,
	}
}

func (m *Manager) hasActivePeer(peerID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[peerID]
	return ok && peer.active != nil
}

func (m *Manager) configuredPeerNodeID(peer *configuredPeer) int64 {
	if peer == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return peer.nodeID
}
