package cluster

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/mesh"
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
	if m.cfg.LibP2P.Enabled && m.libp2p == nil {
		return fmt.Errorf("libp2p transport is not configured")
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
		return m != nil && m.cfg.zeroMQDialEnabled() && m.cfg.ZeroMQForwardingEnabled()
	case transportLibP2P:
		return m != nil && m.cfg.LibP2P.Enabled
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
		if m.libp2p != nil {
			if err := m.libp2p.Start(m.ctx); err != nil {
				m.startErr = err
				m.cancel()
				return
			}
		}

		// Phase 4.5: start the mesh runtime as the primary control plane.
		// Inbound libp2p/zeromq/websocket connections now feed the mesh
		// runtime instead of the legacy session stack.
		if err := m.StartMeshRuntime(m.ctx); err != nil {
			m.startErr = err
			m.logWarn("mesh_runtime_start_failed", err).Msg("mesh runtime failed to start")
			m.cancel()
			if m.libp2p != nil {
				_ = m.libp2p.Close()
			}
			return
		}

		m.wg.Add(1)
		go m.publishLoop()
		m.wg.Add(1)
		go m.transientRetryLoop()
		m.wg.Add(1)
		go m.meshReplicationLoop()
		if !m.cfg.DiscoveryDisabled {
			m.wg.Add(1)
			go m.discoveryLoop()
		}
	})
	return m.startErr
}

func (m *Manager) AcceptLibP2PConn(conn TransportConn) {
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
		Msg("accepted inbound peer libp2p connection")

	if m.routeInboundToMesh(mesh.TransportLibP2P, conn) {
		return
	}
	closeTransport(conn, "mesh runtime unavailable")
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

	if m.routeInboundToMesh(mesh.TransportZeroMQ, conn) {
		return
	}
	closeTransport(conn, "mesh runtime unavailable")
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
		binding := m.meshRuntime
		m.meshRuntime = nil
		m.mu.Unlock()

		for _, sess := range sessions {
			sess.close()
		}
		if binding != nil {
			_ = binding.Close()
		}
		if m.libp2p != nil {
			_ = m.libp2p.Close()
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

func (m *Manager) transientRetryLoop() {
	defer m.wg.Done()

	retryTicker := time.NewTicker(routeRetryInterval)
	defer retryTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-retryTicker.C:
			m.retryTransientPackets()
		}
	}
}

func (m *Manager) meshReplicationLoop() {
	defer m.wg.Done()
	if m == nil || m.ctx == nil {
		return
	}
	catchupTicker := time.NewTicker(catchupRetryInterval)
	defer catchupTicker.Stop()
	antiEntropyTicker := time.NewTicker(antiEntropyInterval)
	defer antiEntropyTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-catchupTicker.C:
			m.ensureMeshPeerSessions()
			for _, sess := range m.meshPeerSessions() {
				if _, err := m.requestCatchupIfNeeded(sess); err != nil {
					m.logSessionWarn("mesh_periodic_catchup_failed", sess, err).
						Msg("mesh periodic catchup failed")
				}
			}
		case <-antiEntropyTicker.C:
			m.ensureMeshPeerSessions()
			for _, sess := range m.meshPeerSessions() {
				m.sendSnapshotDigest(sess)
			}
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

	if m.routeInboundToMesh(mesh.TransportWebSocket, conn) {
		return
	}
	closeTransport(conn, "mesh runtime unavailable")
}

func (m *Manager) newSession(conn TransportConn, outbound bool, configuredPeer *configuredPeer) *session {
	m.mu.Lock()
	m.nextConnectionID++
	connectionID := m.nextConnectionID
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
		supportsMembership:   !m.cfg.DiscoveryDisabled,
	}
}
