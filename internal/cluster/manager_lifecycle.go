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

// validateConfiguredTransports 验证配置的传输层是否可用。
// 检查ZeroMQ和libp2p在需要时是否已编译且配置正确。
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

// transportForPeerURL 返回对等节点URL对应的传输类型名称。
func (m *Manager) transportForPeerURL(peerURL string) (string, error) {
	transport := transportForPeerURL(peerURL)
	if transport == "" {
		return "", fmt.Errorf("unsupported peer transport for %q", peerURL)
	}
	return transport, nil
}

// canDialPeerURL 判断是否可以向给定URL发起出站连接。
// WebSocket始终可以拨号；ZeroMQ需要启用且允许转发；libp2p需要启用。
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

// canDialDiscoveredPeer 判断是否可以连接一个动态发现的节点。
// 额外检查ZeroMQ curve模式下是否有所需的服务器公钥。
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

// zeroMQCurveServerKeyForPeer 查找指定对等节点的ZeroMQ Curve服务器公钥。
// 先在配置的对等节点中查找，再在发现的节点中查找。
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

// dialerForPeerURL 获取用于连接指定URL的Dialer。
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

// Start 启动Manager及其所有后台循环。
//
// 初始化流程：
//  1. 验证传输层配置
//  2. 启动libp2p（如果已启用）
//  3. 启动网格运行时（meshruntime）作为主控制面
//  4. 启动后台循环：publishLoop、snapshotDigestLoop、transientRetryLoop、
//     meshReplicationLoop、presenceLoop、discoveryLoop
//
// Start仅执行一次；重复调用返回第一次调用的结果。
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

		// 启动网格运行时，入站连接将交由网格运行时处理，不再使用传统会话栈。
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
		go m.snapshotDigestLoop()
		m.wg.Add(1)
		go m.transientRetryLoop()
		m.wg.Add(1)
		go m.meshReplicationLoop()
		m.wg.Add(1)
		go m.presenceLoop()
		if !m.cfg.DiscoveryDisabled {
			m.wg.Add(1)
			go m.discoveryLoop()
		}
	})
	return m.startErr
}

// AcceptLibP2PConn 处理从libp2p传输层接收到的新入站连接。
// 将连接路由到网格运行时；如果网格运行时不可用则关闭连接。
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

// AcceptZeroMQConn 处理从ZeroMQ传输层接收到的新入站连接。
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

// SetZeroMQListenerRunning 设置ZeroMQ监听器的运行状态标记。
func (m *Manager) SetZeroMQListenerRunning(running bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.zeroMQListenerRunning = running
}

// Close 关闭Manager，停止所有后台循环并断开所有会话。
// 关闭流程：取消上下文 → 关闭所有活跃会话 → 关闭libp2p → 等待所有goroutine退出 → 关闭网格运行时。
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
		m.mu.Unlock()

		for _, sess := range sessions {
			sess.close()
		}
		if m.libp2p != nil {
			_ = m.libp2p.Close()
		}
		m.wg.Wait()
		m.mu.Lock()
		m.meshRuntime = nil
		m.mu.Unlock()
		if binding != nil {
			_ = binding.Close()
		}
	})
	return nil
}

// Publish 将一个存储事件排队以广播给所有已连接的对等节点。
// 如果事件发布通道已满，则在新的goroutine中异步排队，防止阻塞调用者。
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

// publishLoop 是事件发布的主循环。
// 从publishCh读取事件并放入批次，同时定期刷新到期的批次。
func (m *Manager) publishLoop() {
	defer m.wg.Done()
	flushTicker := time.NewTicker(maxBatchDelay)
	defer flushTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.drainPublishedEvents()
			m.flushReplicationBatches()
			return
		case event := <-m.publishCh:
			m.queuePublishedEvent(event)
		case <-flushTicker.C:
			m.flushReplicationBatchesDue(time.Now().UTC())
		}
	}
}

// snapshotDigestLoop 定期扫描所有对等节点，发送脏的快照摘要。
func (m *Manager) snapshotDigestLoop() {
	defer m.wg.Done()
	if m == nil || m.ctx == nil {
		return
	}
	ticker := time.NewTicker(snapshotDigestSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.flushSnapshotDigestsDue(time.Now().UTC())
		}
	}
}

// transientRetryLoop 定期重试队列中的瞬态数据包。
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

// meshReplicationLoop 是网格复制的主循环。
// 定期进行数据追赶（catchup）和反熵检查。
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
				m.markSnapshotDigestDirty(sess.peerID, false)
			}
		}
	}
}

// activeSessions 返回所有当前活跃的会话。
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

// handleWebSocket 处理HTTP WebSocket升级请求。
// 升级后立即将连接路由到网格运行时。
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

// newSession 创建一个新的会话对象，分配连接ID并初始化内部状态。
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
