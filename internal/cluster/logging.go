package cluster

import (
	"errors"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

// logInfo 创建带有标准集群字段的Info级别日志事件。
func (m *Manager) logInfo(event string) *zerolog.Event {
	return log.Info().
		Str("component", "cluster").
		Str("event", event).
		Int64("local_node_id", m.cfg.NodeID)
}

// logWarn 创建带有标准集群字段的Warn级别日志事件。
func (m *Manager) logWarn(event string, err error) *zerolog.Event {
	e := log.Warn().
		Str("component", "cluster").
		Str("event", event).
		Int64("local_node_id", m.cfg.NodeID)
	if err != nil {
		e = e.Err(err)
	}
	return e
}

// logDebug 创建带有标准集群字段的Debug级别日志事件。
func (m *Manager) logDebug(event string) *zerolog.Event {
	return log.Debug().
		Str("component", "cluster").
		Str("event", event).
		Int64("local_node_id", m.cfg.NodeID)
}

// logSessionEvent 创建Info级别的会话日志事件，包含会话上下文字段。
func (m *Manager) logSessionEvent(event string, sess *session) *zerolog.Event {
	e := m.logInfo(event)
	if sess == nil {
		return e
	}
	return addSessionLogFields(e, sess)
}

// logSessionWarn 创建Warn级别的会话日志事件。
func (m *Manager) logSessionWarn(event string, sess *session, err error) *zerolog.Event {
	e := m.logWarn(event, err)
	if sess == nil {
		return e
	}
	return addSessionLogFields(e, sess)
}

// logSessionDebug 创建Debug级别的会话日志事件。
func (m *Manager) logSessionDebug(event string, sess *session) *zerolog.Event {
	e := m.logDebug(event)
	if sess == nil {
		return e
	}
	return addSessionLogFields(e, sess)
}

// logMeshForwardFailure 根据错误类型选择合适的日志级别记录网格转发失败。
// ErrNoRoute → Debug级别（正常拓扑变化）；其他错误 → Warn级别。
func (m *Manager) logMeshForwardFailure(event string, sess *session, err error, message string) {
	if err == nil {
		return
	}
	if errors.Is(err, mesh.ErrNoRoute) {
		m.logSessionDebug(event, sess).
			Err(err).
			Msg(message)
		return
	}
	m.logSessionWarn(event, sess, err).
		Msg(message)
}

// addSessionLogFields 为日志事件添加会话上下文字段：
// direction连接方向、connection_id、transport、peer_node_id、peer_url、remote_addr。
func addSessionLogFields(e *zerolog.Event, sess *session) *zerolog.Event {
	if sess == nil {
		return e
	}
	e = e.
		Str("direction", sessionDirection(sess)).
		Uint64("connection_id", sess.connectionID)
	if transport := sessionTransport(sess); transport != "" {
		e = e.Str("transport", transport)
	}
	if sess.peerID > 0 {
		e = e.Int64("peer_node_id", sess.peerID)
	}
	if url := configuredPeerURL(sess); url != "" {
		e = e.Str("peer_url", url)
	}
	if remoteAddr := sessionRemoteAddr(sess); remoteAddr != "" {
		e = e.Str("remote_addr", remoteAddr)
	}
	return e
}

// addPacketLogFields 为日志事件添加瞬态数据包的相关字段。
func addPacketLogFields(e *zerolog.Event, packet store.TransientPacket) *zerolog.Event {
	return e.
		Uint64("packet_id", packet.PacketID).
		Int64("source_node_id", packet.SourceNodeID).
		Int64("target_node_id", packet.TargetNodeID).
		Int64("recipient_node_id", packet.Recipient.NodeID).
		Int64("recipient_user_id", packet.Recipient.UserID).
		Int32("ttl_hops", packet.TTLHops).
		Str("delivery_mode", string(packet.DeliveryMode))
}

// sessionRemoteAddr 返回会话的远程地址。
func sessionRemoteAddr(sess *session) string {
	if sess == nil || sess.conn == nil {
		return ""
	}
	return sess.conn.RemoteAddr()
}

// sessionTransport 返回会话的传输类型。
func sessionTransport(sess *session) string {
	if sess == nil || sess.conn == nil {
		return ""
	}
	return sess.conn.Transport()
}

// configuredPeerURL 返回会话配置对等节点的URL。
func configuredPeerURL(sess *session) string {
	if sess == nil || sess.configuredPeer == nil {
		return ""
	}
	return sess.configuredPeer.URL
}
