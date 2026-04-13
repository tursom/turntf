package cluster

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) logInfo(event string) *zerolog.Event {
	return log.Info().
		Str("component", "cluster").
		Str("event", event).
		Int64("local_node_id", m.cfg.NodeID)
}

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

func (m *Manager) logDebug(event string) *zerolog.Event {
	return log.Debug().
		Str("component", "cluster").
		Str("event", event).
		Int64("local_node_id", m.cfg.NodeID)
}

func (m *Manager) logSessionEvent(event string, sess *session) *zerolog.Event {
	e := m.logInfo(event)
	if sess == nil {
		return e
	}
	return addSessionLogFields(e, sess)
}

func (m *Manager) logSessionWarn(event string, sess *session, err error) *zerolog.Event {
	e := m.logWarn(event, err)
	if sess == nil {
		return e
	}
	return addSessionLogFields(e, sess)
}

func (m *Manager) logSessionDebug(event string, sess *session) *zerolog.Event {
	e := m.logDebug(event)
	if sess == nil {
		return e
	}
	return addSessionLogFields(e, sess)
}

func addSessionLogFields(e *zerolog.Event, sess *session) *zerolog.Event {
	if sess == nil {
		return e
	}
	e = e.
		Str("direction", sessionDirection(sess)).
		Uint64("connection_id", sess.connectionID)
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

func sessionRemoteAddr(sess *session) string {
	if sess == nil || sess.conn == nil || sess.conn.RemoteAddr() == nil {
		return ""
	}
	return sess.conn.RemoteAddr().String()
}

func configuredPeerURL(sess *session) string {
	if sess == nil || sess.configuredPeer == nil {
		return ""
	}
	return sess.configuredPeer.URL
}
