package api

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

const (
	// clientWSPath 标准客户端 WebSocket 连接路径，支持持久化消息和即时消息。
	clientWSPath = "/ws/client"
	// clientRealtimeWSPath 实时流路径，仅支持即时消息（不持久化，不推送历史消息）。
	clientRealtimeWSPath = "/ws/realtime"
)

// clientWSUpgrader 将 HTTP 连接升级为 WebSocket 连接，允许所有来源（集群内部通信）。
var clientWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// handleClientWebSocket 处理标准客户端 WebSocket 连接。
func (h *HTTP) handleClientWebSocket(w http.ResponseWriter, r *http.Request) {
	h.handleUpgradedClientWebSocket(w, r)
}

// handleRealtimeWebSocket 处理实时流 WebSocket 连接（仅即时消息）。
func (h *HTTP) handleRealtimeWebSocket(w http.ResponseWriter, r *http.Request) {
	h.handleUpgradedClientWebSocket(w, r)
}

// handleUpgradedClientWebSocket 执行 WebSocket 升级，然后委托给 serveClientConn 处理会话生命周期。
func (h *HTTP) handleUpgradedClientWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := clientWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Warn().
			Err(err).
			Str("component", "api").
			Str("protocol", "ws").
			Str("path", r.URL.Path).
			Str("remote_addr", r.RemoteAddr).
			Str("event", "client_transport_upgrade_failed").
			Msg("client transport upgrade failed")
		return
	}
	h.serveClientConn(newClientWSConn(conn), r.Context(), r.URL.Path)
}
