package api

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

var clientWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (h *HTTP) handleClientWebSocket(w http.ResponseWriter, r *http.Request) {
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
