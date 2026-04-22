package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/tursom/turntf/internal/mesh"
)

type WebSocketMeshTransportAdapter struct {
	transport *webSocketTransport
	acceptCh  chan mesh.TransportConn
	caps      *mesh.TransportCapability

	mu        sync.Mutex
	closeOnce sync.Once
	ctx       context.Context
}

func NewWebSocketMeshTransportAdapter(cfg Config) *WebSocketMeshTransportAdapter {
	cfg = cfg.WithDefaults()
	capability := &mesh.TransportCapability{
		Transport:       mesh.TransportWebSocket,
		InboundEnabled:  cfg.AdvertisePath != "",
		OutboundEnabled: true,
	}
	if cfg.AdvertisePath != "" {
		capability.AdvertisedEndpoints = []string{cfg.AdvertisePath}
	}
	return &WebSocketMeshTransportAdapter{
		transport: newWebSocketTransport(),
		acceptCh:  make(chan mesh.TransportConn, meshTransportAcceptQueue),
		caps:      capability,
	}
}

func (a *WebSocketMeshTransportAdapter) Start(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	a.ctx = ctx
	a.mu.Unlock()
	return nil
}

func (a *WebSocketMeshTransportAdapter) Dial(ctx context.Context, endpoint string) (mesh.TransportConn, error) {
	if a == nil {
		return nil, fmt.Errorf("websocket mesh transport adapter is nil")
	}
	conn, err := a.transport.Dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return wrapMeshTransportConn(mesh.TransportWebSocket, conn, endpoint), nil
}

func (a *WebSocketMeshTransportAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

func (a *WebSocketMeshTransportAdapter) Kind() mesh.TransportKind {
	return mesh.TransportWebSocket
}

func (a *WebSocketMeshTransportAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	return mesh.CloneCapability(a.caps)
}

func (a *WebSocketMeshTransportAdapter) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {})
	return nil
}

// Handler returns an http.HandlerFunc that upgrades incoming websocket
// connections and feeds them into the adapter's Accept channel.
func (a *WebSocketMeshTransportAdapter) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a == nil {
			http.Error(w, "websocket mesh adapter not ready", http.StatusServiceUnavailable)
			return
		}
		a.mu.Lock()
		ctx := a.ctx
		a.mu.Unlock()
		if ctx == nil {
			http.Error(w, "websocket mesh adapter not started", http.StatusServiceUnavailable)
			return
		}
		conn, err := a.transport.Upgrade(w, r)
		if err != nil {
			return
		}
		enqueueMeshTransportConn(ctx, a.acceptCh, mesh.TransportWebSocket, conn)
	}
}
