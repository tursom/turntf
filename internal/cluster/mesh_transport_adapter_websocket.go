package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/tursom/turntf/internal/mesh"
)

// WebSocketMeshTransportAdapter 将WebSocket连接适配到网格运行时。
// 不创建自己的监听器，而是通过Handler()方法暴露HTTP升级端点。
type WebSocketMeshTransportAdapter struct {
	transport *webSocketTransport
	acceptCh  chan mesh.TransportConn
	caps      *mesh.TransportCapability

	mu        sync.Mutex
	closeOnce sync.Once
	ctx       context.Context
}

// NewWebSocketMeshTransportAdapter 创建一个WebSocket网格传输适配器。
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

// Start 存储上下文，供Handler()使用。
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

// Dial 向指定端点发起WebSocket连接。
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

// Accept 返回接受通道。
func (a *WebSocketMeshTransportAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

// Kind 返回传输类型WebSocket。
func (a *WebSocketMeshTransportAdapter) Kind() mesh.TransportKind {
	return mesh.TransportWebSocket
}

// LocalCapabilities 返回本地能力。
func (a *WebSocketMeshTransportAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	return mesh.CloneCapability(a.caps)
}

// Close 关闭适配器。
func (a *WebSocketMeshTransportAdapter) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {})
	return nil
}

// Handler 返回一个HTTP处理函数，用于升级WebSocket连接并将其送入适配器的接受通道。
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
