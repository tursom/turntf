package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/tursom/turntf/internal/mesh"
)

// meshTransportAcceptQueue 是网格传输适配器接受通道的缓冲区大小。
const meshTransportAcceptQueue = 128

// LibP2PMeshTransportAdapter 将libp2p传输适配到网格运行时。
// 创建并管理自己的libp2p主机实例。
type LibP2PMeshTransportAdapter struct {
	cfg           LibP2PConfig
	clusterSecret string
	acceptCh      chan mesh.TransportConn

	mu        sync.Mutex
	transport *libP2PTransport
	closeOnce sync.Once
}

// ZeroMQMeshTransportAdapter 将ZeroMQ传输适配到网格运行时。
type ZeroMQMeshTransportAdapter struct {
	cfg              ZeroMQConfig
	serverKeyForPeer func(string) string
	acceptCh         chan mesh.TransportConn

	mu        sync.Mutex
	listener  *ZeroMQMuxListener
	closeOnce sync.Once
}

// meshTransportConn 包装TransportConn以适配mesh.TransportConn接口。
type meshTransportConn struct {
	conn       TransportConn
	kind       mesh.TransportKind
	remoteHint string
}

// NewMeshTransportAdapters 为所有启用的传输创建适配器。
func NewMeshTransportAdapters(cfg Config, zeroMQCurveServerKeyForPeer func(string) string) []mesh.TransportAdapter {
	adapters := make([]mesh.TransportAdapter, 0, 3)
	if adapter := NewWebSocketMeshTransportAdapter(cfg); adapter != nil {
		adapters = append(adapters, adapter)
	}
	if adapter := NewLibP2PMeshTransportAdapter(cfg); adapter != nil {
		adapters = append(adapters, adapter)
	}
	if adapter := NewZeroMQMeshTransportAdapter(cfg, zeroMQCurveServerKeyForPeer); adapter != nil {
		adapters = append(adapters, adapter)
	}
	return adapters
}

// NewLibP2PMeshTransportAdapter 创建一个libp2p网格传输适配器。
func NewLibP2PMeshTransportAdapter(cfg Config) *LibP2PMeshTransportAdapter {
	cfg = cfg.WithDefaults()
	if !cfg.LibP2P.Enabled {
		return nil
	}
	return &LibP2PMeshTransportAdapter{
		cfg:           cfg.LibP2P,
		clusterSecret: cfg.ClusterSecret,
		acceptCh:      make(chan mesh.TransportConn, meshTransportAcceptQueue),
	}
}

// NewZeroMQMeshTransportAdapter 创建一个ZeroMQ网格传输适配器。
func NewZeroMQMeshTransportAdapter(cfg Config, zeroMQCurveServerKeyForPeer func(string) string) *ZeroMQMeshTransportAdapter {
	cfg = cfg.WithDefaults()
	if !cfg.ZeroMQ.Enabled || !cfg.ZeroMQForwardingEnabled() {
		return nil
	}
	return &ZeroMQMeshTransportAdapter{
		cfg:              cfg.ZeroMQ,
		serverKeyForPeer: zeroMQCurveServerKeyForPeer,
		acceptCh:         make(chan mesh.TransportConn, meshTransportAcceptQueue),
	}
}

// Start 启动libp2p传输和入站接受循环。
func (a *LibP2PMeshTransportAdapter) Start(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if a.transport != nil {
		a.mu.Unlock()
		return fmt.Errorf("libp2p mesh transport adapter is already started")
	}
	transport := newLibP2PTransport(a.cfg, a.clusterSecret, nil)
	transport.SetAccept(func(conn TransportConn) {
		enqueueMeshTransportConn(ctx, a.acceptCh, mesh.TransportLibP2P, conn)
	})
	a.transport = transport
	a.mu.Unlock()

	if err := transport.Start(ctx); err != nil {
		a.mu.Lock()
		if a.transport == transport {
			a.transport = nil
		}
		a.mu.Unlock()
		return err
	}
	go func() {
		<-ctx.Done()
		_ = a.Close()
	}()
	return nil
}

// Dial 通过libp2p传输向指定端点发起连接。
func (a *LibP2PMeshTransportAdapter) Dial(ctx context.Context, endpoint string) (mesh.TransportConn, error) {
	if a == nil {
		return nil, fmt.Errorf("libp2p mesh transport adapter is nil")
	}
	a.mu.Lock()
	transport := a.transport
	a.mu.Unlock()
	if transport == nil {
		return nil, fmt.Errorf("libp2p mesh transport adapter is not started")
	}
	conn, err := transport.Dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return wrapMeshTransportConn(mesh.TransportLibP2P, conn), nil
}

// Accept 返回接受通道。
func (a *LibP2PMeshTransportAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

// Kind 返回传输类型LibP2P。
func (a *LibP2PMeshTransportAdapter) Kind() mesh.TransportKind {
	return mesh.TransportLibP2P
}

// LocalCapabilities 返回本地传输能力。
func (a *LibP2PMeshTransportAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	capability := (&Config{LibP2P: a.cfg}).LibP2PTransportCapability()
	a.mu.Lock()
	transport := a.transport
	a.mu.Unlock()
	if transport != nil {
		if endpoints := transport.ListenAddrs(); len(endpoints) > 0 {
			capability.InboundEnabled = true
			capability.AdvertisedEndpoints = endpoints
		}
	}
	return mesh.CloneCapability(capability)
}

// Close 关闭libp2p传输。
func (a *LibP2PMeshTransportAdapter) Close() error {
	if a == nil {
		return nil
	}
	var err error
	a.closeOnce.Do(func() {
		a.mu.Lock()
		transport := a.transport
		a.transport = nil
		a.mu.Unlock()
		if transport != nil {
			err = transport.Close()
		}
	})
	return err
}

// Start 启动ZeroMQ监听器。
func (a *ZeroMQMeshTransportAdapter) Start(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !zeroMQEnabled() {
		return errZeroMQNotBuilt
	}
	if strings.TrimSpace(a.cfg.BindURL) == "" {
		return nil
	}
	a.mu.Lock()
	if a.listener != nil {
		a.mu.Unlock()
		return fmt.Errorf("zeromq mesh transport adapter is already started")
	}
	listener := NewZeroMQMuxListenerWithConfig(a.cfg.BindURL, a.cfg)
	listener.SetClusterAccept(func(conn TransportConn) {
		enqueueMeshTransportConn(ctx, a.acceptCh, mesh.TransportZeroMQ, conn)
	})
	a.listener = listener
	a.mu.Unlock()

	if err := listener.Start(ctx); err != nil {
		a.mu.Lock()
		if a.listener == listener {
			a.listener = nil
		}
		a.mu.Unlock()
		return err
	}
	go func() {
		<-ctx.Done()
		_ = a.Close()
	}()
	return nil
}

// Dial 通过ZeroMQ传输向指定端点发起连接。
func (a *ZeroMQMeshTransportAdapter) Dial(ctx context.Context, endpoint string) (mesh.TransportConn, error) {
	if a == nil {
		return nil, fmt.Errorf("zeromq mesh transport adapter is nil")
	}
	conn, err := newZeroMQDialerWithConfig(a.cfg, a.serverKeyForPeer).Dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return wrapMeshTransportConn(mesh.TransportZeroMQ, conn, endpoint), nil
}

// Accept 返回接受通道。
func (a *ZeroMQMeshTransportAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

// Kind 返回传输类型ZeroMQ。
func (a *ZeroMQMeshTransportAdapter) Kind() mesh.TransportKind {
	return mesh.TransportZeroMQ
}

// LocalCapabilities 返回本地ZeroMQ传输能力。
func (a *ZeroMQMeshTransportAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	capability := (&Config{ZeroMQ: a.cfg}).ZeroMQTransportCapability()
	return mesh.CloneCapability(capability)
}

// Close 关闭ZeroMQ监听器。
func (a *ZeroMQMeshTransportAdapter) Close() error {
	if a == nil {
		return nil
	}
	var err error
	a.closeOnce.Do(func() {
		a.mu.Lock()
		listener := a.listener
		a.listener = nil
		a.mu.Unlock()
		if listener != nil {
			err = listener.Close()
		}
	})
	return err
}

// enqueueMeshTransportConn 将连接的包装版本放入接受通道。
func enqueueMeshTransportConn(ctx context.Context, acceptCh chan mesh.TransportConn, kind mesh.TransportKind, conn TransportConn) {
	wrapped := wrapMeshTransportConn(kind, conn)
	select {
	case acceptCh <- wrapped:
	case <-ctxDone(ctx):
		closeTransport(conn, "shutdown")
	default:
		closeTransport(conn, "mesh accept queue full")
	}
}

// wrapMeshTransportConn 将TransportConn包装为mesh.TransportConn。
func wrapMeshTransportConn(kind mesh.TransportKind, conn TransportConn, remoteHint ...string) mesh.TransportConn {
	if kind == mesh.TransportUnspecified {
		kind = meshTransportKind(conn)
	}
	hint := ""
	if len(remoteHint) > 0 {
		hint = strings.TrimSpace(remoteHint[0])
	}
	return &meshTransportConn{
		conn:       conn,
		kind:       kind,
		remoteHint: hint,
	}
}

// meshTransportKind 从TransportConn推断网格传输类型。
func meshTransportKind(conn TransportConn) mesh.TransportKind {
	if conn == nil {
		return mesh.TransportUnspecified
	}
	switch conn.Transport() {
	case transportLibP2P:
		return mesh.TransportLibP2P
	case transportZeroMQ:
		return mesh.TransportZeroMQ
	default:
		return mesh.TransportUnspecified
	}
}

// libp2pRemoteAddrSuggestsRelay 检查libp2p远程地址是否暗示使用了中继。
func libp2pRemoteAddrSuggestsRelay(addr string) bool {
	if addr == "" {
		return false
	}
	lower := strings.ToLower(addr)
	return strings.Contains(lower, "/p2p-circuit") || strings.Contains(lower, "relay")
}

// Send 发送消息。
func (c *meshTransportConn) Send(ctx context.Context, envelope []byte) error {
	return c.conn.Send(ctx, envelope)
}

// SendOwned 发送消息（所有权转移版本）。
func (c *meshTransportConn) SendOwned(ctx context.Context, envelope []byte) error {
	if sender, ok := c.conn.(interface {
		SendOwned(context.Context, []byte) error
	}); ok {
		return sender.SendOwned(ctx, envelope)
	}
	return c.conn.Send(ctx, envelope)
}

// Receive 接收消息。
func (c *meshTransportConn) Receive(ctx context.Context) ([]byte, error) {
	return c.conn.Receive(ctx)
}

// Close 关闭连接。
func (c *meshTransportConn) Close() error {
	return c.conn.Close()
}

// RemoteNodeHint 返回远程节点的提示信息。
func (c *meshTransportConn) RemoteNodeHint() string {
	if c == nil || c.conn == nil {
		return ""
	}
	if c.remoteHint != "" {
		return c.remoteHint
	}
	remoteAddr := strings.TrimSpace(c.conn.RemoteAddr())
	if c.kind == mesh.TransportLibP2P && libp2pRemoteAddrSuggestsRelay(remoteAddr) {
		return remoteAddr
	}
	if identityConn, ok := c.conn.(libP2PIdentityConn); ok {
		if hint := strings.TrimSpace(identityConn.RemotePeerID()); hint != "" {
			return hint
		}
	}
	return remoteAddr
}

// Transport 返回网格传输类型。
func (c *meshTransportConn) Transport() mesh.TransportKind {
	if c == nil {
		return mesh.TransportUnspecified
	}
	return c.kind
}
