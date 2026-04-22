package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/tursom/turntf/internal/mesh"
)

const meshTransportAcceptQueue = 128

type LibP2PMeshTransportAdapter struct {
	cfg           LibP2PConfig
	clusterSecret string
	acceptCh      chan mesh.TransportConn

	mu        sync.Mutex
	transport *libP2PTransport
	closeOnce sync.Once
}

type ZeroMQMeshTransportAdapter struct {
	cfg              ZeroMQConfig
	serverKeyForPeer func(string) string
	acceptCh         chan mesh.TransportConn

	mu        sync.Mutex
	listener  *ZeroMQMuxListener
	closeOnce sync.Once
}

type meshTransportConn struct {
	conn       TransportConn
	kind       mesh.TransportKind
	remoteHint string
}

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

func (a *LibP2PMeshTransportAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

func (a *LibP2PMeshTransportAdapter) Kind() mesh.TransportKind {
	return mesh.TransportLibP2P
}

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

func (a *ZeroMQMeshTransportAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

func (a *ZeroMQMeshTransportAdapter) Kind() mesh.TransportKind {
	return mesh.TransportZeroMQ
}

func (a *ZeroMQMeshTransportAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	capability := (&Config{ZeroMQ: a.cfg}).ZeroMQTransportCapability()
	return mesh.CloneCapability(capability)
}

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

func (c *meshTransportConn) Send(ctx context.Context, envelope []byte) error {
	return c.conn.Send(ctx, envelope)
}

func (c *meshTransportConn) Receive(ctx context.Context) ([]byte, error) {
	return c.conn.Receive(ctx)
}

func (c *meshTransportConn) Close() error {
	return c.conn.Close()
}

func (c *meshTransportConn) RemoteNodeHint() string {
	if c == nil || c.conn == nil {
		return ""
	}
	if c.remoteHint != "" {
		return c.remoteHint
	}
	if identityConn, ok := c.conn.(libP2PIdentityConn); ok {
		if hint := strings.TrimSpace(identityConn.RemotePeerID()); hint != "" {
			return hint
		}
	}
	return strings.TrimSpace(c.conn.RemoteAddr())
}

func (c *meshTransportConn) Transport() mesh.TransportKind {
	if c == nil {
		return mesh.TransportUnspecified
	}
	return c.kind
}
