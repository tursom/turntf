package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/tursom/turntf/internal/mesh"
)

// meshInboundAdapter is a transport adapter owned by the Manager. It does
// not create its own listener; Manager-owned transports call
// InjectInbound when an inbound connection arrives, and the adapter
// forwards it to the mesh runtime's accept channel.
type meshInboundAdapter struct {
	kind     mesh.TransportKind
	acceptCh chan mesh.TransportConn
	caps     *mesh.TransportCapability
	dialer   func(ctx context.Context, endpoint string) (TransportConn, error)

	mu        sync.Mutex
	ctx       context.Context
	closeOnce sync.Once
}

func newMeshInboundAdapter(kind mesh.TransportKind, caps *mesh.TransportCapability, dialer func(ctx context.Context, endpoint string) (TransportConn, error)) *meshInboundAdapter {
	return &meshInboundAdapter{
		kind:     kind,
		acceptCh: make(chan mesh.TransportConn, meshTransportAcceptQueue),
		caps:     caps,
		dialer:   dialer,
	}
}

func (a *meshInboundAdapter) Start(ctx context.Context) error {
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

func (a *meshInboundAdapter) Dial(ctx context.Context, endpoint string) (mesh.TransportConn, error) {
	if a == nil || a.dialer == nil {
		return nil, fmt.Errorf("mesh adapter for %v has no dialer", a.kind)
	}
	conn, err := a.dialer(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if a.kind == mesh.TransportLibP2P {
		return wrapMeshTransportConn(a.kind, conn), nil
	}
	return wrapMeshTransportConn(a.kind, conn, endpoint), nil
}

func (a *meshInboundAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

func (a *meshInboundAdapter) Kind() mesh.TransportKind {
	return a.kind
}

func (a *meshInboundAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	return mesh.CloneCapability(a.caps)
}

func (a *meshInboundAdapter) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {})
	return nil
}

// InjectInbound routes a raw inbound TransportConn into the adapter's
// accept channel. Returns false if the adapter has not been started yet
// or the queue is full (caller should close the conn in that case).
func (a *meshInboundAdapter) InjectInbound(conn TransportConn) bool {
	if a == nil || conn == nil {
		return false
	}
	a.mu.Lock()
	ctx := a.ctx
	a.mu.Unlock()
	if ctx == nil {
		return false
	}
	wrapped := wrapMeshTransportConn(a.kind, conn)
	select {
	case a.acceptCh <- wrapped:
		return true
	case <-ctxDone(ctx):
		closeTransport(conn, "shutdown")
		return false
	default:
		closeTransport(conn, "mesh accept queue full")
		return false
	}
}
