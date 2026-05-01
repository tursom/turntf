package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/tursom/turntf/internal/mesh"
)

// meshInboundAdapter 是由Manager拥有的传输适配器。
// 它不创建自己的监听器；Manager拥有的传输在入站连接到达时调用InjectInbound，
// 适配器将其转发到网格运行时的接受通道。
type meshInboundAdapter struct {
	kind     mesh.TransportKind
	acceptCh chan mesh.TransportConn
	caps     *mesh.TransportCapability
	dialer   func(ctx context.Context, endpoint string) (TransportConn, error)

	mu        sync.Mutex
	ctx       context.Context
	closeOnce sync.Once
}

// newMeshInboundAdapter 创建一个新的入站适配器。
func newMeshInboundAdapter(kind mesh.TransportKind, caps *mesh.TransportCapability, dialer func(ctx context.Context, endpoint string) (TransportConn, error)) *meshInboundAdapter {
	return &meshInboundAdapter{
		kind:     kind,
		acceptCh: make(chan mesh.TransportConn, meshTransportAcceptQueue),
		caps:     caps,
		dialer:   dialer,
	}
}

// Start 存储上下文以供InjectInbound使用。
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

// Dial 使用适配器的拨号器向指定端点发起出站连接。
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

// Accept 返回接受通道，网格运行时从此通道获取新连接。
func (a *meshInboundAdapter) Accept() <-chan mesh.TransportConn {
	if a == nil {
		return nil
	}
	return a.acceptCh
}

// Kind 返回传输类型。
func (a *meshInboundAdapter) Kind() mesh.TransportKind {
	return a.kind
}

// LocalCapabilities 返回传输能力的克隆副本。
func (a *meshInboundAdapter) LocalCapabilities() *mesh.TransportCapability {
	if a == nil {
		return nil
	}
	return mesh.CloneCapability(a.caps)
}

// Close 关闭适配器。
func (a *meshInboundAdapter) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {})
	return nil
}

// InjectInbound 将原始入站TransportConn路由到适配器的接受通道。
// 如果适配器尚未启动或队列已满，则返回false（调用者应关闭连接）。
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
