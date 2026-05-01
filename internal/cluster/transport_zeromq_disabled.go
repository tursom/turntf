//go:build !zeromq

package cluster

import (
	"context"
	"errors"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// errZeroMQNotBuilt 在二进制未编译ZeroMQ支持时返回此错误。
var errZeroMQNotBuilt = errors.New("zeromq support was not built into this binary")

// zeroMQDialer 是ZeroMQ禁用时的拨号器存根。
type zeroMQDialer struct{}

// zeroMQClusterListener 是ZeroMQ禁用时的集群监听器存根。
type zeroMQClusterListener struct{ mux *ZeroMQMuxListener }

// ZeroMQMuxListener 是ZeroMQ禁用时的多路复用监听器存根。
type ZeroMQMuxListener struct {
	bindURL string
}

// zeroMQEnabled 当ZeroMQ编译标签未启用时返回false。
func zeroMQEnabled() bool {
	return false
}

// newZeroMQDialer 返回一个存根拨号器。
func newZeroMQDialer() Dialer {
	return &zeroMQDialer{}
}

// newZeroMQDialerWithConfig 返回一个存根拨号器（接受配置，用于接口兼容）。
func newZeroMQDialerWithConfig(ZeroMQConfig, func(string) string) Dialer {
	return &zeroMQDialer{}
}

// newZeroMQListener 返回一个存根监听器。
func newZeroMQListener(bindURL string) Listener {
	return &zeroMQClusterListener{mux: NewZeroMQMuxListener(bindURL)}
}

// NewZeroMQMuxListener 返回一个存根多路复用监听器。
func NewZeroMQMuxListener(bindURL string) *ZeroMQMuxListener {
	return &ZeroMQMuxListener{bindURL: bindURL}
}

// NewZeroMQMuxListenerWithConfig 返回一个存根多路复用监听器（接受配置，用于接口兼容）。
func NewZeroMQMuxListenerWithConfig(bindURL string, _ ZeroMQConfig) *ZeroMQMuxListener {
	return &ZeroMQMuxListener{bindURL: bindURL}
}

// SetClusterAccept 设置集群连接接受回调（存根无操作）。
func (l *ZeroMQMuxListener) SetClusterAccept(func(TransportConn)) {}

// SetClientAccept 设置客户端连接接受回调（存根无操作）。
func (l *ZeroMQMuxListener) SetClientAccept(func(TransportConn)) {}

// writeZeroMQMuxHello 在禁用ZeroMQ时返回errZeroMQNotBuilt。
func writeZeroMQMuxHello(context.Context, TransportConn, internalproto.ZeroMQMuxHello_Role) error {
	return errZeroMQNotBuilt
}

// Dial 存根实现，始终返回errZeroMQNotBuilt。
func (d *zeroMQDialer) Dial(context.Context, string) (TransportConn, error) {
	return nil, errZeroMQNotBuilt
}

// Start 存根实现，始终返回errZeroMQNotBuilt。
func (l *zeroMQClusterListener) Start(context.Context, func(TransportConn)) error {
	return errZeroMQNotBuilt
}

// Close 存根实现，成功无操作。
func (l *zeroMQClusterListener) Close() error {
	return nil
}

// Start 存根实现，始终返回errZeroMQNotBuilt。
func (l *ZeroMQMuxListener) Start(context.Context) error {
	return errZeroMQNotBuilt
}

// Close 存根实现，成功无操作。
func (l *ZeroMQMuxListener) Close() error {
	return nil
}
