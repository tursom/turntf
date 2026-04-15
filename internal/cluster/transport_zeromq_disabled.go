//go:build !zeromq

package cluster

import (
	"context"
	"errors"

	internalproto "github.com/tursom/turntf/internal/proto"
)

var errZeroMQNotBuilt = errors.New("zeromq support was not built into this binary")

type zeroMQDialer struct{}

type zeroMQClusterListener struct{ mux *ZeroMQMuxListener }

type ZeroMQMuxListener struct {
	bindURL string
}

func zeroMQEnabled() bool {
	return false
}

func newZeroMQDialer() Dialer {
	return &zeroMQDialer{}
}

func newZeroMQDialerWithConfig(ZeroMQConfig, func(string) string) Dialer {
	return &zeroMQDialer{}
}

func newZeroMQListener(bindURL string) Listener {
	return &zeroMQClusterListener{mux: NewZeroMQMuxListener(bindURL)}
}

func NewZeroMQMuxListener(bindURL string) *ZeroMQMuxListener {
	return &ZeroMQMuxListener{bindURL: bindURL}
}

func NewZeroMQMuxListenerWithConfig(bindURL string, _ ZeroMQConfig) *ZeroMQMuxListener {
	return &ZeroMQMuxListener{bindURL: bindURL}
}

func (l *ZeroMQMuxListener) SetClusterAccept(func(TransportConn)) {}

func (l *ZeroMQMuxListener) SetClientAccept(func(TransportConn)) {}

func writeZeroMQMuxHello(context.Context, TransportConn, internalproto.ZeroMQMuxHello_Role) error {
	return errZeroMQNotBuilt
}

func (d *zeroMQDialer) Dial(context.Context, string) (TransportConn, error) {
	return nil, errZeroMQNotBuilt
}

func (l *zeroMQClusterListener) Start(context.Context, func(TransportConn)) error {
	return errZeroMQNotBuilt
}

func (l *zeroMQClusterListener) Close() error {
	return nil
}

func (l *ZeroMQMuxListener) Start(context.Context) error {
	return errZeroMQNotBuilt
}

func (l *ZeroMQMuxListener) Close() error {
	return nil
}
