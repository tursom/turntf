//go:build !zeromq

package cluster

import (
	"context"
	"errors"
)

var errZeroMQNotBuilt = errors.New("zeromq support was not built into this binary")

type zeroMQDialer struct{}

type zeroMQListener struct {
	bindURL string
}

func zeroMQEnabled() bool {
	return false
}

func newZeroMQDialer() Dialer {
	return &zeroMQDialer{}
}

func newZeroMQListener(bindURL string) Listener {
	return &zeroMQListener{bindURL: bindURL}
}

func (d *zeroMQDialer) Dial(context.Context, string) (TransportConn, error) {
	return nil, errZeroMQNotBuilt
}

func (l *zeroMQListener) Start(context.Context, func(TransportConn)) error {
	return errZeroMQNotBuilt
}

func (l *zeroMQListener) Close() error {
	return nil
}
