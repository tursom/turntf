package cluster

import "context"

const transportWebSocket = "websocket"
const transportZeroMQ = "zeromq"

type TransportConn interface {
	Send(ctx context.Context, payload []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
	LocalAddr() string
	RemoteAddr() string
	Direction() string
	Transport() string
}

type Dialer interface {
	Dial(ctx context.Context, peerURL string) (TransportConn, error)
}

type Listener interface {
	Start(ctx context.Context, accept func(TransportConn)) error
	Close() error
}

type closeReasonTransport interface {
	CloseWithReason(reason string) error
}

func closeTransport(conn TransportConn, reason string) {
	if conn == nil {
		return
	}
	if closer, ok := conn.(closeReasonTransport); ok {
		_ = closer.CloseWithReason(reason)
		return
	}
	_ = conn.Close()
}
