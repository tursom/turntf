package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const websocketReadLimit = 8 << 20

const websocketIOBufferSize = 32 << 10

var errNonBinaryWebSocketFrame = errors.New("websocket transport received non-binary frame")
var websocketWriteBufferPool = &sync.Pool{
	New: func() any {
		return make([]byte, websocketIOBufferSize)
	},
}

type webSocketTransport struct {
	upgrader     websocket.Upgrader
	dialer       *websocket.Dialer
	readLimit    int64
	readTimeout  time.Duration
	writeWait    time.Duration
	pingInterval time.Duration
}

type webSocketTransportConn struct {
	conn         *websocket.Conn
	direction    string
	localAddr    string
	remoteAddr   string
	writeWait    time.Duration
	pingInterval time.Duration

	writeMu   sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
}

func newWebSocketTransport() *webSocketTransport {
	dialer := *websocket.DefaultDialer
	dialer.ReadBufferSize = websocketIOBufferSize
	dialer.WriteBufferSize = websocketIOBufferSize
	dialer.WriteBufferPool = websocketWriteBufferPool
	return &webSocketTransport{
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(*http.Request) bool { return true },
			ReadBufferSize:  websocketIOBufferSize,
			WriteBufferSize: websocketIOBufferSize,
			WriteBufferPool: websocketWriteBufferPool,
		},
		dialer:       &dialer,
		readLimit:    websocketReadLimit,
		readTimeout:  readTimeout,
		writeWait:    writeWait,
		pingInterval: pingInterval,
	}
}

func (t *webSocketTransport) Dial(ctx context.Context, peerURL string) (TransportConn, error) {
	conn, _, err := t.dialer.DialContext(ctx, peerURL, nil)
	if err != nil {
		return nil, err
	}
	return t.wrapConn(conn, true), nil
}

func (t *webSocketTransport) Upgrade(w http.ResponseWriter, r *http.Request) (TransportConn, error) {
	conn, err := t.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return t.wrapConn(conn, false), nil
}

func (t *webSocketTransport) wrapConn(conn *websocket.Conn, outbound bool) TransportConn {
	conn.SetReadLimit(t.readLimit)
	_ = conn.SetReadDeadline(time.Now().Add(t.readTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(t.readTimeout))
	})

	direction := "inbound"
	if outbound {
		direction = "outbound"
	}
	wrapped := &webSocketTransportConn{
		conn:         conn,
		direction:    direction,
		localAddr:    addrString(conn.LocalAddr()),
		remoteAddr:   addrString(conn.RemoteAddr()),
		writeWait:    t.writeWait,
		pingInterval: t.pingInterval,
		done:         make(chan struct{}),
	}
	go wrapped.pingLoop()
	return wrapped
}

func (c *webSocketTransportConn) Send(ctx context.Context, payload []byte) error {
	return c.writeMessage(ctx, websocket.BinaryMessage, payload)
}

func (c *webSocketTransportConn) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctxDone(ctx):
		return nil, ctx.Err()
	default:
	}

	messageType, data, err := c.conn.ReadMessage()
	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if messageType != websocket.BinaryMessage {
		return nil, errNonBinaryWebSocketFrame
	}
	return data, nil
}

func (c *webSocketTransportConn) Close() error {
	return c.CloseWithReason("session closed")
}

func (c *webSocketTransportConn) CloseWithReason(reason string) error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		err = c.writeControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason))
		if closeErr := c.conn.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}

func (c *webSocketTransportConn) LocalAddr() string {
	return c.localAddr
}

func (c *webSocketTransportConn) RemoteAddr() string {
	return c.remoteAddr
}

func (c *webSocketTransportConn) Direction() string {
	return c.direction
}

func (c *webSocketTransportConn) Transport() string {
	return transportWebSocket
}

func (c *webSocketTransportConn) pingLoop() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.writeMessage(context.Background(), websocket.PingMessage, nil); err != nil {
				_ = c.Close()
				return
			}
		}
	}
}

func (c *webSocketTransportConn) writeMessage(ctx context.Context, messageType int, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errSessionClosed
	default:
	}

	if err := c.conn.SetWriteDeadline(writeDeadline(ctx, c.writeWait)); err != nil {
		return err
	}
	return c.conn.WriteMessage(messageType, payload)
}

func (c *webSocketTransportConn) writeControl(messageType int, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(messageType, payload, time.Now().Add(c.writeWait))
}

func writeDeadline(ctx context.Context, wait time.Duration) time.Time {
	deadline := time.Now().Add(wait)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}
