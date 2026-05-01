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

// WebSocket连接的I/O缓冲区大小（32KB）和读取限制（8MB）。
const websocketReadLimit = 8 << 20

const websocketIOBufferSize = 32 << 10

// errNonBinaryWebSocketFrame 在收到非二进制WebSocket帧时返回。
var errNonBinaryWebSocketFrame = errors.New("websocket transport received non-binary frame")

// websocketWriteBufferPool 是WebSocket写入缓冲区的对象池。
var websocketWriteBufferPool = &sync.Pool{
	New: func() any {
		return make([]byte, websocketIOBufferSize)
	},
}

// webSocketTransport 实现WebSocket传输的Dialer和HTTP升级器。
type webSocketTransport struct {
	upgrader     websocket.Upgrader
	dialer       *websocket.Dialer
	readLimit    int64
	readTimeout  time.Duration
	writeWait    time.Duration
	pingInterval time.Duration
}

// webSocketTransportConn 包装gorilla WebSocket连接，实现TransportConn接口。
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

// newWebSocketTransport 创建一个新的WebSocket传输实现。
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

// Dial 建立出站WebSocket连接到对等节点URL。
func (t *webSocketTransport) Dial(ctx context.Context, peerURL string) (TransportConn, error) {
	conn, _, err := t.dialer.DialContext(ctx, peerURL, nil)
	if err != nil {
		return nil, err
	}
	return t.wrapConn(conn, true), nil
}

// Upgrade 将HTTP请求升级为WebSocket连接（入站）。
func (t *webSocketTransport) Upgrade(w http.ResponseWriter, r *http.Request) (TransportConn, error) {
	conn, err := t.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return t.wrapConn(conn, false), nil
}

// wrapConn 包装gorilla WebSocket连接，配置读取限制、超时和ping/pong处理。
// 启动后台ping循环以保持连接活跃。
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

// Send 发送二进制WebSocket消息。
func (c *webSocketTransportConn) Send(ctx context.Context, payload []byte) error {
	return c.writeMessage(ctx, websocket.BinaryMessage, payload)
}

// Receive 阻塞直到收到一条二进制消息。非二进制帧会返回错误。
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

// Close 使用默认原因关闭WebSocket连接。
func (c *webSocketTransportConn) Close() error {
	return c.CloseWithReason("session closed")
}

// CloseWithReason 发送关闭帧并关闭底层连接。
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

// LocalAddr 返回本地网络地址。
func (c *webSocketTransportConn) LocalAddr() string {
	return c.localAddr
}

// RemoteAddr 返回远程网络地址。
func (c *webSocketTransportConn) RemoteAddr() string {
	return c.remoteAddr
}

// Direction 返回连接方向：inbound或outbound。
func (c *webSocketTransportConn) Direction() string {
	return c.direction
}

// Transport 返回传输类型字符串websocket。
func (c *webSocketTransportConn) Transport() string {
	return transportWebSocket
}

// pingLoop 定期发送ping帧以保持连接活跃。
// 如果在ping上失败则关闭连接。
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

// writeMessage 以二进制帧发送消息。支持上下文取消和写入截止时间。
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

// writeControl 发送WebSocket控制帧（ping/pong/close）。
func (c *webSocketTransportConn) writeControl(messageType int, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(messageType, payload, time.Now().Add(c.writeWait))
}

// writeDeadline 计算写入截止时间，优先使用上下文截止。
func writeDeadline(ctx context.Context, wait time.Duration) time.Time {
	deadline := time.Now().Add(wait)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

// ctxDone 返回上下文的Done通道，nil安全。
func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

// addrString 将net.Addr转换为字符串，nil安全。
func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}
