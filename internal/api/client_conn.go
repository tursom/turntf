package api

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	clientWSWriteWait   = 10 * time.Second
	clientWSReadTimeout = 45 * time.Second
)

var errNonBinaryClientFrame = errors.New("client transport received non-binary frame")

type clientTransportConn interface {
	Send(ctx context.Context, payload []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
	RemoteAddr() string
	Transport() string
}

type clientWSConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func newClientWSConn(conn *websocket.Conn) *clientWSConn {
	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(clientWSReadTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(clientWSReadTimeout))
	})
	return &clientWSConn{conn: conn}
}

func (c *clientWSConn) Send(ctx context.Context, payload []byte) error {
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

	if err := c.conn.SetWriteDeadline(clientWriteDeadline(ctx)); err != nil {
		return err
	}
	return c.conn.WriteMessage(websocket.BinaryMessage, payload)
}

func (c *clientWSConn) Receive(ctx context.Context) ([]byte, error) {
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
		return nil, errNonBinaryClientFrame
	}
	return data, nil
}

func (c *clientWSConn) Close() error {
	return c.conn.Close()
}

func (c *clientWSConn) RemoteAddr() string {
	if c.conn == nil || c.conn.RemoteAddr() == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

func (c *clientWSConn) Transport() string {
	return "ws"
}

func clientWriteDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(clientWSWriteWait)
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
