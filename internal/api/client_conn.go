// Package api 提供 turntf 消息系统的对外接口层，包括 REST HTTP API、WebSocket/ZeroMQ
// 客户端传输、RPC 处理、消息推送、事件分发、集群运维可观测性等功能。
//
// 架构层次（由底向上）：
//   - 传输层（client_conn.go）：统一 WebSocket 和 ZeroMQ 的连接抽象
//   - 协议转换（client_proto.go）：store 内部类型与 protobuf 外部类型互转
//   - 服务层（service.go）：核心业务逻辑，封装 store 操作和事件发布
//   - HTTP 层（http.go）：REST 路由、认证、会话管理
//   - 会话层（client_session.go, client_ws.go）：客户端连接生命周期
//   - RPC 层（client_ws_rpc.go, client_dispatch.go）：protobuf 信封分发与处理
//   - 推送层（client_push.go, client_dispatcher_shared.go）：持久化消息推送与事件广播
//   - 运维层（operations.go）：集群状态、节点、指标导出
package api

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// clientWSWriteWait WebSocket 写入超时时间
	clientWSWriteWait = 10 * time.Second
	// clientWSReadTimeout WebSocket 读取超时时间，同时用于 pong 消息的心跳重置
	clientWSReadTimeout = 45 * time.Second
)

// errNonBinaryClientFrame 客户端发送了非二进制帧时返回的错误。
// 客户端协议要求所有 WebSocket 消息均使用二进制帧（protobuf 编码）。
var errNonBinaryClientFrame = errors.New("client transport received non-binary frame")

// clientTransportConn 定义客户端传输连接的统一接口，同时支持 WebSocket 和 ZeroMQ 两种传输方式。
// 这使得上层会话管理（clientWSSession）无需感知底层传输差异。
type clientTransportConn interface {
	Send(ctx context.Context, payload []byte) error    // 发送二进制消息
	Receive(ctx context.Context) ([]byte, error)       // 接收二进制消息
	Close() error                                       // 关闭连接
	RemoteAddr() string                                 // 对端地址（用于日志和会话标识）
	Transport() string                                  // 传输类型标识："ws" 或 "zmq"
}

// clientWSConn 是 clientTransportConn 的 WebSocket 实现，包装 gorilla/websocket.Conn。
type clientWSConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex // 保护并发写入，gorilla/websocket 要求写入串行化
}

// newClientWSConn 从 HTTP 升级后的 WebSocket 连接创建 clientWSConn。
// 设置读取大小限制（1MB）、初始读取超时以及 pong 处理器（用于心跳保活）。
func newClientWSConn(conn *websocket.Conn) *clientWSConn {
	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(clientWSReadTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(clientWSReadTimeout))
	})
	return &clientWSConn{conn: conn}
}

// Send 发送二进制消息。使用互斥锁保证串行写入，写入超时取 ctx 截止时间和 clientWSWriteWait 中的较小值。
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

// Receive 接收下一条消息，只接受二进制帧。当 ctx 取消时优先返回 ctx 错误。
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

// Close 关闭底层 WebSocket 连接。
func (c *clientWSConn) Close() error {
	return c.conn.Close()
}

// RemoteAddr 返回对端网络地址的字符串表示。
func (c *clientWSConn) RemoteAddr() string {
	if c.conn == nil || c.conn.RemoteAddr() == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

// Transport 返回传输类型标识，WebSocket 实现固定返回 "ws"。
func (c *clientWSConn) Transport() string {
	return "ws"
}

// clientWriteDeadline 计算写入截止时间：取 clientWSWriteWait 和 ctx 截止时间中的较小值。
// 这确保写入不会被无限阻塞，同时尊重上层请求的取消信号。
func clientWriteDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(clientWSWriteWait)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

// ctxDone 安全地从 ctx 获取 Done channel，nil ctx 返回 nil channel（永远不会被 select 选中）。
func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}
