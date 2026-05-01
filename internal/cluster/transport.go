// Package cluster 实现了一个P2P集群管理层，用于多节点之间的状态复制、
// 时钟同步、对等节点发现和网状路由。核心结构Manager协调三个传输后端
// (WebSocket、ZeroMQ、libp2p)之上的会话管理、事件复制和快照同步。
//
// 集群模块的主要功能包括：
//   - 传输抽象：通过TransportConn/Dialer/Listener接口统一三种传输后端
//   - 时钟保护：基于分布式时钟状态机控制写入门控，防止脑裂场景下的数据损坏
//   - 对等发现：通过成员资格更新协议自动发现和连接新的对等节点
//   - 事件复制：将存储事件广播到所有已连接的对等节点，支持批次合并
//   - 快照同步：通过摘要/分块交换实现节点间状态的最终一致性
//   - 网状路由：通过覆盖网络在中继节点之间转发数据包，支持多种流量类别
//   - 在线状态：跟踪整个集群中的用户会话位置，支持跨节点查询
package cluster

import "context"

// 传输后端名称常量。
const (
	transportWebSocket = "websocket"
	transportZeroMQ    = "zeromq"
	transportLibP2P    = "libp2p"
)

// TransportConn 表示集群节点之间的单个传输连接。
// 每个连接封装了底层的网络传输（WebSocket、ZeroMQ或libp2p），
// 提供统一的消息发送和接收接口。
type TransportConn interface {
	// Send 发送一条二进制消息到对端。ctx可用于取消发送操作。
	Send(ctx context.Context, payload []byte) error
	// Receive 阻塞直到收到一条消息或ctx被取消。
	Receive(ctx context.Context) ([]byte, error)
	// Close 关闭连接，释放底层资源。
	Close() error
	// LocalAddr 返回连接的本地地址。
	LocalAddr() string
	// RemoteAddr 返回连接的远程地址。
	RemoteAddr() string
	// Direction 返回连接方向："inbound"表示由对端发起的连接，"outbound"表示本节点发起的连接。
	Direction() string
	// Transport 返回传输类型：websocket、zeromq或libp2p。
	Transport() string
}

// Dialer 用于向对等节点发起出站连接。
type Dialer interface {
	// Dial 向指定peerURL发起连接。peerURL的scheme决定了使用哪种传输协议。
	Dial(ctx context.Context, peerURL string) (TransportConn, error)
}

// Listener 用于接受来自其他对等节点的入站连接。
type Listener interface {
	// Start 启动监听器，将每个接受的连接传递给accept回调函数。
	Start(ctx context.Context, accept func(TransportConn)) error
	// Close 停止监听器，释放绑定的端口和资源。
	Close() error
}

// closeReasonTransport 是支持带原因关闭的连接接口。
type closeReasonTransport interface {
	CloseWithReason(reason string) error
}

// closeTransport 安全关闭一个传输连接，优先使用带原因的关闭方式。
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
