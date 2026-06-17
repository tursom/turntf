// Package proto 定义 turntf 各层协议使用的版本常量。
// 不同通信层次有独立的版本号，允许按需演进协议而不影响其他层级。
package proto

// ProtocolVersion 定义核心数据协议版本。
// 用于标识节点间数据交换所使用的协议格式。
const ProtocolVersion = "v1alpha15"

// SnapshotVersion 定义快照数据格式版本。
// 用于标识持久化快照的序列化格式，版本变更时需迁移数据。
const SnapshotVersion = "snapshot-v1alpha11"

// ClientProtocolVersion 定义客户端与服务端之间的通信协议版本。
// 客户端需声明此版本，服务端据此判断兼容性。
const ClientProtocolVersion = "client-v1alpha4"

// ZeroMQMuxProtocolVersion 定义 ZeroMQ 多路复用子协议版本。
// 用于 ZeroMQ 多路复用层的协议协商。
const ZeroMQMuxProtocolVersion = "zeromq-mux-v1"
