// Package app 提供应用程序层的核心类型定义，包括集群运维状态数据结构
// 和通用错误变量。该包定义了集群节点状态、发现子系统状态、Mesh 网络状态、
// 对等节点连接状态等数据模型，是整个集群监控和运维的基础类型集合。
package app

import "errors"

var (
	// ErrClockNotSynchronized 表示集群时钟尚未同步。
	// 当节点无法确认其时钟相对于集群参考时钟的偏移量在可信范围内时，
	// 返回此错误。此时节点不应参与需要时间一致性的集群操作，
	// 直到时钟同步完成。
	ErrClockNotSynchronized = errors.New("cluster clock not synchronized")
	// ErrServiceUnavailable 表示服务当前不可用。
	// 当节点处于非运行状态（如正在关闭、尚未就绪、或发生严重故障）
	// 无法处理请求时返回此错误。调用方应进行重试或切换到其他节点。
	ErrServiceUnavailable = errors.New("service unavailable")
)
