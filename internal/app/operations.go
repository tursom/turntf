// Package app 提供应用程序层的核心类型定义，包括集群运维状态数据结构
// 和通用错误变量。该包定义了集群节点状态、发现子系统状态、Mesh 网络状态、
// 对等节点连接状态等数据模型，是整个集群监控和运维的基础类型集合。
package app

import "time"

// ClusterStatus 描述集群中单个节点的完整运行时状态快照。
// 包含节点标识、消息窗口、写入门控、时钟同步、对等节点连接、
// 发现子系统和 Mesh 网络等全部子系统的最新状态信息。
type ClusterStatus struct {
	// NodeID 集群节点唯一标识符
	NodeID int64
	// MessageWindowSize 消息窗口大小，即批量确认的消息数上限
	MessageWindowSize int
	// WriteGateReady 写入门控是否就绪。为 true 时允许节点执行写入操作，
	// 为 false 时写入被阻塞（如尚未完成追赶快照同步）
	WriteGateReady bool
	// ClockState 时钟同步状态，如 synchronized、unsynchronized、unknown 等
	ClockState string
	// ClockReason 时钟状态的原因说明，描述当前状态的具体原因
	ClockReason string
	// LastTrustedClockSync 最后一次可信时钟同步的时间。为 nil 表示从未完成过可信同步
	LastTrustedClockSync *time.Time
	// ClockTransitions 时钟状态转换记录列表，每次状态切换的记录
	ClockTransitions []ClockStateTransition
	// Peers 对等节点状态列表，每个元素描述一个对等连接的信息
	Peers []ClusterPeerStatus
	// Discovery 发现子系统的当前状态
	Discovery ClusterDiscoveryStatus
	// Mesh Mesh 网络的当前状态
	Mesh ClusterMeshStatus
}

// ClusterDiscoveryStatus 描述节点发现子系统的运行时状态。
// 涵盖节点发现计数、成员变更统计、ZeroMQ 和 libp2p 两种传输协议的配置与运行状况。
type ClusterDiscoveryStatus struct {
	// DiscoveredPeers 已发现的节点总数
	DiscoveredPeers int
	// DynamicPeers 通过动态发现（非静态配置）发现的节点数
	DynamicPeers int
	// MembershipUpdatesSent 已发送的成员变更通知数
	MembershipUpdatesSent uint64
	// MembershipUpdatesRecv 已接收的成员变更通知数
	MembershipUpdatesRecv uint64
	// RejectedTotal 被拒绝的成员变更申请总数（如不兼容版本、无效签名等）
	RejectedTotal uint64
	// PersistFailuresTotal 持久化成员信息失败的累计次数
	PersistFailuresTotal uint64
	// PeersByState 按发现状态（如 discovered、connecting、connected）分组的节点计数字典
	PeersByState map[string]int
	// PeersByScheme 按连接协议方案（如 zmq、libp2p）分组的节点计数字典
	PeersByScheme map[string]int
	// ZeroMQMode ZeroMQ 运行模式，如 bind、connect、relay
	ZeroMQMode string
	// ZeroMQSecurity ZeroMQ 安全配置，如 none（不加密）、curve（CurveZMQ 加密）
	ZeroMQSecurity string
	// ZeroMQListenerRunning ZeroMQ 监听器是否正在运行
	ZeroMQListenerRunning bool
	// LibP2PMode libp2p 运行模式，如 relay、direct
	LibP2PMode string
	// LibP2PPeerID libp2p 节点标识符（Peer ID）
	LibP2PPeerID string
	// LibP2PListenAddrs libp2p 监听地址列表
	LibP2PListenAddrs []string
	// LibP2PVerifiedAddrs libp2p 已验证的外部可连接地址列表
	LibP2PVerifiedAddrs []string
	// LibP2PDHTEnabled libp2p 分布式哈希表（DHT）是否启用
	LibP2PDHTEnabled bool
	// LibP2PDHTBootstrapped libp2p DHT 是否已完成引导并成功加入全局网络
	LibP2PDHTBootstrapped bool
	// LibP2PGossipSubTopic libp2p GossipSub 订阅的主题名称
	LibP2PGossipSubTopic string
	// LibP2PGossipSubPeers libp2p GossipSub 的订阅对等节点数量
	LibP2PGossipSubPeers int
	// LibP2PRelayEnabled libp2p 中继（Relay）传输是否启用
	LibP2PRelayEnabled bool
	// LibP2PHolePunching libp2p NAT 穿透（Hole Punching）是否启用
	LibP2PHolePunching bool
}

// ClusterMeshStatus 描述集群 Mesh 网络的运行时状态。
// Mesh 网络负责节点间的流量转发、路由决策和桥接通信等功能。
type ClusterMeshStatus struct {
	// Enabled Mesh 网络是否启用
	Enabled bool
	// ForwardingEnabled 跨节点流量转发是否启用
	ForwardingEnabled bool
	// BridgeEnabled Mesh 桥接是否启用，用于连接多个不直连的 Mesh 网络
	BridgeEnabled bool
	// NodeFeeWeight 节点费用权重，用于路由成本计算和路径选择
	NodeFeeWeight int64
	// TopologyGeneration 拓扑世代号，每次拓扑结构变更时递增
	TopologyGeneration uint64
	// TransportCapabilities 当前节点支持的各传输协议能力列表
	TransportCapabilities []ClusterMeshTransportCapability
	// TrafficRules 已配置的流量规则列表，定义不同流量类别的处置策略
	TrafficRules []ClusterMeshTrafficRule
	// Routes 当前路由表，包含到各目标节点的可用路径信息
	Routes []ClusterMeshRoute
	// Metrics Mesh 网络的各类统计指标
	Metrics ClusterMeshMetrics
}

// ClusterMeshTransportCapability 描述一种传输协议的能力配置。
type ClusterMeshTransportCapability struct {
	// Transport 传输协议名称，如 tcp、quic、webrtc
	Transport string
	// InboundEnabled 入站传输是否启用（允许接收来自该传输协议的连接）
	InboundEnabled bool
	// OutboundEnabled 出站传输是否启用（允许通过该传输协议发起连接）
	OutboundEnabled bool
	// NativeRelayClientEnabled 原生中继客户端功能是否启用
	NativeRelayClientEnabled bool
	// NativeRelayServiceEnabled 原生中继服务端功能是否启用
	NativeRelayServiceEnabled bool
	// AdvertisedEndpoints 对外广播的端点地址列表，供其他节点连接本节点
	AdvertisedEndpoints []string
}

// ClusterMeshTrafficRule 定义某类流量的处置策略。
type ClusterMeshTrafficRule struct {
	// TrafficClass 流量类别，如 normal、priority、bulk、control
	TrafficClass string
	// Disposition 处置策略，如 allow（允许）、deny（拒绝）、defer（延迟处理）
	Disposition string
}

// ClusterMeshRoute 描述到达某个目标节点的一条路由信息。
type ClusterMeshRoute struct {
	// DestinationNodeID 目标节点 ID
	DestinationNodeID int64
	// TrafficClass 该路由适用的流量类别
	TrafficClass string
	// Reachable 目标节点是否可通过此路由到达
	Reachable bool
	// NextHopNodeID 下一跳节点 ID（直连的对等节点）
	NextHopNodeID int64
	// OutboundTransport 出站使用的传输协议名称
	OutboundTransport string
	// PathClass 路径类别，如 direct（直连）、relay（中继）、bridge（桥接）
	PathClass string
	// EstimatedCost 该路由的估算成本，用于路由选择决策
	EstimatedCost int64
	// TopologyGeneration 此路由所属的拓扑世代号，用于判断路由是否过期
	TopologyGeneration uint64
}

// ClusterMeshMetrics 包含 Mesh 网络的各类统计指标采样序列。
type ClusterMeshMetrics struct {
	// ForwardedPackets 转发数据包数量的历史采样序列
	ForwardedPackets []ClusterMeshMetricSample
	// ForwardedBytes 转发字节数量的历史采样序列
	ForwardedBytes []ClusterMeshMetricSample
	// RoutingNoPath 因无可用路由而丢弃的数据包数量的历史采样序列
	RoutingNoPath []ClusterMeshMetricSample
	// DecisionCost 路由决策成本的历史采样序列
	DecisionCost []ClusterMeshCostSample
	// BridgeForwards 经过桥接转发的数据包数量的历史采样序列
	BridgeForwards []ClusterMeshMetricSample
}

// ClusterMeshMetricSample 表示一个数值型指标的时序采样点。
type ClusterMeshMetricSample struct {
	// TrafficClass 该采样点对应的流量类别
	TrafficClass string
	// PathClass 该采样点对应的路径类别
	PathClass string
	// Value 采样值
	Value uint64
}

// ClusterMeshCostSample 表示一个路由成本的时序采样点。
type ClusterMeshCostSample struct {
	// TrafficClass 该采样点对应的流量类别
	TrafficClass string
	// Value 成本值
	Value int64
}

// LoggedInUserSummary 描述集群中当前登录的用户会话信息。
type LoggedInUserSummary struct {
	// NodeID 用户登录会话所在的节点 ID
	NodeID int64
	// UserID 用户 ID
	UserID int64
	// Username 用户名
	Username string
	// LoginName 登录名
	LoginName string
}

// ClusterPeerOriginStatus 描述对等节点上一个来源（Origin）的事件同步状态。
// 每个来源表示一个需要从远端节点同步的事件流分支，用于增量事件同步。
type ClusterPeerOriginStatus struct {
	// OriginNodeID 来源节点 ID，即事件产生的原始节点
	OriginNodeID int64
	// RemoteLastEventID 远端节点的最后事件 ID，用于判断本地追赶进度
	RemoteLastEventID uint64
	// PendingCatchup 是否正在追赶同步中（本地事件落后于远端）
	PendingCatchup bool
}

// ClusterPeerStatus 描述集群中对等节点的完整连接和同步状态。
type ClusterPeerStatus struct {
	// NodeID 对等节点 ID
	NodeID int64
	// ConfiguredURL 配置文件中指定的节点连接 URL
	ConfiguredURL string
	// Transport 当前使用的传输协议
	Transport string
	// Source 节点来源，config（静态配置）或 discovery（动态发现）
	Source string
	// DiscoveredURL 通过动态发现获取的节点实际 URL
	DiscoveredURL string
	// DiscoveryState 节点发现状态：discovered、connecting、connected、failed
	DiscoveryState string
	// LastDiscoveredAt 最后一次成功发现该节点的时间
	LastDiscoveredAt *time.Time
	// LastConnectedAt 最后一次成功与该节点建立连接的时间
	LastConnectedAt *time.Time
	// LastDiscoveryError 最近一次发现或连接过程中产生的错误描述
	LastDiscoveryError string
	// Connected 当前是否已与对等节点建立连接
	Connected bool
	// SessionDirection 会话方向：inbound（对端主动发起连接）或 outbound（本端主动发起连接）
	SessionDirection string
	// Origins 来源（事件源）状态列表，每个 Origin 对应一个待同步的事件流分支
	Origins []ClusterPeerOriginStatus
	// PendingSnapshotPartitions 待处理的快照分片数量
	PendingSnapshotPartitions int
	// RemoteSnapshotVersion 远端节点的快照版本标识
	RemoteSnapshotVersion string
	// RemoteMessageWindowSize 远端节点的消息窗口大小
	RemoteMessageWindowSize int
	// ClockState 远端节点的时钟同步状态
	ClockState string
	// ClockOffsetMs 本端与远端之间的时钟偏移量，单位为毫秒
	ClockOffsetMs int64
	// ClockUncertaintyMs 时钟偏移测量的不确定度，单位为毫秒
	ClockUncertaintyMs int64
	// ClockFailures 与该节点进行时钟同步的失败累计次数
	ClockFailures uint64
	// LastClockError 最近一次时钟同步过程中产生的错误描述
	LastClockError string
	// LastClockSync 最后一次时钟同步的时间
	LastClockSync *time.Time
	// LastCredibleClockSync 最后一次被判定为可信的时钟同步时间
	LastCredibleClockSync *time.Time
	// TrustedForOffset 该节点是否被信任用于时钟偏移校准
	TrustedForOffset bool
	// SnapshotDigestsSentTotal 向该节点发送的快照摘要消息总数
	SnapshotDigestsSentTotal uint64
	// SnapshotDigestsRecvTotal 从该节点接收的快照摘要消息总数
	SnapshotDigestsRecvTotal uint64
	// SnapshotChunksSentTotal 向该节点发送的快照分片总数
	SnapshotChunksSentTotal uint64
	// SnapshotChunksRecvTotal 从该节点接收的快照分片总数
	SnapshotChunksRecvTotal uint64
	// LastSnapshotDigestAt 最后一次发送或接收快照摘要的时间
	LastSnapshotDigestAt *time.Time
	// LastSnapshotChunkAt 最后一次发送或接收快照分片的时间
	LastSnapshotChunkAt *time.Time
}

// ClockStateTransition 记录时钟状态的一次转换事件。
type ClockStateTransition struct {
	// FromState 转换前的时钟状态
	FromState string
	// ToState 转换后的时钟状态
	ToState string
	// Reason 触发本次状态转换的原因
	Reason string
	// Total 该类型转换事件累计发生的次数
	Total uint64
}
