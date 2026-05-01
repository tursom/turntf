package mesh

import (
	"context"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// ProtocolVersion 是节点建立连接时握手的线缆协议版本。
// 不匹配的版本会被拒绝。
const ProtocolVersion = "mesh-v1alpha2"

// ---------------- 线缆消息类型别名 ----------------
// 以下类型别名重新导出 protobuf 生成的类型，使用更短的包内名称。

// ClusterEnvelope 是所有节点间消息的顶层容器。
type ClusterEnvelope = internalproto.ClusterEnvelope

// ClusterEnvelope_NodeHello 包装 NodeHello 载荷。
type ClusterEnvelope_NodeHello = internalproto.ClusterEnvelope_NodeHello

// ClusterEnvelope_TimeSyncRequest 包装时间同步请求。
type ClusterEnvelope_TimeSyncRequest = internalproto.ClusterEnvelope_TimeSyncRequest

// ClusterEnvelope_TimeSyncResponse 包装时间同步响应。
type ClusterEnvelope_TimeSyncResponse = internalproto.ClusterEnvelope_TimeSyncResponse

// ClusterEnvelope_TopologyUpdate 包装拓扑更新载荷。
type ClusterEnvelope_TopologyUpdate = internalproto.ClusterEnvelope_TopologyUpdate

// ClusterEnvelope_QueryRequest 包装查询请求载荷。
type ClusterEnvelope_QueryRequest = internalproto.ClusterEnvelope_QueryRequest

// ClusterEnvelope_QueryResponse 包装查询响应载荷。
type ClusterEnvelope_QueryResponse = internalproto.ClusterEnvelope_QueryResponse

// ClusterEnvelope_ForwardedPacket 包装转发数据包载荷。
type ClusterEnvelope_ForwardedPacket = internalproto.ClusterEnvelope_ForwardedPacket

// ClusterEnvelope_ReplicationBatch 包装复制批处理载荷。
type ClusterEnvelope_ReplicationBatch = internalproto.ClusterEnvelope_ReplicationBatch

// ClusterEnvelope_PullRequest 包装拉取请求载荷。
type ClusterEnvelope_PullRequest = internalproto.ClusterEnvelope_PullRequest

// ClusterEnvelope_ReplicationAck 包装复制确认载荷。
type ClusterEnvelope_ReplicationAck = internalproto.ClusterEnvelope_ReplicationAck

// ClusterEnvelope_SnapshotManifest 包装快照清单载荷。
type ClusterEnvelope_SnapshotManifest = internalproto.ClusterEnvelope_SnapshotManifest

// ClusterEnvelope_SnapshotChunk 包装快照块载荷。
type ClusterEnvelope_SnapshotChunk = internalproto.ClusterEnvelope_SnapshotChunk

// ClusterEnvelope_RouteDiagnostic 包装路由诊断载荷。
type ClusterEnvelope_RouteDiagnostic = internalproto.ClusterEnvelope_RouteDiagnostic

// ClusterEnvelope_MembershipUpdate 包装成员变更广播载荷。
type ClusterEnvelope_MembershipUpdate = internalproto.ClusterEnvelope_MembershipUpdate

// ClusterEnvelope_PresenceUpdate 包装在线状态更新载荷。
type ClusterEnvelope_PresenceUpdate = internalproto.ClusterEnvelope_PresenceUpdate

// ClusterEnvelope_ConnectivityRumor 包装连通性谣言传播载荷。
type ClusterEnvelope_ConnectivityRumor = internalproto.ClusterEnvelope_ConnectivityRumor

// ---------------- 独立消息类型别名 ----------------

// NodeHello 是连接建立时每个节点发送的第一个消息，包含节点身份、协议版本、传输能力和转发策略。
type NodeHello = internalproto.MeshNodeHello

// TransportCapability 描述节点在一种传输上的能力（入站、出站、中继支持及其通告地址）。
type TransportCapability = internalproto.MeshTransportCapability

// ForwardingPolicy 声明节点的中转、桥接及流量分类处置偏好。
type ForwardingPolicy = internalproto.MeshForwardingPolicy

// TrafficRule 将流量分类与处置动作绑定。
type TrafficRule = internalproto.MeshTrafficRule

// TopologyUpdate 携带一个节点的本地链路、传输能力和转发策略的增量快照。
type TopologyUpdate = internalproto.MeshTopologyUpdate

// LinkAdvertisement 描述一条有向边的源、目标、传输、路径类型、代价和活跃状态。
type LinkAdvertisement = internalproto.MeshLinkAdvertisement

// ForwardedPacket 是从源到目标经过中间节点转发的数据包。
type ForwardedPacket = internalproto.MeshForwardedPacket

// ReplicationBatch 是数据复制操作的批处理消息。
type ReplicationBatch = internalproto.MeshReplicationBatch

// PullRequest 请求远程节点发送复制数据。
type PullRequest = internalproto.MeshPullRequest

// ReplicationAck 确认复制批处理接收完成。
type ReplicationAck = internalproto.MeshReplicationAck

// SnapshotManifest 描述快照的元数据（标识符、块列表）。
type SnapshotManifest = internalproto.MeshSnapshotManifest

// SnapshotChunk 是快照数据的单个块。
type SnapshotChunk = internalproto.MeshSnapshotChunk

// TimeSyncRequest 启动一次往返时间测量。
type TimeSyncRequest = internalproto.MeshTimeSyncRequest

// TimeSyncResponse 携带服务端接收和发送时间戳以完成 RTT 计算。
type TimeSyncResponse = internalproto.MeshTimeSyncResponse

// QueryRequest 是查询-响应 RPC 的请求载荷。
type QueryRequest = internalproto.MeshQueryRequest

// QueryResponse 是查询-响应 RPC 的响应载荷。
type QueryResponse = internalproto.MeshQueryResponse

// TransientPacket 携带瞬时交互载荷，不持久化。
type TransientPacket = internalproto.TransientPacket

// MembershipUpdate 广播节点集群成员变更。
type MembershipUpdate = internalproto.MeshMembershipUpdate

// MeshPresenceUpdate 广播节点在线状态变更。
type MeshPresenceUpdate = internalproto.MeshPresenceUpdate

// MeshConnectivityRumor 传播节点间的连通性信息。
type MeshConnectivityRumor = internalproto.MeshConnectivityRumor

// RouteDiagnostic 携带用于调试的路由追踪信息。
type RouteDiagnostic = internalproto.MeshRouteDiagnostic

// ---------------- 枚举类型别名 ----------------
// 以下类型重新导出 protobuf 枚举，实现强类型化。

// PathClass 分类路径：直连、同传输转发、跨传输桥接或本地中继。
type PathClass = internalproto.PathClass

// TrafficClass 对消息进行 QoS 分类，影响路径选择和代价计算。
type TrafficClass = internalproto.TrafficClass

// TransportKind 标识传输层实现（WebSocket、LibP2P、ZeroMQ）。
type TransportKind = internalproto.TransportKind

// ForwardingDisposition 指定节点对某流量分类的处理动作：允许、劝阻或拒绝。
type ForwardingDisposition = internalproto.ForwardingDisposition

// TransportKind 常量。
const (
	TransportUnspecified = internalproto.TransportKind_TRANSPORT_KIND_UNSPECIFIED // 零值，表示未指定传输类型。
	TransportLibP2P      = internalproto.TransportKind_TRANSPORT_KIND_LIBP2P      // libp2p 传输。
	TransportZeroMQ      = internalproto.TransportKind_TRANSPORT_KIND_ZEROMQ      // ZeroMQ 传输。
	TransportWebSocket   = internalproto.TransportKind_TRANSPORT_KIND_WEBSOCKET   // WebSocket 传输。
)

// TrafficClass 常量。
const (
	TrafficClassUnspecified     = internalproto.TrafficClass_TRAFFIC_CLASS_UNSPECIFIED       // 零值，表示未指定类别。
	TrafficControlCritical      = internalproto.TrafficClass_TRAFFIC_CONTROL_CRITICAL        // 控制面关键消息（Hello、拓扑更新等）。
	TrafficControlQuery         = internalproto.TrafficClass_TRAFFIC_CONTROL_QUERY           // 查询-响应 RPC 消息。
	TrafficTransientInteractive = internalproto.TrafficClass_TRAFFIC_TRANSIENT_INTERACTIVE   // 瞬时交互流量（转发数据包）。
	TrafficReplicationStream    = internalproto.TrafficClass_TRAFFIC_REPLICATION_STREAM      // 复制流数据。
	TrafficSnapshotBulk         = internalproto.TrafficClass_TRAFFIC_SNAPSHOT_BULK           // 批量快照传输。
)

// ForwardingDisposition 常量。
const (
	DispositionUnspecified = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_UNSPECIFIED // 零值，默认等同于 Allow。
	DispositionAllow       = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_ALLOW       // 允许该流量类型通过。
	DispositionDiscourage  = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_DISCOURAGE  // 允许通过但增加代价值。
	DispositionDeny        = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_DENY        // 拒绝该流量类型通过。
)

// PathClass 常量。
const (
	PathClassUnspecified          = internalproto.PathClass_PATH_CLASS_UNSPECIFIED            // 零值，表示未指定路径类型。
	PathClassDirect               = internalproto.PathClass_PATH_CLASS_DIRECT                 // 两个节点之间的直连链路。
	PathClassSameTransportForward = internalproto.PathClass_PATH_CLASS_SAME_TRANSPORT_FORWARD // 同一传输上的多跳转发路径。
	PathClassCrossTransportBridge = internalproto.PathClass_PATH_CLASS_CROSS_TRANSPORT_BRIDGE // 同一节点内跨传输桥接。
	PathClassNativeRelay          = internalproto.PathClass_PATH_CLASS_NATIVE_RELAY            // 通过传输层本地中继的路径。
)

// ---------------- 核心接口 ----------------

// TransportAdapter 拥有一个传输监听器，可以拨出连接，并提供接收通道。
// 每个运行时至少需要一个适配器才能运行。
type TransportAdapter interface {
	Start(ctx context.Context) error
	Dial(ctx context.Context, endpoint string) (TransportConn, error)
	Accept() <-chan TransportConn
	Kind() TransportKind
	LocalCapabilities() *TransportCapability
}

// TransportConn 表示一个单一的传输连接，能够发送和接收原始信封字节。
type TransportConn interface {
	Send(ctx context.Context, envelope []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
	RemoteNodeHint() string
	Transport() TransportKind
}

// TopologyStore 是一个可变的拓扑存储，接受 Hello 和拓扑更新，
// 并生成不可变的快照供路由规划器使用。
// 默认实现是 MemoryTopologyStore。
type TopologyStore interface {
	ApplyHello(nodeID int64, hello *NodeHello)
	ApplyTopologyUpdate(update *TopologyUpdate)
	Snapshot() TopologySnapshot
}

// RoutePlanner 计算从本地节点到目标节点的最佳路径，
// 考虑流量类别和入站传输。
// 默认实现是 Planner。
type RoutePlanner interface {
	Compute(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind) (RouteDecision, bool)
}

// ForwardingEngine 处理出站和入站的转发数据包，包括去重和 TTL 管理。
// 默认实现是 Engine。
type ForwardingEngine interface {
	Forward(ctx context.Context, packet *ForwardedPacket) error
	HandleInbound(ctx context.Context, packet *ForwardedPacket) error
}

// TrafficClassifier 将信封体类型映射到 TrafficClass 用于 QoS 决策。
// 默认实现是 DefaultTrafficClassifier。
type TrafficClassifier interface {
	Classify(envelope *ClusterEnvelope) TrafficClass
}

// RouteDecision 描述路由计算的结果：下一跳、出站传输、路径类型及预估代价。
type RouteDecision struct {
	DestinationNodeID  int64           // 目标节点 ID。
	NextHopNodeID      int64           // 直接下一跳节点 ID。
	OutboundTransport  TransportKind   // 发往下一跳使用的传输类型。
	PathClass          PathClass       // 此路径的分类。
	EstimatedCost      int64           // 路径的总预估往返时间（毫秒）。
	TopologyGeneration uint64          // 计算路径时使用的拓扑世代号。
}
