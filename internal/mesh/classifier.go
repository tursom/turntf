package mesh

// DefaultTrafficClassifier 是 TrafficClassifier 接口的默认实现。
// 它根据 ClusterEnvelope.Body 的具体类型（oneof）选择对应的流量分类，
// 用于后续的 QoS 路径选择和代价计算。
// 该实现是无状态的，所有实例等价，可直接使用 struct{} 零值。
type DefaultTrafficClassifier struct{}

// Classify 根据 ClusterEnvelope.Body 的具体 oneof 类型返回对应的流量分类。
// 分类映射表：
//
//	控制关键消息（TrafficControlCritical）：
//	  NodeHello, TimeSyncRequest, TimeSyncResponse, TopologyUpdate,
//	  ReplicationAck, MembershipUpdate, PresenceUpdate,
//	  ConnectivityRumor, RouteDiagnostic
//
//	控制查询消息（TrafficControlQuery）：
//	  QueryRequest, QueryResponse
//
//	瞬时交互消息（TrafficTransientInteractive）：
//	  ForwardedPacket
//
//	复制流消息（TrafficReplicationStream）：
//	  ReplicationBatch, PullRequest
//
//	批量快照消息（TrafficSnapshotBulk）：
//	  SnapshotManifest, SnapshotChunk
//
//	未识别或 nil（TrafficClassUnspecified）：
//	  其他所有类型
func (DefaultTrafficClassifier) Classify(envelope *ClusterEnvelope) TrafficClass {
	if envelope == nil {
		return TrafficClassUnspecified
	}
	switch envelope.Body.(type) {
	case *ClusterEnvelope_NodeHello,
		*ClusterEnvelope_TimeSyncRequest,
		*ClusterEnvelope_TimeSyncResponse,
		*ClusterEnvelope_TopologyUpdate,
		*ClusterEnvelope_ReplicationAck,
		*ClusterEnvelope_MembershipUpdate,
		*ClusterEnvelope_PresenceUpdate,
		*ClusterEnvelope_ConnectivityRumor,
		*ClusterEnvelope_RouteDiagnostic:
		return TrafficControlCritical
	case *ClusterEnvelope_QueryRequest,
		*ClusterEnvelope_QueryResponse:
		return TrafficControlQuery
	case *ClusterEnvelope_ForwardedPacket:
		return TrafficTransientInteractive
	case *ClusterEnvelope_ReplicationBatch,
		*ClusterEnvelope_PullRequest:
		return TrafficReplicationStream
	case *ClusterEnvelope_SnapshotManifest,
		*ClusterEnvelope_SnapshotChunk:
		return TrafficSnapshotBulk
	default:
		return TrafficClassUnspecified
	}
}
