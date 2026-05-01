package mesh

// DefaultTrafficClassifier 是 TrafficClassifier 的默认实现。
// 它根据信封体类型选择流量分类。
type DefaultTrafficClassifier struct{}

// Classify 根据信封体类型返回流量分类：
//   - 控制消息（Hello、时间同步、拓扑、复制确认、成员、在线状态、连通性谣言、路由诊断）→ TrafficControlCritical
//   - 查询请求/响应 → TrafficControlQuery
//   - 转发数据包 → TrafficTransientInteractive
//   - 复制批处理、拉取请求 → TrafficReplicationStream
//   - 快照清单、快照块 → TrafficSnapshotBulk
//   - 未知类型 → TrafficClassUnspecified
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
