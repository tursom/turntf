package mesh

type DefaultTrafficClassifier struct{}

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
