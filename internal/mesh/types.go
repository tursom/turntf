package mesh

import (
	"context"

	internalproto "github.com/tursom/turntf/internal/proto"
)

const ProtocolVersion = "mesh-v1alpha1"

type ClusterEnvelope = internalproto.ClusterEnvelope
type ClusterEnvelope_NodeHello = internalproto.ClusterEnvelope_NodeHello
type ClusterEnvelope_TimeSyncRequest = internalproto.ClusterEnvelope_TimeSyncRequest
type ClusterEnvelope_TimeSyncResponse = internalproto.ClusterEnvelope_TimeSyncResponse
type ClusterEnvelope_TopologyUpdate = internalproto.ClusterEnvelope_TopologyUpdate
type ClusterEnvelope_QueryRequest = internalproto.ClusterEnvelope_QueryRequest
type ClusterEnvelope_QueryResponse = internalproto.ClusterEnvelope_QueryResponse
type ClusterEnvelope_ForwardedPacket = internalproto.ClusterEnvelope_ForwardedPacket
type ClusterEnvelope_ReplicationBatch = internalproto.ClusterEnvelope_ReplicationBatch
type ClusterEnvelope_PullRequest = internalproto.ClusterEnvelope_PullRequest
type ClusterEnvelope_SnapshotManifest = internalproto.ClusterEnvelope_SnapshotManifest
type ClusterEnvelope_SnapshotChunk = internalproto.ClusterEnvelope_SnapshotChunk
type ClusterEnvelope_RouteDiagnostic = internalproto.ClusterEnvelope_RouteDiagnostic
type NodeHello = internalproto.MeshNodeHello
type TransportCapability = internalproto.MeshTransportCapability
type ForwardingPolicy = internalproto.MeshForwardingPolicy
type TrafficRule = internalproto.MeshTrafficRule
type TopologyUpdate = internalproto.MeshTopologyUpdate
type LinkAdvertisement = internalproto.MeshLinkAdvertisement
type ForwardedPacket = internalproto.MeshForwardedPacket
type ReplicationBatch = internalproto.MeshReplicationBatch
type PullRequest = internalproto.MeshPullRequest
type SnapshotManifest = internalproto.MeshSnapshotManifest
type SnapshotChunk = internalproto.MeshSnapshotChunk
type TimeSyncRequest = internalproto.MeshTimeSyncRequest
type TimeSyncResponse = internalproto.MeshTimeSyncResponse
type QueryRequest = internalproto.MeshQueryRequest
type QueryResponse = internalproto.MeshQueryResponse
type RouteDiagnostic = internalproto.MeshRouteDiagnostic

type PathClass = internalproto.PathClass
type TrafficClass = internalproto.TrafficClass
type TransportKind = internalproto.TransportKind
type ForwardingDisposition = internalproto.ForwardingDisposition

const (
	TransportUnspecified = internalproto.TransportKind_TRANSPORT_KIND_UNSPECIFIED
	TransportLibP2P      = internalproto.TransportKind_TRANSPORT_KIND_LIBP2P
	TransportZeroMQ      = internalproto.TransportKind_TRANSPORT_KIND_ZEROMQ
)

const (
	TrafficClassUnspecified     = internalproto.TrafficClass_TRAFFIC_CLASS_UNSPECIFIED
	TrafficControlCritical      = internalproto.TrafficClass_TRAFFIC_CONTROL_CRITICAL
	TrafficControlQuery         = internalproto.TrafficClass_TRAFFIC_CONTROL_QUERY
	TrafficTransientInteractive = internalproto.TrafficClass_TRAFFIC_TRANSIENT_INTERACTIVE
	TrafficReplicationStream    = internalproto.TrafficClass_TRAFFIC_REPLICATION_STREAM
	TrafficSnapshotBulk         = internalproto.TrafficClass_TRAFFIC_SNAPSHOT_BULK
)

const (
	DispositionUnspecified = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_UNSPECIFIED
	DispositionAllow       = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_ALLOW
	DispositionDiscourage  = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_DISCOURAGE
	DispositionDeny        = internalproto.ForwardingDisposition_FORWARDING_DISPOSITION_DENY
)

const (
	PathClassUnspecified          = internalproto.PathClass_PATH_CLASS_UNSPECIFIED
	PathClassDirect               = internalproto.PathClass_PATH_CLASS_DIRECT
	PathClassSameTransportForward = internalproto.PathClass_PATH_CLASS_SAME_TRANSPORT_FORWARD
	PathClassCrossTransportBridge = internalproto.PathClass_PATH_CLASS_CROSS_TRANSPORT_BRIDGE
	PathClassNativeRelay          = internalproto.PathClass_PATH_CLASS_NATIVE_RELAY
)

type TransportAdapter interface {
	Start(ctx context.Context) error
	Dial(ctx context.Context, endpoint string) (TransportConn, error)
	Accept() <-chan TransportConn
	Kind() TransportKind
	LocalCapabilities() *TransportCapability
}

type TransportConn interface {
	Send(ctx context.Context, envelope []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
	RemoteNodeHint() string
	Transport() TransportKind
}

type TopologyStore interface {
	ApplyHello(nodeID int64, hello *NodeHello)
	ApplyTopologyUpdate(update *TopologyUpdate)
	Snapshot() TopologySnapshot
}

type RoutePlanner interface {
	Compute(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind) (RouteDecision, bool)
}

type ForwardingEngine interface {
	Forward(ctx context.Context, packet *ForwardedPacket) error
	HandleInbound(ctx context.Context, packet *ForwardedPacket) error
}

type TrafficClassifier interface {
	Classify(envelope *ClusterEnvelope) TrafficClass
}

type RouteDecision struct {
	DestinationNodeID  int64
	NextHopNodeID      int64
	OutboundTransport  TransportKind
	PathClass          PathClass
	EstimatedCost      int64
	TopologyGeneration uint64
}
