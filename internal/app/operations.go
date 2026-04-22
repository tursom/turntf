package app

import "time"

type ClusterStatus struct {
	NodeID               int64
	MessageWindowSize    int
	WriteGateReady       bool
	ClockState           string
	ClockReason          string
	LastTrustedClockSync *time.Time
	ClockTransitions     []ClockStateTransition
	Peers                []ClusterPeerStatus
	Discovery            ClusterDiscoveryStatus
	Mesh                 ClusterMeshStatus
}

type ClusterDiscoveryStatus struct {
	DiscoveredPeers       int
	DynamicPeers          int
	MembershipUpdatesSent uint64
	MembershipUpdatesRecv uint64
	RejectedTotal         uint64
	PersistFailuresTotal  uint64
	PeersByState          map[string]int
	PeersByScheme         map[string]int
	ZeroMQMode            string
	ZeroMQSecurity        string
	ZeroMQListenerRunning bool
	LibP2PMode            string
	LibP2PPeerID          string
	LibP2PListenAddrs     []string
	LibP2PVerifiedAddrs   []string
	LibP2PDHTEnabled      bool
	LibP2PDHTBootstrapped bool
	LibP2PGossipSubTopic  string
	LibP2PGossipSubPeers  int
	LibP2PRelayEnabled    bool
	LibP2PHolePunching    bool
}

type ClusterMeshStatus struct {
	Enabled               bool
	ForwardingEnabled     bool
	BridgeEnabled         bool
	NodeFeeWeight         int64
	TopologyGeneration    uint64
	TransportCapabilities []ClusterMeshTransportCapability
	TrafficRules          []ClusterMeshTrafficRule
	Routes                []ClusterMeshRoute
	Metrics               ClusterMeshMetrics
}

type ClusterMeshTransportCapability struct {
	Transport                 string
	InboundEnabled            bool
	OutboundEnabled           bool
	NativeRelayClientEnabled  bool
	NativeRelayServiceEnabled bool
	AdvertisedEndpoints       []string
}

type ClusterMeshTrafficRule struct {
	TrafficClass string
	Disposition  string
}

type ClusterMeshRoute struct {
	DestinationNodeID  int64
	TrafficClass       string
	Reachable          bool
	NextHopNodeID      int64
	OutboundTransport  string
	PathClass          string
	EstimatedCost      int64
	TopologyGeneration uint64
}

type ClusterMeshMetrics struct {
	ForwardedPackets []ClusterMeshMetricSample
	ForwardedBytes   []ClusterMeshMetricSample
	RoutingNoPath    []ClusterMeshMetricSample
	DecisionCost     []ClusterMeshCostSample
	BridgeForwards   []ClusterMeshMetricSample
}

type ClusterMeshMetricSample struct {
	TrafficClass string
	PathClass    string
	Value        uint64
}

type ClusterMeshCostSample struct {
	TrafficClass string
	Value        int64
}

type LoggedInUserSummary struct {
	NodeID   int64
	UserID   int64
	Username string
}

type ClusterPeerOriginStatus struct {
	OriginNodeID      int64
	RemoteLastEventID uint64
	PendingCatchup    bool
}

type ClusterPeerStatus struct {
	NodeID                    int64
	ConfiguredURL             string
	Transport                 string
	Source                    string
	DiscoveredURL             string
	DiscoveryState            string
	LastDiscoveredAt          *time.Time
	LastConnectedAt           *time.Time
	LastDiscoveryError        string
	Connected                 bool
	SessionDirection          string
	Origins                   []ClusterPeerOriginStatus
	PendingSnapshotPartitions int
	RemoteSnapshotVersion     string
	RemoteMessageWindowSize   int
	ClockState                string
	ClockOffsetMs             int64
	ClockUncertaintyMs        int64
	ClockFailures             uint64
	LastClockError            string
	LastClockSync             *time.Time
	LastCredibleClockSync     *time.Time
	TrustedForOffset          bool
	SnapshotDigestsSentTotal  uint64
	SnapshotDigestsRecvTotal  uint64
	SnapshotChunksSentTotal   uint64
	SnapshotChunksRecvTotal   uint64
	LastSnapshotDigestAt      *time.Time
	LastSnapshotChunkAt       *time.Time
}

type ClockStateTransition struct {
	FromState string
	ToState   string
	Reason    string
	Total     uint64
}
