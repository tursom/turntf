package mesh

import "testing"

func TestPlannerAvoidsNonTransitNode(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, false, 1, false, TransportLibP2P),
		testNode(3, false, 1, true, TransportLibP2P),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	if _, ok := planner.Compute(snapshot, 3, TrafficControlCritical, TransportUnspecified); ok {
		t.Fatal("expected forwarding-disabled node to block transit route")
	}
}

func TestPlannerEnforcesLocalTransitPolicyForInbound(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, false, TransportLibP2P),
		testNode(3, false, 1, true, TransportLibP2P),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	if _, ok := planner.Compute(snapshot, 3, TrafficControlQuery, TransportLibP2P); ok {
		t.Fatal("expected inbound transit to honor local forwarding-disabled policy")
	}
	if _, ok := planner.Compute(snapshot, 3, TrafficControlQuery, TransportUnspecified); !ok {
		t.Fatal("expected local-origin traffic to ignore transit-disabled policy")
	}
}

func TestPlannerEnforcesLocalTrafficDenyForInbound(t *testing.T) {
	t.Parallel()

	policy := DefaultForwardingPolicy(1)
	for _, rule := range policy.TrafficRules {
		if rule.TrafficClass == TrafficReplicationStream {
			rule.Disposition = DispositionDeny
		}
	}
	snapshot := testSnapshotWithNodes(
		NodeState{
			NodeID:           1,
			ForwardingPolicy: policy,
			TransportCaps: map[TransportKind]*TransportCapability{
				TransportLibP2P: {Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		},
		testNode(3, false, 1, true, TransportLibP2P),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	if _, ok := planner.Compute(snapshot, 3, TrafficReplicationStream, TransportLibP2P); ok {
		t.Fatal("expected inbound transit to honor local traffic deny policy")
	}
	if _, ok := planner.Compute(snapshot, 3, TrafficReplicationStream, TransportUnspecified); !ok {
		t.Fatal("expected local-origin traffic to ignore local transit deny policy")
	}
}

func TestPlannerCrossTransportBridgeForControlTraffic(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, true, 1, true, TransportLibP2P, TransportZeroMQ),
		testNode(3, false, 1, true, TransportZeroMQ),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	decision, ok := planner.Compute(snapshot, 3, TrafficControlCritical, TransportUnspecified)
	if !ok {
		t.Fatal("expected control traffic route across bridge")
	}
	if decision.NextHopNodeID != 2 || decision.OutboundTransport != TransportLibP2P {
		t.Fatalf("unexpected route decision: %+v", decision)
	}
	if decision.PathClass != PathClassCrossTransportBridge {
		t.Fatalf("unexpected path class: got=%v want=%v", decision.PathClass, PathClassCrossTransportBridge)
	}
}

func TestPlannerBridgeChargesTransitFeeOnce(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, true, 10, true, TransportLibP2P, TransportZeroMQ),
		testNode(3, false, 1, true, TransportZeroMQ),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 7, Established: true},
	}

	planner := NewPlanner(1)
	decision, ok := planner.Compute(snapshot, 3, TrafficControlCritical, TransportUnspecified)
	if !ok {
		t.Fatal("expected control traffic route across bridge")
	}
	wantCost := int64(5+BridgePenaltyMs+7) + 10*TrafficClassFactor(TrafficControlCritical)
	if decision.EstimatedCost != wantCost {
		t.Fatalf("unexpected bridge route cost: got=%d want=%d", decision.EstimatedCost, wantCost)
	}
}

func TestPlannerRejectsBridgeForReplicationTraffic(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, true, 1, true, TransportLibP2P, TransportZeroMQ),
		testNode(3, false, 1, true, TransportZeroMQ),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	if _, ok := planner.Compute(snapshot, 3, TrafficReplicationStream, TransportUnspecified); ok {
		t.Fatal("expected replication traffic to reject cross-transport bridge")
	}
}

func TestPlannerRejectsHighFeeBridgeForReplicationTraffic(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, true, 10, true, TransportLibP2P, TransportZeroMQ),
		testNode(3, false, 1, true, TransportZeroMQ),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	if _, ok := planner.Compute(snapshot, 3, TrafficReplicationStream, TransportUnspecified); ok {
		t.Fatal("expected high-fee bridge node to reject replication traffic")
	}
}

func TestPlannerAllowsHighFeeTransitForControlButNotReplication(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, false, 10, true, TransportLibP2P),
		testNode(3, false, 1, true, TransportLibP2P),
	)
	snapshot.Nodes[2] = NodeState{
		NodeID: 2,
		ForwardingPolicy: func() *ForwardingPolicy {
			policy := DefaultForwardingPolicy(10)
			policy.TransitEnabled = true
			return policy
		}(),
		TransportCaps: map[TransportKind]*TransportCapability{
			TransportLibP2P: {Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	}
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	if _, ok := planner.Compute(snapshot, 3, TrafficControlCritical, TransportUnspecified); !ok {
		t.Fatal("expected control traffic to use high-fee transit when it is the only path")
	}
	if _, ok := planner.Compute(snapshot, 3, TrafficReplicationStream, TransportUnspecified); ok {
		t.Fatal("expected replication traffic to avoid high-fee deny path")
	}
}

func TestPlannerPrefersAllowOverDiscouragedPath(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, false, 5, true, TransportLibP2P),
		testNode(3, false, 1, true, TransportLibP2P),
		testNode(4, false, 1, true, TransportLibP2P),
	)
	snapshot.Nodes[2] = NodeState{
		NodeID: 2,
		ForwardingPolicy: &ForwardingPolicy{
			TransitEnabled: true,
			BridgeEnabled:  false,
			NodeFeeWeight:  5,
			TrafficRules: []*TrafficRule{
				{TrafficClass: TrafficTransientInteractive, Disposition: DispositionDiscourage},
			},
		},
		TransportCaps: map[TransportKind]*TransportCapability{
			TransportLibP2P: {Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	}
	snapshot.Nodes[4] = NodeState{
		NodeID:           4,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		TransportCaps: map[TransportKind]*TransportCapability{
			TransportLibP2P: {Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	}
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{FromNodeID: 1, ToNodeID: 4, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		{FromNodeID: 4, ToNodeID: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	decision, ok := planner.Compute(snapshot, 3, TrafficTransientInteractive, TransportUnspecified)
	if !ok {
		t.Fatal("expected a route decision")
	}
	if decision.NextHopNodeID != 4 {
		t.Fatalf("unexpected next hop: got=%d want=4", decision.NextHopNodeID)
	}
}

func TestPlannerMarksNativeRelayPath(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshotWithNodes(
		testNode(1, false, 1, true, TransportLibP2P),
		testNode(2, false, 1, true, TransportLibP2P),
	)
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, PathClass: PathClassNativeRelay, CostMs: 5, Established: true},
	}

	planner := NewPlanner(1)
	decision, ok := planner.Compute(snapshot, 2, TrafficControlCritical, TransportUnspecified)
	if !ok {
		t.Fatal("expected native relay route")
	}
	if decision.PathClass != PathClassNativeRelay {
		t.Fatalf("unexpected path class: got=%v want=%v", decision.PathClass, PathClassNativeRelay)
	}
}

func testSnapshotWithNodes(nodes ...NodeState) TopologySnapshot {
	snapshot := TopologySnapshot{
		Nodes: make(map[int64]NodeState, len(nodes)),
	}
	for _, node := range nodes {
		snapshot.Nodes[node.NodeID] = node
	}
	return snapshot
}

func testNode(nodeID int64, bridge bool, feeWeight int64, transit bool, transports ...TransportKind) NodeState {
	caps := make(map[TransportKind]*TransportCapability, len(transports))
	for _, transport := range transports {
		caps[transport] = &TransportCapability{
			Transport:       transport,
			InboundEnabled:  true,
			OutboundEnabled: true,
		}
	}
	policy := DefaultForwardingPolicy(feeWeight)
	policy.TransitEnabled = transit
	policy.BridgeEnabled = bridge
	return NodeState{
		NodeID:           nodeID,
		ForwardingPolicy: policy,
		TransportCaps:    caps,
	}
}
