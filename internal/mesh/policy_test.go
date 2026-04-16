package mesh

import "testing"

func TestDefaultForwardingPolicyDefaults(t *testing.T) {
	t.Parallel()

	policy := DefaultForwardingPolicy(1)
	if !policy.TransitEnabled {
		t.Fatal("expected transit to be enabled by default")
	}
	if !policy.BridgeEnabled {
		t.Fatal("expected bridge to be enabled by default")
	}
	for _, class := range []TrafficClass{
		TrafficControlCritical,
		TrafficControlQuery,
		TrafficTransientInteractive,
		TrafficReplicationStream,
		TrafficSnapshotBulk,
	} {
		if got := DispositionForTraffic(policy, class); got != DispositionAllow {
			t.Fatalf("unexpected default disposition for %v: got=%v want=%v", class, got, DispositionAllow)
		}
	}
}

func TestDefaultForwardingPolicyHighFeeDefaults(t *testing.T) {
	t.Parallel()

	policy := DefaultForwardingPolicy(9)
	if !policy.TransitEnabled {
		t.Fatal("expected high-fee transit to stay enabled by default")
	}
	if !policy.BridgeEnabled {
		t.Fatal("expected high-fee bridge to stay enabled by default")
	}
	tests := []struct {
		class TrafficClass
		want  ForwardingDisposition
	}{
		{class: TrafficControlCritical, want: DispositionAllow},
		{class: TrafficControlQuery, want: DispositionAllow},
		{class: TrafficTransientInteractive, want: DispositionDiscourage},
		{class: TrafficReplicationStream, want: DispositionDeny},
		{class: TrafficSnapshotBulk, want: DispositionDeny},
	}
	for _, tc := range tests {
		if got := DispositionForTraffic(policy, tc.class); got != tc.want {
			t.Fatalf("unexpected disposition for %v: got=%v want=%v", tc.class, got, tc.want)
		}
	}
}

func TestNormalizeForwardingPolicyUsesNewDefaults(t *testing.T) {
	t.Parallel()

	policy := NormalizeForwardingPolicy(nil)
	if !policy.TransitEnabled {
		t.Fatal("expected normalized nil policy to enable transit")
	}
	if !policy.BridgeEnabled {
		t.Fatal("expected normalized nil policy to enable bridge")
	}
}

func TestNormalizeForwardingPolicyBackfillsMissingRules(t *testing.T) {
	t.Parallel()

	policy := NormalizeForwardingPolicy(&ForwardingPolicy{
		TransitEnabled: true,
		BridgeEnabled:  true,
		NodeFeeWeight:  7,
		TrafficRules: []*TrafficRule{
			{TrafficClass: TrafficControlCritical, Disposition: DispositionAllow},
		},
	})

	if got := DispositionForTraffic(policy, TrafficControlQuery); got != DispositionAllow {
		t.Fatalf("unexpected control query disposition: got=%v want=%v", got, DispositionAllow)
	}
	if got := DispositionForTraffic(policy, TrafficTransientInteractive); got != DispositionDiscourage {
		t.Fatalf("unexpected transient disposition: got=%v want=%v", got, DispositionDiscourage)
	}
	if got := DispositionForTraffic(policy, TrafficReplicationStream); got != DispositionDeny {
		t.Fatalf("unexpected replication disposition: got=%v want=%v", got, DispositionDeny)
	}
	if got := DispositionForTraffic(policy, TrafficSnapshotBulk); got != DispositionDeny {
		t.Fatalf("unexpected snapshot disposition: got=%v want=%v", got, DispositionDeny)
	}
}
