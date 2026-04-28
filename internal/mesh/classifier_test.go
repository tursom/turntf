package mesh

import "testing"

func TestDefaultTrafficClassifier(t *testing.T) {
	t.Parallel()

	classifier := DefaultTrafficClassifier{}
	tests := []struct {
		name string
		env  *ClusterEnvelope
		want TrafficClass
	}{
		{
			name: "hello",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_NodeHello{
				NodeHello: &NodeHello{},
			}},
			want: TrafficControlCritical,
		},
		{
			name: "query",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_QueryRequest{
				QueryRequest: &QueryRequest{},
			}},
			want: TrafficControlQuery,
		},
		{
			name: "forwarded packet",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{
				ForwardedPacket: &ForwardedPacket{},
			}},
			want: TrafficTransientInteractive,
		},
		{
			name: "replication",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_ReplicationBatch{
				ReplicationBatch: &ReplicationBatch{},
			}},
			want: TrafficReplicationStream,
		},
		{
			name: "replication ack",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_ReplicationAck{
				ReplicationAck: &ReplicationAck{},
			}},
			want: TrafficControlCritical,
		},
		{
			name: "membership update",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_MembershipUpdate{
				MembershipUpdate: &MembershipUpdate{},
			}},
			want: TrafficControlCritical,
		},
		{
			name: "connectivity rumor",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_ConnectivityRumor{
				ConnectivityRumor: &MeshConnectivityRumor{},
			}},
			want: TrafficControlCritical,
		},
		{
			name: "snapshot",
			env: &ClusterEnvelope{Body: &ClusterEnvelope_SnapshotChunk{
				SnapshotChunk: &SnapshotChunk{},
			}},
			want: TrafficSnapshotBulk,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := classifier.Classify(tc.env); got != tc.want {
				t.Fatalf("unexpected traffic class: got=%v want=%v", got, tc.want)
			}
		})
	}
}
