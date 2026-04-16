package cluster

import (
	"testing"

	"github.com/tursom/turntf/internal/mesh"
)

func TestConfigEffectiveForwardingDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	forwarding := cfg.EffectiveForwarding()
	if !boolValue(forwarding.Enabled, false) {
		t.Fatal("expected forwarding to default to enabled")
	}
	if !boolValue(forwarding.BridgeEnabled, false) {
		t.Fatal("expected bridge to default to enabled")
	}
	if forwarding.NodeFeeWeight != 1 {
		t.Fatalf("unexpected node fee weight: got=%d want=1", forwarding.NodeFeeWeight)
	}
	if !cfg.ZeroMQForwardingEnabled() {
		t.Fatal("expected zeromq forwarding to follow global forwarding default")
	}
}

func TestConfigEffectiveForwardingHighFeeDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		Forwarding: ForwardingConfig{
			NodeFeeWeight: 10,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	policy := cfg.MeshForwardingPolicy()
	if !policy.TransitEnabled {
		t.Fatal("expected high-fee transit to stay enabled")
	}
	if !policy.BridgeEnabled {
		t.Fatal("expected high-fee bridge to stay enabled")
	}
	if got := mesh.DispositionForTraffic(policy, mesh.TrafficReplicationStream); got != mesh.DispositionDeny {
		t.Fatalf("unexpected replication disposition: got=%v want=%v", got, mesh.DispositionDeny)
	}
	if got := mesh.DispositionForTraffic(policy, mesh.TrafficSnapshotBulk); got != mesh.DispositionDeny {
		t.Fatalf("unexpected snapshot disposition: got=%v want=%v", got, mesh.DispositionDeny)
	}
}

func TestConfigValidateRejectsInvalidForwardingDisposition(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		Forwarding: ForwardingConfig{
			Traffic: ForwardingTrafficConfig{
				ControlCritical: mesh.ForwardingDisposition(99),
			},
		},
	}

	if err := cfg.Validate(); err == nil || err.Error() != "cluster forwarding traffic control_critical disposition is invalid" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigZeroMQForwardingFollowsGlobalSetting(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		Forwarding: ForwardingConfig{
			Enabled:       boolPtr(false),
			BridgeEnabled: boolPtr(false),
		},
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if cfg.ZeroMQForwardingEnabled() {
		t.Fatal("expected zeromq forwarding to follow explicit global disable")
	}
}

func TestConfigTransportCapabilitiesReflectMeshSettings(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		LibP2P: LibP2PConfig{
			Enabled:                   true,
			PrivateKeyPath:            DefaultLibP2PPrivateKeyPath,
			ListenAddrs:               []string{"/ip4/0.0.0.0/tcp/4001"},
			NativeRelayClientEnabled:  true,
			NativeRelayServiceEnabled: true,
		},
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
			BindURL: "tcp://127.0.0.1:9090",
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	libp2pCapability := cfg.LibP2PTransportCapability()
	if libp2pCapability == nil {
		t.Fatal("expected libp2p capability")
	}
	if libp2pCapability.Transport != mesh.TransportLibP2P {
		t.Fatalf("unexpected libp2p transport: %v", libp2pCapability.Transport)
	}
	if !libp2pCapability.NativeRelayClientEnabled || !libp2pCapability.NativeRelayServiceEnabled {
		t.Fatalf("unexpected libp2p relay capability: %+v", libp2pCapability)
	}

	zeroMQCapability := cfg.ZeroMQTransportCapability()
	if zeroMQCapability == nil {
		t.Fatal("expected zeromq capability")
	}
	if zeroMQCapability.Transport != mesh.TransportZeroMQ {
		t.Fatalf("unexpected zeromq transport: %v", zeroMQCapability.Transport)
	}
	if !zeroMQCapability.InboundEnabled || !zeroMQCapability.OutboundEnabled {
		t.Fatalf("unexpected zeromq capability: %+v", zeroMQCapability)
	}
	if len(zeroMQCapability.AdvertisedEndpoints) != 1 || zeroMQCapability.AdvertisedEndpoints[0] != "zmq+tcp://127.0.0.1:9090" {
		t.Fatalf("unexpected zeromq advertised endpoints: %+v", zeroMQCapability.AdvertisedEndpoints)
	}
}
