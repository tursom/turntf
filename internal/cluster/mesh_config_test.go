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

func TestParseForwardingDispositionAcceptsDocumentedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want mesh.ForwardingDisposition
	}{
		{name: "unspecified", raw: "  ", want: mesh.DispositionUnspecified},
		{name: "allow", raw: " allow ", want: mesh.DispositionAllow},
		{name: "discourage", raw: "DisCoUrAgE", want: mesh.DispositionDiscourage},
		{name: "deny", raw: "DENY", want: mesh.DispositionDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseForwardingDisposition(tt.raw)
			if err != nil {
				t.Fatalf("parse forwarding disposition %q: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("unexpected forwarding disposition: got=%v want=%v", got, tt.want)
			}
		})
	}
}

func TestParseForwardingDispositionRejectsUnknownValue(t *testing.T) {
	t.Parallel()

	got, err := ParseForwardingDisposition("forward")
	if err == nil {
		t.Fatal("expected unknown forwarding disposition to fail")
	}
	if got != mesh.DispositionUnspecified {
		t.Fatalf("unexpected disposition on failure: got=%v want=%v", got, mesh.DispositionUnspecified)
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

func TestConfigZeroMQForwardingDisableRemovesMeshCapability(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		ZeroMQ: ZeroMQConfig{
			Enabled:           true,
			BindURL:           "tcp://127.0.0.1:9090",
			ForwardingEnabled: boolPtr(false),
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	zeroMQCapability := cfg.ZeroMQTransportCapability()
	if zeroMQCapability == nil {
		t.Fatal("expected zeromq capability object even when forwarding is disabled")
	}
	if zeroMQCapability.InboundEnabled || zeroMQCapability.OutboundEnabled {
		t.Fatalf("expected zeromq mesh forwarding capability to be disabled, got %+v", zeroMQCapability)
	}
	if len(zeroMQCapability.AdvertisedEndpoints) != 0 {
		t.Fatalf("expected zeromq advertised endpoints to be hidden when forwarding is disabled, got %+v", zeroMQCapability.AdvertisedEndpoints)
	}
	if adapter := NewZeroMQMeshTransportAdapter(cfg, nil); adapter != nil {
		t.Fatal("expected zeromq mesh adapter to be omitted when forwarding is disabled")
	}

	mgr, err := NewManager(cfg, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	binding, err := mgr.BuildMeshRuntime()
	if err != nil {
		t.Fatalf("build mesh runtime: %v", err)
	}
	if binding.InboundAdapter(mesh.TransportZeroMQ) != nil {
		t.Fatal("expected mesh runtime to omit zeromq adapter when forwarding is disabled")
	}
}
