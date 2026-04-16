package cluster

import (
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/mesh"
)

type ForwardingConfig struct {
	Enabled       *bool
	BridgeEnabled *bool
	NodeFeeWeight int64
	Traffic       ForwardingTrafficConfig
}

type ForwardingTrafficConfig struct {
	ControlCritical      mesh.ForwardingDisposition
	ControlQuery         mesh.ForwardingDisposition
	TransientInteractive mesh.ForwardingDisposition
	ReplicationStream    mesh.ForwardingDisposition
	SnapshotBulk         mesh.ForwardingDisposition
}

func ParseForwardingDisposition(raw string) (mesh.ForwardingDisposition, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "":
		return mesh.DispositionUnspecified, nil
	case "ALLOW":
		return mesh.DispositionAllow, nil
	case "DISCOURAGE":
		return mesh.DispositionDiscourage, nil
	case "DENY":
		return mesh.DispositionDeny, nil
	default:
		return mesh.DispositionUnspecified, fmt.Errorf("forwarding disposition must be ALLOW, DISCOURAGE, or DENY")
	}
}

func (c Config) EffectiveForwarding() ForwardingConfig {
	return c.Forwarding.withDefaults()
}

func (c Config) ZeroMQForwardingEnabled() bool {
	withDefaults := c.WithDefaults()
	return boolValue(withDefaults.ZeroMQ.ForwardingEnabled, boolValue(withDefaults.Forwarding.Enabled, true))
}

func (c Config) MeshForwardingPolicy() *mesh.ForwardingPolicy {
	forwarding := c.EffectiveForwarding()
	return &mesh.ForwardingPolicy{
		TransitEnabled: boolValue(forwarding.Enabled, true),
		BridgeEnabled:  boolValue(forwarding.BridgeEnabled, true),
		NodeFeeWeight:  forwarding.NodeFeeWeight,
		TrafficRules: []*mesh.TrafficRule{
			{TrafficClass: mesh.TrafficControlCritical, Disposition: forwarding.Traffic.ControlCritical},
			{TrafficClass: mesh.TrafficControlQuery, Disposition: forwarding.Traffic.ControlQuery},
			{TrafficClass: mesh.TrafficTransientInteractive, Disposition: forwarding.Traffic.TransientInteractive},
			{TrafficClass: mesh.TrafficReplicationStream, Disposition: forwarding.Traffic.ReplicationStream},
			{TrafficClass: mesh.TrafficSnapshotBulk, Disposition: forwarding.Traffic.SnapshotBulk},
		},
	}
}

func (c Config) LibP2PTransportCapability() *mesh.TransportCapability {
	if !c.LibP2P.Enabled {
		return nil
	}
	return &mesh.TransportCapability{
		Transport:                 mesh.TransportLibP2P,
		InboundEnabled:            len(c.LibP2P.ListenAddrs) > 0,
		OutboundEnabled:           true,
		NativeRelayClientEnabled:  c.LibP2P.NativeRelayClientEnabled,
		NativeRelayServiceEnabled: c.LibP2P.NativeRelayServiceEnabled,
		AdvertisedEndpoints:       append([]string(nil), c.LibP2P.ListenAddrs...),
	}
}

func (c Config) ZeroMQTransportCapability() *mesh.TransportCapability {
	if !c.ZeroMQ.Enabled {
		return nil
	}
	capability := &mesh.TransportCapability{
		Transport:       mesh.TransportZeroMQ,
		InboundEnabled:  c.zeroMQListenerEnabled(),
		OutboundEnabled: c.zeroMQDialEnabled(),
	}
	if c.zeroMQListenerEnabled() {
		capability.AdvertisedEndpoints = []string{c.ZeroMQ.BindURL}
	}
	return capability
}

func (c ForwardingConfig) withDefaults() ForwardingConfig {
	if c.Enabled == nil {
		c.Enabled = boolPtr(true)
	}
	if c.BridgeEnabled == nil {
		c.BridgeEnabled = boolPtr(true)
	}
	if c.NodeFeeWeight <= 0 {
		c.NodeFeeWeight = 1
	}
	c.Traffic = c.Traffic.withDefaults(c.NodeFeeWeight)
	return c
}

func (c ForwardingConfig) validate() error {
	for _, item := range []struct {
		name        string
		disposition mesh.ForwardingDisposition
	}{
		{name: "cluster forwarding traffic control_critical", disposition: c.Traffic.ControlCritical},
		{name: "cluster forwarding traffic control_query", disposition: c.Traffic.ControlQuery},
		{name: "cluster forwarding traffic transient_interactive", disposition: c.Traffic.TransientInteractive},
		{name: "cluster forwarding traffic replication_stream", disposition: c.Traffic.ReplicationStream},
		{name: "cluster forwarding traffic snapshot_bulk", disposition: c.Traffic.SnapshotBulk},
	} {
		if !isValidDisposition(item.disposition) {
			return fmt.Errorf("%s disposition is invalid", item.name)
		}
	}
	return nil
}

func (c ForwardingTrafficConfig) withDefaults(nodeFeeWeight int64) ForwardingTrafficConfig {
	defaults := mesh.DefaultForwardingPolicy(nodeFeeWeight)
	if c.ControlCritical == mesh.DispositionUnspecified {
		c.ControlCritical = mesh.DispositionForTraffic(defaults, mesh.TrafficControlCritical)
	}
	if c.ControlQuery == mesh.DispositionUnspecified {
		c.ControlQuery = mesh.DispositionForTraffic(defaults, mesh.TrafficControlQuery)
	}
	if c.TransientInteractive == mesh.DispositionUnspecified {
		c.TransientInteractive = mesh.DispositionForTraffic(defaults, mesh.TrafficTransientInteractive)
	}
	if c.ReplicationStream == mesh.DispositionUnspecified {
		c.ReplicationStream = mesh.DispositionForTraffic(defaults, mesh.TrafficReplicationStream)
	}
	if c.SnapshotBulk == mesh.DispositionUnspecified {
		c.SnapshotBulk = mesh.DispositionForTraffic(defaults, mesh.TrafficSnapshotBulk)
	}
	return c
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func boolPtr(value bool) *bool {
	return &value
}

func isValidDisposition(disposition mesh.ForwardingDisposition) bool {
	switch disposition {
	case mesh.DispositionUnspecified, mesh.DispositionAllow, mesh.DispositionDiscourage, mesh.DispositionDeny:
		return true
	default:
		return false
	}
}
