package cluster

import (
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/mesh"
)

// ForwardingConfig 定义网格转发的全局策略配置。
type ForwardingConfig struct {
	// Enabled 控制网格转发是否启用。nil表示默认启用。
	Enabled *bool
	// BridgeEnabled 控制跨传输网桥是否启用。nil表示默认启用。
	BridgeEnabled *bool
	// NodeFeeWeight 是路由决策中节点费用的权重因子。
	NodeFeeWeight int64
	// Traffic 定义各流量类别的转发处置策略。
	Traffic ForwardingTrafficConfig
}

// ForwardingTrafficConfig 定义五种流量类别的转发处置策略。
type ForwardingTrafficConfig struct {
	ControlCritical      mesh.ForwardingDisposition
	ControlQuery         mesh.ForwardingDisposition
	TransientInteractive mesh.ForwardingDisposition
	ReplicationStream    mesh.ForwardingDisposition
	SnapshotBulk         mesh.ForwardingDisposition
}

// ParseForwardingDisposition 将字符串解析为转发处置枚举。
// 支持: ALLOW、DISCOURAGE、DENY（大小写不敏感）。
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

// EffectiveForwarding 返回填充了默认值的转发配置。
func (c Config) EffectiveForwarding() ForwardingConfig {
	return c.Forwarding.withDefaults()
}

// ZeroMQForwardingEnabled 检查ZeroMQ转发是否启用。
func (c Config) ZeroMQForwardingEnabled() bool {
	withDefaults := c.WithDefaults()
	return boolValue(withDefaults.ZeroMQ.ForwardingEnabled, boolValue(withDefaults.Forwarding.Enabled, true))
}

// MeshForwardingPolicy 构建网格运行时使用的转发策略对象。
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

// LibP2PTransportCapability 构建libp2p传输能力描述。
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

// ZeroMQTransportCapability 构建ZeroMQ传输能力描述。
func (c Config) ZeroMQTransportCapability() *mesh.TransportCapability {
	if !c.ZeroMQ.Enabled {
		return nil
	}
	forwardingEnabled := c.ZeroMQForwardingEnabled()
	capability := &mesh.TransportCapability{
		Transport:       mesh.TransportZeroMQ,
		InboundEnabled:  c.zeroMQListenerEnabled() && forwardingEnabled,
		OutboundEnabled: c.zeroMQDialEnabled() && forwardingEnabled,
	}
	if capability.InboundEnabled {
		capability.AdvertisedEndpoints = []string{zeroMQPeerURLForBindURL(c.ZeroMQ.BindURL)}
	}
	return capability
}

// zeroMQPeerURLForBindURL 将ZeroMQ绑定URL转换为对等节点URL格式。
func zeroMQPeerURLForBindURL(bindURL string) string {
	trimmed := strings.TrimSpace(bindURL)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(trimmed), peerSchemeZeroMQTCP+"://") {
		return trimmed
	}
	return peerSchemeZeroMQTCP + strings.TrimPrefix(trimmed, zeroMQBindSchemeTCP)
}

// withDefaults 返回填充了默认值的ForwardingConfig副本。
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

// validate 验证转发配置中的处置值是否有效。
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

// withDefaults 为未指定的流量类别填充默认处置策略。
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

// boolValue 解引用可选布尔值，nil时返回默认值。
func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

// boolPtr 创建布尔值的指针。
//
//go:fix inline
func boolPtr(value bool) *bool {
	return new(value)
}

// isValidDisposition 检查转发处置值是否有效。
func isValidDisposition(disposition mesh.ForwardingDisposition) bool {
	switch disposition {
	case mesh.DispositionUnspecified, mesh.DispositionAllow, mesh.DispositionDiscourage, mesh.DispositionDeny:
		return true
	default:
		return false
	}
}
