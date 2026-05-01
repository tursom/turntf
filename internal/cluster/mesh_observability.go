package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
)

// meshMetricSample 是网格转发的单个指标样本。
type meshMetricSample struct {
	TrafficClass string
	PathClass    string
	Value        uint64
}

// meshCostSample 是路由决策成本的单个样本。
type meshCostSample struct {
	TrafficClass string
	Value        int64
}

// meshMetricsSnapshot 是网格指标的快照集合。
type meshMetricsSnapshot struct {
	ForwardedPackets []meshMetricSample
	ForwardedBytes   []meshMetricSample
	RoutingNoPath    []meshMetricSample
	DecisionCost     []meshCostSample
	BridgeForwards   []meshMetricSample
}

// meshTrafficClassLabel 将流量类别枚举转换为可读的字符串标签。
func meshTrafficClassLabel(class mesh.TrafficClass) string {
	switch class {
	case mesh.TrafficControlCritical:
		return "control_critical"
	case mesh.TrafficControlQuery:
		return "control_query"
	case mesh.TrafficTransientInteractive:
		return "transient_interactive"
	case mesh.TrafficReplicationStream:
		return "replication_stream"
	case mesh.TrafficSnapshotBulk:
		return "snapshot_bulk"
	default:
		return "unspecified"
	}
}

// meshPathClassLabel 将路径类别枚举转换为可读的字符串标签。
func meshPathClassLabel(class mesh.PathClass) string {
	switch class {
	case mesh.PathClassDirect:
		return "direct"
	case mesh.PathClassSameTransportForward:
		return "same_transport_forward"
	case mesh.PathClassCrossTransportBridge:
		return "cross_transport_bridge"
	case mesh.PathClassNativeRelay:
		return "native_relay"
	default:
		return "unspecified"
	}
}

// meshTransportLabel 将传输类型枚举转换为字符串标签。
func meshTransportLabel(kind mesh.TransportKind) string {
	switch kind {
	case mesh.TransportLibP2P:
		return "libp2p"
	case mesh.TransportZeroMQ:
		return "zeromq"
	case mesh.TransportWebSocket:
		return "websocket"
	default:
		return "unspecified"
	}
}

// meshDispositionLabel 将转发处置枚举转换为字符串标签。
func meshDispositionLabel(disposition mesh.ForwardingDisposition) string {
	switch disposition {
	case mesh.DispositionAllow:
		return "allow"
	case mesh.DispositionDiscourage:
		return "discourage"
	case mesh.DispositionDeny:
		return "deny"
	default:
		return "unspecified"
	}
}

// meshMetricKey 组合流量类别和路径类别生成指标键。
func meshMetricKey(trafficClass string, pathClass string) string {
	return trafficClass + "|" + pathClass
}

// splitMeshMetricKey 将指标键拆分为流量类别和路径类别。
func splitMeshMetricKey(key string) (string, string) {
	parts := strings.SplitN(key, "|", 2)
	if len(parts) != 2 {
		return key, ""
	}
	return parts[0], parts[1]
}

// forwardMeshPayload 通过网格转发二进制负载。
func (m *Manager) forwardMeshPayload(ctx context.Context, targetNodeID int64, trafficClass mesh.TrafficClass, payload []byte) error {
	return m.forwardMeshPayloadWithPacketID(ctx, targetNodeID, trafficClass, 0, payload)
}

// routeMeshEnvelope 通过网格路由一个ClusterEnvelope。
func (m *Manager) routeMeshEnvelope(ctx context.Context, targetNodeID int64, trafficClass mesh.TrafficClass, envelope *mesh.ClusterEnvelope) error {
	binding := m.MeshRuntime()
	if binding == nil {
		return fmt.Errorf("mesh runtime is not attached")
	}
	return binding.RouteEnvelope(ctx, targetNodeID, envelope)
}

// forwardMeshPayloadWithPacketID 通过网格转发带数据包ID的二进制负载。
func (m *Manager) forwardMeshPayloadWithPacketID(ctx context.Context, targetNodeID int64, trafficClass mesh.TrafficClass, packetID uint64, payload []byte) error {
	return m.forwardMeshPayloadWithPacketIDAndTTL(ctx, targetNodeID, trafficClass, packetID, 0, payload)
}

// forwardMeshTransientPacket 通过网格转发一个瞬态数据包。
func (m *Manager) forwardMeshTransientPacket(ctx context.Context, packet store.TransientPacket) error {
	binding := m.MeshRuntime()
	if binding == nil {
		return fmt.Errorf("mesh runtime is not attached")
	}
	ttlHops := uint32(packet.TTLHops)
	if ttlHops == 0 {
		ttlHops = mesh.DefaultTTLHops
	}
	return binding.ForwardPacket(ctx, &mesh.ForwardedPacket{
		PacketId:        packet.PacketID,
		SourceNodeId:    packet.SourceNodeID,
		TargetNodeId:    packet.TargetNodeID,
		TrafficClass:    mesh.TrafficTransientInteractive,
		TtlHops:         ttlHops,
		TransientPacket: transientPacketProto(packet),
	})
}

// forwardMeshPayloadWithPacketIDAndTTL 通过网格转发带数据包ID和TTL的二进制负载。
func (m *Manager) forwardMeshPayloadWithPacketIDAndTTL(ctx context.Context, targetNodeID int64, trafficClass mesh.TrafficClass, packetID uint64, ttlHops uint32, payload []byte) error {
	binding := m.MeshRuntime()
	if binding == nil {
		return fmt.Errorf("mesh runtime is not attached")
	}
	if packetID == 0 {
		packetID = m.nextMeshForwardPacketID()
	}
	if ttlHops == 0 {
		ttlHops = mesh.DefaultTTLHops
	}
	packet := &mesh.ForwardedPacket{
		PacketId:     packetID,
		SourceNodeId: m.cfg.NodeID,
		TargetNodeId: targetNodeID,
		TrafficClass: trafficClass,
		TtlHops:      ttlHops,
		Payload:      payload,
	}
	return binding.ForwardPacket(ctx, packet)
}

// recordMeshForwarded 记录一次网格转发操作的指标。
func (m *Manager) recordMeshForwarded(trafficClass mesh.TrafficClass, pathClass mesh.PathClass, bytes int, cost int64) {
	if m == nil {
		return
	}
	classLabel := meshTrafficClassLabel(trafficClass)
	pathLabel := meshPathClassLabel(pathClass)
	key := meshMetricKey(classLabel, pathLabel)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.meshForwardedPackets == nil {
		m.meshForwardedPackets = make(map[string]uint64)
	}
	if m.meshForwardedBytes == nil {
		m.meshForwardedBytes = make(map[string]uint64)
	}
	if m.meshRoutingDecisionCost == nil {
		m.meshRoutingDecisionCost = make(map[string]int64)
	}
	m.meshForwardedPackets[key]++
	m.meshForwardedBytes[key] += uint64(maxInt64(int64(bytes), 0))
	m.meshRoutingDecisionCost[classLabel] = cost
	if pathClass == mesh.PathClassCrossTransportBridge {
		if m.meshBridgeForwards == nil {
			m.meshBridgeForwards = make(map[string]uint64)
		}
		m.meshBridgeForwards[meshMetricKey(classLabel, pathLabel)]++
	}
}

// observeMeshForwarding 处理网格转发的观测结果。
func (m *Manager) observeMeshForwarding(observation mesh.ForwardingObservation) {
	if m == nil {
		return
	}
	if observation.NoPath {
		m.recordMeshNoPath(observation.TrafficClass)
		return
	}
	m.recordMeshForwarded(observation.TrafficClass, observation.PathClass, observation.PayloadBytes, observation.EstimatedCost)
}

// recordMeshNoPath 记录一次无路径的网格路由尝试。
func (m *Manager) recordMeshNoPath(trafficClass mesh.TrafficClass) {
	if m == nil {
		return
	}
	classLabel := meshTrafficClassLabel(trafficClass)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.meshRoutingNoPath == nil {
		m.meshRoutingNoPath = make(map[string]uint64)
	}
	m.meshRoutingNoPath[meshMetricKey(classLabel, "no_path")]++
}

// meshMetricsSnapshot 获取网格指标的快照副本。
func (m *Manager) meshMetricsSnapshot() meshMetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return meshMetricsSnapshot{
		ForwardedPackets: meshMetricSamples(m.meshForwardedPackets),
		ForwardedBytes:   meshMetricSamples(m.meshForwardedBytes),
		RoutingNoPath:    meshMetricSamples(m.meshRoutingNoPath),
		DecisionCost:     meshCostSamples(m.meshRoutingDecisionCost),
		BridgeForwards:   meshMetricSamples(m.meshBridgeForwards),
	}
}

// meshMetricSamples 将map转换为排序的样本切片。
func meshMetricSamples(values map[string]uint64) []meshMetricSample {
	out := make([]meshMetricSample, 0, len(values))
	for key, value := range values {
		traffic, path := splitMeshMetricKey(key)
		out = append(out, meshMetricSample{TrafficClass: traffic, PathClass: path, Value: value})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TrafficClass != out[j].TrafficClass {
			return out[i].TrafficClass < out[j].TrafficClass
		}
		return out[i].PathClass < out[j].PathClass
	})
	return out
}

// meshCostSamples 将成本map转换为排序的样本切片。
func meshCostSamples(values map[string]int64) []meshCostSample {
	out := make([]meshCostSample, 0, len(values))
	for key, value := range values {
		out = append(out, meshCostSample{TrafficClass: key, Value: value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TrafficClass < out[j].TrafficClass })
	return out
}
