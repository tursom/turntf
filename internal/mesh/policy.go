package mesh

import (
	internalproto "github.com/tursom/turntf/internal/proto"
)

// ---------------- 策略常量 ----------------

// DefaultTTLHops 是数据包默认的最大跳数。
const DefaultTTLHops = 8

// RelayPenaltyMs 是使用本地中继路径的额外代价（毫秒）。
const RelayPenaltyMs = 20

// BridgePenaltyMs 是在同一节点内跨传输桥接的额外代价（毫秒）。
const BridgePenaltyMs = 30

// DiscouragePenaltyMs 是节点策略标记为 Discourage 时施加的额外代价（毫秒）。
const DiscouragePenaltyMs = 200

// trafficClassFactor 定义各流量类别的权重系数，用于路径选择时的代价计算。
// 权重越大，路径选择越倾向于避开该流量，从而为批量流量减少拥塞。
// 控制关键: 1, 控制查询: 2, 瞬时交互: 6, 复制流: 40, 快照批量: 120。
var trafficClassFactor = map[TrafficClass]int64{
	TrafficControlCritical:      1,
	TrafficControlQuery:         2,
	TrafficTransientInteractive: 6,
	TrafficReplicationStream:    40,
	TrafficSnapshotBulk:         120,
}

// DefaultForwardingPolicy 返回基于 nodeFeeWeight 的默认转发策略。
// nodeFeeWeight <= 1 时允许所有流量；> 1 时劝阻瞬时交互、拒绝批量流量。
func DefaultForwardingPolicy(nodeFeeWeight int64) *ForwardingPolicy {
	if nodeFeeWeight <= 0 {
		nodeFeeWeight = 1
	}
	policy := &ForwardingPolicy{
		TransitEnabled: true,
		BridgeEnabled:  true,
		NodeFeeWeight:  nodeFeeWeight,
		TrafficRules: []*TrafficRule{
			{TrafficClass: TrafficControlCritical, Disposition: DispositionAllow},
			{TrafficClass: TrafficControlQuery, Disposition: DispositionAllow},
			{TrafficClass: TrafficTransientInteractive, Disposition: DispositionAllow},
			{TrafficClass: TrafficReplicationStream, Disposition: DispositionAllow},
			{TrafficClass: TrafficSnapshotBulk, Disposition: DispositionAllow},
		},
	}
	if nodeFeeWeight > 1 {
		policy.TrafficRules = []*TrafficRule{
			{TrafficClass: TrafficControlCritical, Disposition: DispositionAllow},
			{TrafficClass: TrafficControlQuery, Disposition: DispositionAllow},
			{TrafficClass: TrafficTransientInteractive, Disposition: DispositionDiscourage},
			{TrafficClass: TrafficReplicationStream, Disposition: DispositionDeny},
			{TrafficClass: TrafficSnapshotBulk, Disposition: DispositionDeny},
		}
	}
	return policy
}

// NormalizeForwardingPolicy 对策略进行标准化：为缺失的流量分类补充默认规则，
// 删除重复条目和未设置的条目。若输入为 nil 则返回默认策略。
func NormalizeForwardingPolicy(policy *ForwardingPolicy) *ForwardingPolicy {
	if policy == nil {
		return DefaultForwardingPolicy(1)
	}
	if policy.NodeFeeWeight <= 0 {
		policy.NodeFeeWeight = 1
	}
	seen := make(map[TrafficClass]struct{}, len(policy.TrafficRules))
	normalized := make([]*TrafficRule, 0, len(policy.TrafficRules)+5)
	for _, rule := range policy.TrafficRules {
		if rule == nil || rule.TrafficClass == TrafficClassUnspecified {
			continue
		}
		disposition := rule.Disposition
		if disposition == DispositionUnspecified {
			disposition = DispositionAllow
		}
		normalized = append(normalized, &TrafficRule{
			TrafficClass: rule.TrafficClass,
			Disposition:  disposition,
		})
		seen[rule.TrafficClass] = struct{}{}
	}
	for _, class := range []TrafficClass{
		TrafficControlCritical,
		TrafficControlQuery,
		TrafficTransientInteractive,
		TrafficReplicationStream,
		TrafficSnapshotBulk,
	} {
		if _, ok := seen[class]; ok {
			continue
		}
		normalized = append(normalized, &TrafficRule{
			TrafficClass: class,
			Disposition:  DispositionForTraffic(DefaultForwardingPolicy(policy.NodeFeeWeight), class),
		})
	}
	policy.TrafficRules = normalized
	return policy
}

// DispositionForTraffic 在策略中查找指定流量分类的处置动作。
// 未找到时默认返回 Allow。
func DispositionForTraffic(policy *ForwardingPolicy, class TrafficClass) ForwardingDisposition {
	if policy == nil {
		return DispositionAllow
	}
	for _, rule := range policy.TrafficRules {
		if rule != nil && rule.TrafficClass == class {
			if rule.Disposition == DispositionUnspecified {
				return DispositionAllow
			}
			return rule.Disposition
		}
	}
	return DispositionAllow
}

// BridgeAllowedForTrafficClass 检查指定流量类别是否允许跨传输桥接。
// 仅控制关键、控制查询和瞬时交互流量允许桥接。
func BridgeAllowedForTrafficClass(class TrafficClass) bool {
	switch class {
	case TrafficControlCritical, TrafficControlQuery, TrafficTransientInteractive:
		return true
	default:
		return false
	}
}

// TrafficClassFactor 返回指定流量类别的权重系数。未知类别返回 1（控制关键）。
func TrafficClassFactor(class TrafficClass) int64 {
	if v, ok := trafficClassFactor[class]; ok {
		return v
	}
	return trafficClassFactor[TrafficControlCritical]
}

// ClonePolicy 深度复制转发策略。输入为 nil 时返回 nil。
func ClonePolicy(policy *ForwardingPolicy) *ForwardingPolicy {
	if policy == nil {
		return nil
	}
	cloned := &ForwardingPolicy{
		TransitEnabled: policy.TransitEnabled,
		BridgeEnabled:  policy.BridgeEnabled,
		NodeFeeWeight:  policy.NodeFeeWeight,
		TrafficRules:   make([]*TrafficRule, 0, len(policy.TrafficRules)),
	}
	for _, rule := range policy.TrafficRules {
		if rule == nil {
			continue
		}
		cloned.TrafficRules = append(cloned.TrafficRules, &TrafficRule{
			TrafficClass: rule.TrafficClass,
			Disposition:  rule.Disposition,
		})
	}
	return cloned
}

// CloneCapability 深度复制传输能力。输入为 nil 时返回 nil。
func CloneCapability(capability *TransportCapability) *TransportCapability {
	if capability == nil {
		return nil
	}
	cloned := protoCloneCapability(capability)
	return cloned
}

func protoCloneCapability(capability *TransportCapability) *TransportCapability {
	cloned := &internalproto.MeshTransportCapability{
		Transport:                 capability.Transport,
		InboundEnabled:            capability.InboundEnabled,
		OutboundEnabled:           capability.OutboundEnabled,
		NativeRelayClientEnabled:  capability.NativeRelayClientEnabled,
		NativeRelayServiceEnabled: capability.NativeRelayServiceEnabled,
		AdvertisedEndpoints:       append([]string(nil), capability.AdvertisedEndpoints...),
	}
	return cloned
}
