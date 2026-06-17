package mesh

import (
	internalproto "github.com/tursom/turntf/internal/proto"
)

// ---------------- 策略常量 ----------------

// DefaultTTLHops 是转发数据包在网内存活的最大跳数（生存时间）。
// 当 TTL 降为 0 时，数据包被丢弃，防止无限循环转发。
// 默认值为 8，适用于中等规模的 mesh 网络。
const DefaultTTLHops = 8

// RelayPenaltyMs 是路径使用本地中继（NativeRelay）时施加的额外代价（毫秒）。
// 中继路径需要经过专门的 relay 节点，因此比直连路径代价更高。
const RelayPenaltyMs = 20

// BridgePenaltyMs 是在同一节点内跨传输桥接（如 WebSocket → LibP2P）时施加的额外代价（毫秒）。
// 跨传输桥接涉及协议转换和数据拷贝，因此代价值高于单传输中继。
const BridgePenaltyMs = 30

// DiscouragePenaltyMs 是当中间节点的转发策略对某流量类别标记为 Discourage 时，
// 经过该节点额外增加的代价值（毫秒）。这会使路径选择倾向于绕开该节点，
// 但不会完全禁止经过它（与 Deny 的区别）。
const DiscouragePenaltyMs = 200

// trafficClassFactor 定义各流量类别的权重系数，用于转发代价计算。
//
// 权重含义：该值作为 NodeFeeWeight 的乘数，控制中转节点对各类流量的"收费"比例。
// 权重越大，批量大流量越倾向于绕过中转节点，实现拥塞感知的流量工程。
//
// 各流量类别权重：
//   - TrafficControlCritical:      1   （控制消息，最小代价）
//   - TrafficControlQuery:         2   （查询请求/响应）
//   - TrafficTransientInteractive: 6   （交互式瞬时转发）
//   - TrafficReplicationStream:    40  （复制流数据）
//   - TrafficSnapshotBulk:         120 （快照批量传输，最大代价，倾向于直连）
var trafficClassFactor = map[TrafficClass]int64{
	TrafficControlCritical:      1,
	TrafficControlQuery:         2,
	TrafficTransientInteractive: 6,
	TrafficReplicationStream:    40,
	TrafficSnapshotBulk:         120,
}

// DefaultForwardingPolicy 基于 NodeFeeWeight 生成默认转发策略。
// NodeFeeWeight 控制节点对中转流量的"收费"意愿：
//   - <= 1：允许所有流量类别（利好中转，适合家庭节点或友好中继）。
//   - > 1：瞬时交互标记为 Discourage（劝退），复制流和快照批量标记为 Deny（拒绝）。
//     这可以防止大流量节点通过该节点中转造成拥塞。
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

// NormalizeForwardingPolicy 对转发策略进行标准化处理，返回标准化后的输入指针（原地修改）。
// 标准化步骤：
//  1. nil 输入 → 返回 DefaultForwardingPolicy(1)（宽松默认策略）。
//  2. NodeFeeWeight <= 0 → 设为 1。
//  3. 遍历 TrafficRules，过滤 nil 和 TrafficClassUnspecified 的条目；DispositionUnspecified → Allow。
//  4. 检查所有 5 种标准流量类别是否都已覆盖，缺失的按 DefaultForwardingPolicy(NodeFeeWeight) 补充。
//  5. 删除重复的流量类别规则（保留第一个出现的）。
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

// DispositionForTraffic 在给定转发策略中查找指定流量分类的处置动作（Allow / Discourage / Deny）。
// 遍历 TrafficRules 列表查找精确匹配的流量类别，返回对应的处置动作。
// DispositionUnspecified 视为 Allow。
// 未找到匹配规则或策略为 nil 时默认返回 DispositionAllow（宽松默认）。
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
// 仅允许延迟敏感或小体积的流量类别进行桥接：
//   - TrafficControlCritical（控制消息）：允许。
//   - TrafficControlQuery（查询请求/响应）：允许。
//   - TrafficTransientInteractive（瞬时交互转发）：允许。
//   - TrafficReplicationStream 和 TrafficSnapshotBulk（大流量批量传输）：不允许。
//
// 限制桥接防止大流量跨传输造成拥塞和资源浪费。
func BridgeAllowedForTrafficClass(class TrafficClass) bool {
	switch class {
	case TrafficControlCritical, TrafficControlQuery, TrafficTransientInteractive:
		return true
	default:
		return false
	}
}

// TrafficClassFactor 返回指定流量类别的权重系数，用于转发代价计算。
// 系数越大，该流量经过中转节点时产生的 NodeFeeWeight × Factor 代价越高，
// 路径选择会倾向于减少中转跳数。
// 未知流量类别返回 TrafficControlCritical 的权重（1），确保向后兼容。
func TrafficClassFactor(class TrafficClass) int64 {
	if v, ok := trafficClassFactor[class]; ok {
		return v
	}
	return trafficClassFactor[TrafficControlCritical]
}

// ClonePolicy 深度复制 ForwardingPolicy，返回独立的副本。
// 复制包括所有流量规则（TrafficRules）的逐条深度复制。
// 输入为 nil 时返回 nil，调用方可直接比较或存储副本而不影响原件。
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

// CloneCapability 深度复制 TransportCapability，返回独立的副本。
// AdvertisedEndpoints 切片也被深度复制，修改副本不会影响原件。
// 输入为 nil 时返回 nil。
func CloneCapability(capability *TransportCapability) *TransportCapability {
	if capability == nil {
		return nil
	}
	cloned := protoCloneCapability(capability)
	return cloned
}

// protoCloneCapability 是 CloneCapability 的内部实现。
// 手动深度复制 MeshTransportCapability 的所有字段，
// 并对 AdvertisedEndpoints 切片进行独立复制。
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
