package mesh

import (
	internalproto "github.com/tursom/turntf/internal/proto"
)

const (
	DefaultTTLHops      = 8
	RelayPenaltyMs      = 20
	BridgePenaltyMs     = 30
	DiscouragePenaltyMs = 200
)

var trafficClassFactor = map[TrafficClass]int64{
	TrafficControlCritical:      1,
	TrafficControlQuery:         2,
	TrafficTransientInteractive: 6,
	TrafficReplicationStream:    40,
	TrafficSnapshotBulk:         120,
}

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

func BridgeAllowedForTrafficClass(class TrafficClass) bool {
	switch class {
	case TrafficControlCritical, TrafficControlQuery, TrafficTransientInteractive:
		return true
	default:
		return false
	}
}

func TrafficClassFactor(class TrafficClass) int64 {
	if v, ok := trafficClassFactor[class]; ok {
		return v
	}
	return trafficClassFactor[TrafficControlCritical]
}

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
