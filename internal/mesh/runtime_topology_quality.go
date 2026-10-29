package mesh

import "google.golang.org/protobuf/proto"

// topologyQualityChanged compares normalized aggregates against the last
// advertised costs, not the preceding sample. This both suppresses unchanged
// parallel-path measurements and retains cumulative, gradual quality changes.
// Structural differences always pass; only ordinary metric drift is filtered.
func topologyQualityChanged(previous, current *TopologyUpdate) bool {
	if previous == nil || current == nil {
		return previous != current
	}
	if previous.OriginNodeId != current.OriginNodeId || previous.RuntimeEpoch != current.RuntimeEpoch ||
		!proto.Equal(previous.ForwardingPolicy, current.ForwardingPolicy) ||
		len(previous.Transports) != len(current.Transports) || len(previous.Links) != len(current.Links) {
		return true
	}
	for i, capability := range current.Transports {
		if !proto.Equal(previous.Transports[i], capability) {
			return true
		}
	}
	for i, link := range current.Links {
		old := previous.Links[i]
		if old.FromNodeId != link.FromNodeId || old.ToNodeId != link.ToNodeId || old.Transport != link.Transport ||
			old.PathClass != link.PathClass || old.Established != link.Established ||
			linkCostChangedMeaningfully(int64(old.CostMs)+int64(old.JitterMs), int64(link.CostMs)+int64(link.JitterMs)) {
			return true
		}
	}
	return false
}
