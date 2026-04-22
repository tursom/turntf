package mesh

import (
	"bytes"
	"sort"

	"google.golang.org/protobuf/proto"
)

var topologyMarshalOptions = proto.MarshalOptions{Deterministic: true}

func NormalizeTopologyUpdate(update *TopologyUpdate) *TopologyUpdate {
	if update == nil || update.OriginNodeId <= 0 {
		return nil
	}
	return &TopologyUpdate{
		OriginNodeId:     update.OriginNodeId,
		Generation:       update.Generation,
		ForwardingPolicy: NormalizeForwardingPolicy(ClonePolicy(update.ForwardingPolicy)),
		Transports:       normalizeTopologyCapabilities(update.Transports),
		Links:            normalizeTopologyLinks(update.OriginNodeId, update.Links),
	}
}

func TopologyUpdatesEqual(left, right *TopologyUpdate) bool {
	leftFingerprint, ok := topologyUpdateFingerprint(left)
	if !ok {
		return right == nil || NormalizeTopologyUpdate(right) == nil
	}
	rightFingerprint, ok := topologyUpdateFingerprint(right)
	if !ok {
		return false
	}
	return bytes.Equal(leftFingerprint, rightFingerprint)
}

func topologyUpdateFingerprint(update *TopologyUpdate) ([]byte, bool) {
	normalized := NormalizeTopologyUpdate(update)
	if normalized == nil {
		return nil, false
	}
	fingerprint, err := topologyMarshalOptions.Marshal(normalized)
	if err != nil {
		return nil, false
	}
	return fingerprint, true
}

func normalizeTopologyCapabilities(capabilities []*TransportCapability) []*TransportCapability {
	byTransport := make(map[TransportKind]*TransportCapability, len(capabilities))
	kinds := make([]TransportKind, 0, len(capabilities))
	for _, capability := range capabilities {
		if capability == nil || capability.Transport == TransportUnspecified {
			continue
		}
		cloned := CloneCapability(capability)
		sort.Strings(cloned.AdvertisedEndpoints)
		if _, exists := byTransport[cloned.Transport]; !exists {
			kinds = append(kinds, cloned.Transport)
		}
		byTransport[cloned.Transport] = cloned
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	normalized := make([]*TransportCapability, 0, len(kinds))
	for _, kind := range kinds {
		normalized = append(normalized, byTransport[kind])
	}
	return normalized
}

func normalizeTopologyLinks(originNodeID int64, links []*LinkAdvertisement) []*LinkAdvertisement {
	type normalizedKey struct {
		from      int64
		to        int64
		transport TransportKind
	}
	byKey := make(map[normalizedKey]*LinkAdvertisement, len(links))
	keys := make([]normalizedKey, 0, len(links))
	for _, link := range links {
		if link == nil || link.FromNodeId <= 0 || link.ToNodeId <= 0 || link.Transport == TransportUnspecified {
			continue
		}
		if link.FromNodeId != originNodeID {
			continue
		}
		key := normalizedKey{
			from:      link.FromNodeId,
			to:        link.ToNodeId,
			transport: link.Transport,
		}
		if _, exists := byKey[key]; !exists {
			keys = append(keys, key)
		}
		byKey[key] = &LinkAdvertisement{
			FromNodeId:  link.FromNodeId,
			ToNodeId:    link.ToNodeId,
			Transport:   link.Transport,
			PathClass:   link.PathClass,
			CostMs:      link.CostMs,
			JitterMs:    link.JitterMs,
			Established: link.Established,
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].from != keys[j].from {
			return keys[i].from < keys[j].from
		}
		if keys[i].to != keys[j].to {
			return keys[i].to < keys[j].to
		}
		return keys[i].transport < keys[j].transport
	})
	normalized := make([]*LinkAdvertisement, 0, len(keys))
	for _, key := range keys {
		normalized = append(normalized, byKey[key])
	}
	return normalized
}
