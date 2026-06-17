package mesh

import (
	"bytes"
	"sort"

	"google.golang.org/protobuf/proto"
)

var topologyMarshalOptions = proto.MarshalOptions{Deterministic: true}

// NormalizeTopologyUpdate 对拓扑更新消息进行标准化处理，返回一个新的标准化副本。
// 标准化过程：
//  1. 检查输入有效性（nil 或 OriginNodeId <= 0 时返回 nil）。
//  2. 深度克隆并标准化转发策略（补全缺失流量分类、去重）。
//  3. 按传输类型去重传输能力、排序端点、按传输类型排序。
//  4. 过滤链路至发出节点（OriginNodeId）、去重 (from, to, transport) 组合、排序。
//
// 标准化确保后续的指纹比较和版本比较具有一致性。
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

// TopologyUpdatesEqual 通过确定性的 protobuf 指纹比较两个拓扑更新是否语义相等。
// 比较过程：
//  1. 分别对两个更新调用 NormalizeTopologyUpdate 进行标准化。
//  2. 对标准化后的结果进行确定性 Marshal，生成字节指纹。
//  3. 比较两个指纹的字节是否相等。
//
// 这种方式避免了因字段顺序、端点列表排序等非语义差异导致的不相等判定。
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

// topologyUpdateFingerprint 对拓扑更新进行标准化后，使用确定性 protobuf 序列化生成字节指纹。
// 标准化保证了输入的一致性，确定性 Marshal 保证相同内容的序列化结果始终一致。
// 返回的指纹可直接用于相等性比较或作为哈希键。
// 标准化失败（如输入无效）时第二个返回值为 false。
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

// normalizeTopologyCapabilities 标准化传输能力列表：按 TransportKind 去重、对每个能力的端
// 点列表排序、按 TransportKind 升序排序输出。跳过 nil 或 TransportUnspecified 的能力。
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

// normalizeTopologyLinks 标准化链路列表：仅保留 FromNodeID 等于 originNodeID 的链路（源节点
// 只通告自己的出站链路），按 (FromNodeID, ToNodeID, Transport) 三元组去重，并按 (from, to, transport)
// 升序排序输出。跳过 nil 或关键字段无效的链路。
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
