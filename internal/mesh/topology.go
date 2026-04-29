package mesh

import "sync"

type NodeState struct {
	NodeID           int64
	ProtocolVersion  string
	ForwardingPolicy *ForwardingPolicy
	TransportCaps    map[TransportKind]*TransportCapability
}

type LinkState struct {
	OriginNodeID int64
	FromNodeID   int64
	ToNodeID     int64
	Transport    TransportKind
	PathClass    PathClass
	CostMs       int64
	JitterMs     int64
	Established  bool
}

type TopologySnapshot struct {
	Nodes              map[int64]NodeState
	Links              []LinkState
	TopologyGeneration uint64
	outgoingLinks      map[topologyAdjacencyKey][]LinkState
}

type linkKey struct {
	origin    int64
	from      int64
	to        int64
	transport TransportKind
}

type topologyAdjacencyKey struct {
	from      int64
	transport TransportKind
}

type storeNode struct {
	protocolVersion string
	policy          *ForwardingPolicy
	capabilities    map[TransportKind]*TransportCapability
}

type MemoryTopologyStore struct {
	mu         sync.RWMutex
	nodes      map[int64]*storeNode
	links      map[linkKey]LinkState
	generation map[int64]uint64
	updates    map[int64]*TopologyUpdate
	snapshot   TopologySnapshot
}

func NewMemoryTopologyStore() *MemoryTopologyStore {
	return &MemoryTopologyStore{
		nodes:      make(map[int64]*storeNode),
		links:      make(map[linkKey]LinkState),
		generation: make(map[int64]uint64),
		updates:    make(map[int64]*TopologyUpdate),
	}
}

func (s *MemoryTopologyStore) ApplyHello(nodeID int64, hello *NodeHello) {
	if s == nil || hello == nil || nodeID <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.ensureNodeLocked(nodeID)
	node.protocolVersion = hello.ProtocolVersion
	node.policy = NormalizeForwardingPolicy(ClonePolicy(hello.ForwardingPolicy))
	node.capabilities = make(map[TransportKind]*TransportCapability, len(hello.Transports))
	for _, capability := range hello.Transports {
		if capability == nil || capability.Transport == TransportUnspecified {
			continue
		}
		node.capabilities[capability.Transport] = CloneCapability(capability)
	}
	s.rebuildSnapshotLocked()
}

func (s *MemoryTopologyStore) ApplyTopologyUpdate(update *TopologyUpdate) {
	normalized := NormalizeTopologyUpdate(update)
	if s == nil || normalized == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	currentGeneration := s.generation[normalized.OriginNodeId]
	if normalized.Generation < currentGeneration {
		return
	}
	if normalized.Generation == currentGeneration && currentGeneration != 0 {
		if TopologyUpdatesEqual(normalized, s.updates[normalized.OriginNodeId]) {
			return
		}
		return
	}
	s.generation[normalized.OriginNodeId] = normalized.Generation
	s.updates[normalized.OriginNodeId] = NormalizeTopologyUpdate(normalized)
	node := s.ensureNodeLocked(normalized.OriginNodeId)
	node.policy = NormalizeForwardingPolicy(ClonePolicy(normalized.ForwardingPolicy))
	node.capabilities = make(map[TransportKind]*TransportCapability, len(normalized.Transports))
	for _, capability := range normalized.Transports {
		node.capabilities[capability.Transport] = CloneCapability(capability)
	}
	for key := range s.links {
		if key.origin == normalized.OriginNodeId {
			delete(s.links, key)
		}
	}
	for _, link := range normalized.Links {
		key := linkKey{
			origin:    normalized.OriginNodeId,
			from:      link.FromNodeId,
			to:        link.ToNodeId,
			transport: link.Transport,
		}
		s.links[key] = LinkState{
			OriginNodeID: normalized.OriginNodeId,
			FromNodeID:   link.FromNodeId,
			ToNodeID:     link.ToNodeId,
			Transport:    link.Transport,
			PathClass:    link.PathClass,
			CostMs:       int64(link.CostMs),
			JitterMs:     int64(link.JitterMs),
			Established:  link.Established,
		}
	}
	s.rebuildSnapshotLocked()
}

func (s *MemoryTopologyStore) Snapshot() TopologySnapshot {
	if s == nil {
		return TopologySnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *MemoryTopologyStore) rebuildSnapshotLocked() {
	if s == nil {
		return
	}
	snapshot := TopologySnapshot{
		Nodes:         make(map[int64]NodeState, len(s.nodes)),
		Links:         make([]LinkState, 0, len(s.links)),
		outgoingLinks: make(map[topologyAdjacencyKey][]LinkState, len(s.links)),
	}
	for nodeID, node := range s.nodes {
		caps := make(map[TransportKind]*TransportCapability, len(node.capabilities))
		for kind, capability := range node.capabilities {
			caps[kind] = CloneCapability(capability)
		}
		snapshot.Nodes[nodeID] = NodeState{
			NodeID:           nodeID,
			ProtocolVersion:  node.protocolVersion,
			ForwardingPolicy: NormalizeForwardingPolicy(ClonePolicy(node.policy)),
			TransportCaps:    caps,
		}
	}
	for _, link := range s.links {
		snapshot.Links = append(snapshot.Links, link)
		snapshot.addOutgoingLink(link)
		if gen := s.generation[link.OriginNodeID]; gen > snapshot.TopologyGeneration {
			snapshot.TopologyGeneration = gen
		}
	}
	for origin, gen := range s.generation {
		if _, ok := snapshot.Nodes[origin]; !ok {
			snapshot.Nodes[origin] = NodeState{
				NodeID:           origin,
				ForwardingPolicy: NormalizeForwardingPolicy(DefaultForwardingPolicy(1)),
				TransportCaps:    make(map[TransportKind]*TransportCapability),
			}
		}
		if gen > snapshot.TopologyGeneration {
			snapshot.TopologyGeneration = gen
		}
	}
	s.snapshot = snapshot
}

func (s *MemoryTopologyStore) ensureNodeLocked(nodeID int64) *storeNode {
	node := s.nodes[nodeID]
	if node == nil {
		node = &storeNode{
			policy:       NormalizeForwardingPolicy(DefaultForwardingPolicy(1)),
			capabilities: make(map[TransportKind]*TransportCapability),
		}
		s.nodes[nodeID] = node
	}
	if node.policy == nil {
		node.policy = NormalizeForwardingPolicy(DefaultForwardingPolicy(1))
	}
	if node.capabilities == nil {
		node.capabilities = make(map[TransportKind]*TransportCapability)
	}
	return node
}

func (s TopologySnapshot) Node(nodeID int64) (NodeState, bool) {
	node, ok := s.Nodes[nodeID]
	return node, ok
}

func (s *TopologySnapshot) ensureOutgoingLinks() {
	if s == nil || s.outgoingLinks != nil {
		return
	}
	s.outgoingLinks = make(map[topologyAdjacencyKey][]LinkState, len(s.Links))
	for _, link := range s.Links {
		s.addOutgoingLink(link)
	}
}

func (s *TopologySnapshot) addOutgoingLink(link LinkState) {
	if s == nil {
		return
	}
	if s.outgoingLinks == nil {
		s.outgoingLinks = make(map[topologyAdjacencyKey][]LinkState)
	}
	key := topologyAdjacencyKey{
		from:      link.FromNodeID,
		transport: link.Transport,
	}
	s.outgoingLinks[key] = append(s.outgoingLinks[key], link)
}

func (s TopologySnapshot) outgoing(nodeID int64, transport TransportKind) []LinkState {
	return s.outgoingLinks[topologyAdjacencyKey{
		from:      nodeID,
		transport: transport,
	}]
}

func (n NodeState) OutboundEnabled(kind TransportKind) bool {
	capability := n.TransportCaps[kind]
	return capability != nil && capability.OutboundEnabled
}

func (n NodeState) HasTransport(kind TransportKind) bool {
	_, ok := n.TransportCaps[kind]
	return ok
}
