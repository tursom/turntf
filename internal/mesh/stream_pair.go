package mesh

import "time"

// Round-trip coherent stream paths.
//
// A stream's goodput depends on its round trip: Data travels one way and the
// receiver's Acks the other, and TCP carried inside the stream (TUN) has its
// own acknowledgements in the peer's stream back. Ping RTT measures one
// adjacency's round trip, so paths are coherent when both directions use the
// same adjacency:
//
//   - Acks and OpenAcks for an inbound stream return on the adjacency where its
//     Open, Resume or Data last arrived. Cumulative Acks tolerate reordering,
//     so they may follow every move of the sender.
//   - Between two nodes the lower node ID leads: it ranks adjacencies on its
//     own. The other aligns its streams with the adjacency where the leader's
//     Data arrives, unless that adjacency scores worse than its own choice by
//     more than the reselection margin. A fixed leader keeps both sides from
//     chasing each other's previous choice.
const pairIngressFreshFor = 30 * time.Second

type pairIngressEntry struct {
	adj *Adjacency
	at  time.Time
}

func adjacencyEstablished(adj *Adjacency) bool {
	adj.mu.Lock()
	defer adj.mu.Unlock()
	return adj.established
}

// observeStreamIngress records where the remote's stream frames arrive. adj is
// nil for frames that reached this node through forwarding.
func (r *Runtime) observeStreamIngress(key directStreamAffinityKey, adj *Adjacency, frame *StreamFrame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if frame.Kind == streamFrameKindClose {
		delete(r.streamIngress, key)
		return
	}
	if adj == nil {
		return
	}
	switch frame.Kind {
	case streamFrameKindOpen, streamFrameKindResume, streamFrameKindData:
		if _, ok := r.streamIngress[key]; ok || len(r.streamIngress) < directStreamAffinityLimit {
			r.streamIngress[key] = adj
		}
	}
	if frame.Kind == streamFrameKindData {
		r.pairIngress[key.targetNodeID] = pairIngressEntry{adj: adj, at: time.Now()}
	}
}

// streamAckIngressLocked requires r.mu. It returns the adjacency an Ack or
// OpenAck for an inbound stream should return on, or nil to select normally.
func (r *Runtime) streamAckIngressLocked(key directStreamAffinityKey, frame *StreamFrame) *Adjacency {
	if frame.Kind != streamFrameKindAck && frame.Kind != streamFrameKindOpenAck {
		return nil
	}
	adj := r.streamIngress[key]
	if adj == nil || !adjacencyEstablished(adj) {
		return nil
	}
	return adj
}

// pairAlignedAdjacencyLocked requires r.mu. For the following side of a node
// pair it returns the leader's ingress adjacency when it is usable and not
// worse than best by the reselection margin; otherwise nil.
func (r *Runtime) pairAlignedAdjacencyLocked(targetNodeID int64, best *Adjacency, decision RouteDecision, directRoute bool) *Adjacency {
	if r.localNodeID < targetNodeID {
		return nil
	}
	entry, ok := r.pairIngress[targetNodeID]
	if !ok || time.Since(entry.at) > pairIngressFreshFor {
		return nil
	}
	adj := entry.adj
	if adj == best {
		return adj
	}
	if !adjacencyEstablished(adj) {
		return nil
	}
	if directRoute && adj.Transport != decision.OutboundTransport {
		return nil
	}
	if !directRoute && best != nil && best.Transport == TransportTCPMTLS && adj.Transport != TransportTCPMTLS {
		return nil
	}
	if best == nil {
		return adj
	}
	now := time.Now()
	best.mu.Lock()
	bestScore := r.directStreamScoreLocked(best, now)
	best.mu.Unlock()
	adj.mu.Lock()
	score := r.directStreamScoreLocked(adj, now)
	adj.mu.Unlock()
	if score > bestScore+directStreamReselectMargin(bestScore) {
		return nil
	}
	return adj
}
