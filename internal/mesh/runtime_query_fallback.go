package mesh

import "time"

const controlQueryHold = 10 * time.Second

// A backup must leave headroom in the three-second session-query budget for
// scheduling and remote processing. RTT plus twice the smoothed deviation is a
// conservative local estimate, not a delivery deadline or a measured quantile.
// A jittery backup can still be useful when native TCP is persistently worse;
// an absolute low-jitter gate would reject both real cc/home backup paths.
const controlQueryMaxDelayMillis int64 = 1000

// The ordinary planner retains native TCP priority. This local wrapper changes
// only locally originated control queries, using measurements unavailable in
// topology advertisements. Ordered stream and consensus decisions stay intact.
type runtimeQueryPlanner struct {
	runtime *Runtime
	base    *Planner
}

func (p *runtimeQueryPlanner) Compute(snapshot TopologySnapshot, destination int64, class TrafficClass, ingress TransportKind) (RouteDecision, bool) {
	decision, ok := p.base.Compute(snapshot, destination, class, ingress)
	if !ok || class != TrafficControlQuery || ingress != TransportUnspecified || decision.NextHopNodeID != destination || decision.OutboundTransport != TransportTCPMTLS {
		return decision, ok
	}
	return p.runtime.controlQueryRoute(snapshot, destination, decision, time.Now()), true
}

type controlQueryFallback struct {
	native, backup          *Adjacency
	lastSample              int
	badSamples, goodSamples int
	badSince, goodSince     time.Time
	active                  bool
}

type queryQuality struct {
	cost    int64
	samples int
	healthy bool
}

// queryQualityLocked requires adj.mu. Freshness fits the existing eight-second
// standby cadence, while refusing stale, excessively slow or failing backups.
func (r *Runtime) queryQualityLocked(adj *Adjacency, now time.Time) queryQuality {
	freshFor := min(r.livenessTimeout/2, max(3*r.pingInterval, 2*min(4*r.pingInterval, r.livenessTimeout/3)))
	fresh := !adj.lastProbeSuccess.IsZero() && !now.Before(adj.lastProbeSuccess) && now.Sub(adj.lastProbeSuccess) <= freshFor
	// Match the connection score used by bestAdjacencyLocked so the comparison
	// includes both baseline delay and variability on the actual sending path.
	cost := r.adjacencyCostLocked(adj, now) + int64(adj.jitterEWMA)
	return queryQuality{cost: cost, samples: adj.samples,
		healthy: adj.established && adj.samples >= 8 && fresh && adj.sendFailures == 0 && !adj.probeDegraded &&
			cost <= controlQueryMaxDelayMillis && (adj.pingStarted.IsZero() || now.Sub(adj.pingStarted) < 3*r.pingInterval)}
}

// packetAdjacency uses the same qualification at send time as the fallback
// planner. A newly registered, zero-sample connection must not displace the
// measured backup between planning and sending. Ordinary and transit traffic
// retain their existing connection selection.
func (r *Runtime) packetAdjacency(nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) *Adjacency {
	if nextHopNodeID <= 0 || transport == TransportUnspecified {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if packet != nil && packet.SourceNodeId == r.localNodeID && packet.TargetNodeId == nextHopNodeID &&
		packet.TrafficClass == TrafficControlQuery && transport != TransportTCPMTLS &&
		r.bestAdjacencyLocked(nextHopNodeID, TransportTCPMTLS) != nil {
		adj, _ := r.queryBackupAdjacencyLocked(nextHopNodeID, transport, time.Now())
		return adj
	}
	return r.bestAdjacencyLocked(nextHopNodeID, transport)
}

// queryBackupAdjacencyLocked requires r.mu. Both planning and sending select
// the lowest-scoring qualified direct connection, never an unmeasured sibling.
func (r *Runtime) queryBackupAdjacencyLocked(nodeID int64, transport TransportKind, now time.Time) (*Adjacency, queryQuality) {
	var best *Adjacency
	var quality queryQuality
	for _, adj := range r.adjByRoute[routeAdjacencyKey{nodeID: nodeID, transport: transport}] {
		if adj == nil || classifyPathClass(adj) != PathClassDirect {
			continue
		}
		adj.mu.Lock()
		q := r.queryQualityLocked(adj, now)
		adj.mu.Unlock()
		if q.healthy && (best == nil || q.cost < quality.cost) {
			best, quality = adj, q
		}
	}
	return best, quality
}

// queryCandidatesLocked requires r.mu. Only direct backup links qualify, so
// no new transit is introduced. The sender repeats the same qualification.
func (r *Runtime) queryCandidatesLocked(nodeID int64, now time.Time) (*Adjacency, queryQuality, *Adjacency, queryQuality) {
	native := r.bestAdjacencyLocked(nodeID, TransportTCPMTLS)
	if native == nil {
		return nil, queryQuality{}, nil, queryQuality{}
	}
	native.mu.Lock()
	nq := r.queryQualityLocked(native, now)
	native.mu.Unlock()
	var backup *Adjacency
	var bq queryQuality
	for key := range r.adjByRoute {
		if key.nodeID != nodeID || key.transport == TransportTCPMTLS {
			continue
		}
		adj, q := r.queryBackupAdjacencyLocked(nodeID, key.transport, now)
		if adj == nil {
			continue
		}
		if q.healthy && (backup == nil || q.cost < bq.cost || (q.cost == bq.cost && adj.Transport < backup.Transport)) {
			backup, bq = adj, q
		}
	}
	return native, nq, backup, bq
}

func (r *Runtime) refreshControlQueryFallback(nodeID int64, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refreshControlQueryFallbackLocked(nodeID, now)
}

func (r *Runtime) refreshControlQueryFallbackLocked(nodeID int64, now time.Time) *controlQueryFallback {
	native, nq, backup, bq := r.queryCandidatesLocked(nodeID, now)
	if native == nil || backup == nil || nq.samples < 8 {
		delete(r.queryFallbacks, nodeID)
		return nil
	}
	if r.queryFallbacks == nil {
		r.queryFallbacks = make(map[int64]*controlQueryFallback)
	}
	state := r.queryFallbacks[nodeID]
	if state == nil || state.native != native || state.backup != backup {
		state = &controlQueryFallback{native: native, backup: backup, lastSample: -1}
		r.queryFallbacks[nodeID] = state
	}
	// One unusually bad RTT and repeated route lookups cannot trigger a switch:
	// both transitions require new native probe samples and ten elapsed seconds.
	newSample := nq.samples != state.lastSample
	state.lastSample = nq.samples
	bad := nq.cost >= 2*bq.cost && nq.cost-bq.cost >= 100
	good := nq.healthy && (nq.cost <= bq.cost+50 || nq.cost*4 <= bq.cost*5)
	if !state.active {
		if !bad {
			state.badSamples = 0
			state.badSince = time.Time{}
			return state
		}
		if newSample {
			if state.badSamples == 0 {
				state.badSince = now
			}
			state.badSamples++
			if state.badSamples >= 3 && now.Sub(state.badSince) >= controlQueryHold {
				state.active = true
			}
		}
	} else {
		if !good {
			state.goodSamples = 0
			state.goodSince = time.Time{}
			return state
		}
		if newSample {
			if state.goodSamples == 0 {
				state.goodSince = now
			}
			state.goodSamples++
			if state.goodSamples >= 3 && now.Sub(state.goodSince) >= controlQueryHold {
				delete(r.queryFallbacks, nodeID)
				return nil
			}
		}
	}
	return state
}

func (r *Runtime) controlQueryRoute(snapshot TopologySnapshot, nodeID int64, original RouteDecision, now time.Time) RouteDecision {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.refreshControlQueryFallbackLocked(nodeID, now)
	if state == nil || !state.active {
		return original
	}
	kind := state.backup.Transport
	local, ok := snapshot.Node(r.localNodeID)
	if !ok {
		return original
	}
	if cap := local.TransportCaps[kind]; cap == nil || !cap.OutboundEnabled {
		return original
	}
	// The immutable topology must also permit the measured direct alternative.
	// No policy, multi-hop search, remote advertisement or stream pin is changed.
	snapshot.ensureOutgoingLinks()
	for _, link := range snapshot.outgoing(r.localNodeID, kind) {
		if link.ToNodeID == nodeID && link.Established && link.PathClass == PathClassDirect {
			return buildRouteDecision(nodeID, snapshot.TopologyGeneration, plannerMeta{cost: link.CostMs + link.JitterMs, externalHops: 1, firstHopNode: nodeID, firstTransport: kind})
		}
	}
	return original
}
