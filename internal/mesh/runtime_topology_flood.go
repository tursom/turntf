package mesh

import "context"

// queueTopologyFlood hands immutable normalized advertisements to one writer
// per adjacency. Only the newest pending version of each origin is retained;
// congestion cannot accumulate an unbounded history of periodic updates.
// Runtime.mu protects the mailbox and worker lifecycle, including wg.Add vs Close.
func (r *Runtime) queueTopologyFlood(conn TransportConn, update *TopologyUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	adj := r.adjByConn[conn]
	if r.closed || adj == nil {
		return
	}
	if adj.pendingTopology == nil {
		adj.pendingTopology = make(map[int64]*TopologyUpdate)
	}
	if old := adj.pendingTopology[update.OriginNodeId]; old == nil || old.Generation < update.Generation {
		adj.pendingTopology[update.OriginNodeId] = update
	}
	if adj.topologySending {
		return
	}
	adj.topologySending = true
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r.wg.Add(1)
	go r.drainTopologyFlood(ctx, adj)
}

func (r *Runtime) drainTopologyFlood(ctx context.Context, adj *Adjacency) {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		if r.closed || ctx.Err() != nil || r.adjByConn[adj.Conn] != adj || len(adj.pendingTopology) == 0 {
			adj.pendingTopology = nil
			adj.topologySending = false
			r.mu.Unlock()
			return
		}
		// Take a bounded round so an origin publishing frequently cannot starve
		// other origins. Updates arriving during I/O are coalesced into the next round.
		updates := adj.pendingTopology
		adj.pendingTopology = make(map[int64]*TopologyUpdate)
		r.mu.Unlock()
		for _, update := range updates {
			r.mu.Lock()
			stopped := r.closed || r.adjByConn[adj.Conn] != adj
			r.mu.Unlock()
			if stopped || ctx.Err() != nil {
				break
			}
			envelope := &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: update}}
			_ = r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout)
		}
	}
}
