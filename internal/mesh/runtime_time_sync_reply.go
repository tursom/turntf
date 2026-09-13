package mesh

import "context"

// queueTimeSyncReply keeps time-sync output off the ingress reader. The peer
// tracks only its current probe, so a slow connection needs at most one newest
// pending reply, rather than accumulating replies for obsolete request IDs.
func (r *Runtime) queueTimeSyncReply(adj *Adjacency, reply *TimeSyncResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.adjByConn[adj.Conn] != adj {
		return
	}
	adj.pendingTimeSyncReply = reply
	if adj.timeSyncReplying {
		return
	}
	adj.timeSyncReplying = true
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			r.mu.Lock()
			if r.closed || ctx.Err() != nil || r.adjByConn[adj.Conn] != adj || adj.pendingTimeSyncReply == nil {
				adj.pendingTimeSyncReply = nil
				adj.timeSyncReplying = false
				r.mu.Unlock()
				return
			}
			current := adj.pendingTimeSyncReply
			adj.pendingTimeSyncReply = nil
			r.mu.Unlock()
			// Preserve receive time captured by the reader; include local queue delay
			// as server processing, not as network clock offset.
			current.ServerSendTimeMs = r.now().UnixMilli()
			if current.ServerSendTimeMs < current.ServerReceiveTimeMs {
				current.ServerSendTimeMs = current.ServerReceiveTimeMs
			}
			_ = r.sendEnvelopeCtx(ctx, adj.Conn, &ClusterEnvelope{Body: &ClusterEnvelope_TimeSyncResponse{TimeSyncResponse: current}}, r.helloTimeout)
		}
	}()
}
