package mesh

import "time"

func measurementEnvelope(envelope *ClusterEnvelope) bool {
	return envelope.GetTimeSyncRequest() != nil || envelope.GetTimeSyncResponse() != nil ||
		envelope.GetTopologyUpdate() != nil || envelope.GetNodeHello() != nil
}

// 只降低稳定空闲备用连接的测量发送频率；检查循环仍按 pingInterval
// 执行。主连接失效、备用承载业务或探测退化后，下一次检查恢复原频率。
func (r *Runtime) standbyProbeInterval(adj *Adjacency, now time.Time) time.Duration {
	interval := min(4*r.pingInterval, r.livenessTimeout/3)
	if interval <= r.pingInterval {
		return r.pingInterval
	}
	r.mu.Lock()
	primary := r.topologyAdjacencyLocked(adj.RemoteNodeID, nil)
	if primary == nil || primary == adj {
		r.mu.Unlock()
		return r.pingInterval
	}
	primary.mu.Lock()
	healthy := primary.samples >= 8 && primary.sendFailures == 0 && !primary.probeDegraded && len(primary.inflightPings) <= 1
	primary.mu.Unlock()
	r.mu.Unlock()
	adj.mu.Lock()
	stable := adj.samples >= 8 && adj.sendFailures == 0 && !adj.probeDegraded && adj.jitterEWMA <= max(50, adj.rttEWMA/4)
	adj.mu.Unlock()
	activity := adj.lastBusinessActivity.Load()
	if !healthy || !stable || (activity != 0 && now.UnixNano()-activity < int64(interval)) {
		return r.pingInterval
	}
	return interval
}
