package mesh

import (
	"context"
	"time"
)

type topologyFloodQueue struct {
	pending map[int64]*TopologyUpdate
	running bool
}

// 一个邻居共用一个邮箱，保留每个来源的最新公告。发送任务在网络 I/O
// 之外持有 Runtime.mu；备用连接不会收到重复广播，但主连接失败时可接替。
func (r *Runtime) queueTopologyFlood(conn TransportConn, update *TopologyUpdate) {
	r.mu.Lock()
	adj := r.adjByConn[conn]
	r.mu.Unlock()
	if adj != nil {
		r.queueTopologyFloodNode(adj.RemoteNodeID, update)
	}
}

func (r *Runtime) queueTopologyFloodNode(nodeID int64, update *TopologyUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || update == nil {
		return
	}
	if r.topologyFloods == nil {
		r.topologyFloods = make(map[int64]*topologyFloodQueue)
	}
	q := r.topologyFloods[nodeID]
	if q == nil {
		q = &topologyFloodQueue{pending: make(map[int64]*TopologyUpdate)}
		r.topologyFloods[nodeID] = q
	}
	if old := q.pending[update.OriginNodeId]; topologyUpdateNewer(update, old) {
		q.pending[update.OriginNodeId] = update
	}
	if q.running {
		return
	}
	q.running = true
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r.wg.Add(1)
	go r.drainTopologyFlood(ctx, nodeID, q)
}

func (r *Runtime) drainTopologyFlood(ctx context.Context, nodeID int64, q *topologyFloodQueue) {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		if r.closed || ctx.Err() != nil || len(q.pending) == 0 {
			delete(r.topologyFloods, nodeID)
			r.mu.Unlock()
			return
		}
		updates := q.pending
		q.pending = make(map[int64]*TopologyUpdate)
		r.mu.Unlock()
		for _, update := range updates {
			envelope := &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: update}}
			// 每个候选最多尝试一次。一次成功后停止，连接失败只影响本邻居。
			tried := make(map[TransportConn]bool)
			for ctx.Err() == nil {
				r.mu.Lock()
				adj := r.topologyAdjacencyLocked(nodeID, tried)
				closed := r.closed
				r.mu.Unlock()
				if closed || adj == nil {
					break
				}
				tried[adj.Conn] = true
				if r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout) == nil {
					break
				}
			}
		}
	}
}

// topologyAdjacencyLocked 按控制消息的实时成本选择连接，不改变有序数据流
// 的固定路径。相同分数优先 mTLS，然后按稳定的连接属性打破平局。
func (r *Runtime) topologyAdjacencyLocked(nodeID int64, excluded map[TransportConn]bool) *Adjacency {
	var best *Adjacency
	var bestScore int64
	for key, candidates := range r.adjByRoute {
		if key.nodeID != nodeID {
			continue
		}
		for _, adj := range candidates {
			if excluded[adj.Conn] {
				continue
			}
			adj.mu.Lock()
			established := adj.established
			score := r.adjacencyCostLocked(adj, time.Now()) + int64(adj.jitterEWMA)
			adj.mu.Unlock()
			if !established {
				continue
			}
			prefer := best == nil || score < bestScore
			if best != nil && score == bestScore {
				if (adj.Transport == TransportTCPMTLS) != (best.Transport == TransportTCPMTLS) {
					prefer = adj.Transport == TransportTCPMTLS
				} else if adj.Transport != best.Transport {
					prefer = adj.Transport < best.Transport
				} else if adj.RemoteHint != best.RemoteHint {
					prefer = adj.RemoteHint < best.RemoteHint
				} else if adj.Inbound != best.Inbound {
					prefer = !adj.Inbound
				}
			}
			if prefer {
				best, bestScore = adj, score
			}
		}
	}
	return best
}
