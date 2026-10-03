package mesh

import (
	"context"
	"fmt"
	"math"
	"time"
)

// 点对点流的负载感知选路。
//
// 每条邻接按 TCP_INFO 估计发送侧重传率与容量（近期实际达成吞吐的衰减最大值），量化后随链路公告传播；stream
// 在直连与"经一个邻居中转"的路径间选择：
//   - 轻负载：选 RTT + 抖动 + 丢包惩罚最低的路径（入境丢包重时常是中转）。
//   - 重负载：只在中转容量估计明显高于直连时才走中转，避免中转降低带宽。
//
// 已发送未确认的数据不能换路，否则接收端会因乱序丢帧；Data 换路先排空：
// 该流的新帧在 core 内按序排队（不阻塞客户端会话，同会话的反向 Ack 照常发送），
// 等旧路径数据全部确认后从新路径按序发出；超时或排队过多则放弃换路、发回旧路径。
// 全部确认时、以及只发累计确认的接收方，可立即换路。
const (
	streamPathEvalInterval   = time.Second
	streamPathDrainMaxBytes  = 16 << 20 // 排队超过此量放弃换路，回到旧路径发送
	streamPathSwitchBackoff  = 30 * time.Second
	streamHeavyEnterBps      = 512 << 10 // 约 4 Mbps 进入重负载
	streamHeavyExitBps       = 128 << 10 // 约 1 Mbps 退出重负载
	streamRateWindow         = 500 * time.Millisecond
	streamRateIdleReset      = 5 * time.Second
	streamLossPenaltyMillis  = 3  // 每千分之一重传率折算的延迟惩罚（毫秒）
	streamTransitExtraMillis = 10 // 中转的固定额外代价，平局时偏向直连
	streamHeavyTransitGain   = 1.5
	streamHeavyTransitKeep   = 1.1

	// 重负载中转探测：直连基线至少测 8 秒，探测 10 秒，中转需达到基线 1.2 倍才保留；
	// 结论有效 10 分钟，失败后 10 分钟内不再探测。
	streamProbeBaselineMin = 8 * time.Second
	streamProbeDuration    = 10 * time.Second
	streamProbeWinGain     = 1.2
	streamProbeValidFor    = 10 * time.Minute
	streamProbeRetryAfter  = 10 * time.Minute

	tcpQualityMinSegs        = 50
	tcpQualityEWMAWeight     = 0.3
	tcpCapacityDecay         = 0.995 // 每个采样周期（约 2 秒）的衰减，半衰期约 4.6 分钟
	tcpQualityPublishMin     = 10 * time.Second
	tcpLossAdvertiseStep     = 5
	tcpCapacityAdvertiseGain = 1.25
)

// streamPathDrainTimeout 是等待旧路径数据全部确认的上限；测试可缩短。
var streamPathDrainTimeout = 5 * time.Second

// streamDrain 是一次进行中的 Data 换路：旧路径数据全部确认后关闭 done，
// 期间该流的帧按序排在 queue 中。字段由 Runtime.mu 保护。
type streamDrain struct {
	to          *Adjacency
	done        chan struct{}
	signaled    bool
	epoch       uint64
	queue       []streamQueuedFrame
	queuedBytes int
	overflow    bool
}

type streamQueuedFrame struct {
	frame    *StreamFrame
	envelope *ClusterEnvelope
}

func (d *streamDrain) enqueue(frame *StreamFrame, envelope *ClusterEnvelope) {
	d.queue = append(d.queue, streamQueuedFrame{frame: frame, envelope: envelope})
	d.queuedBytes += len(frame.Payload)
	if d.queuedBytes > streamPathDrainMaxBytes && !d.signaled {
		d.overflow, d.signaled = true, true
		close(d.done)
	}
}

type streamPathCounters struct {
	switches    uint64
	drains      uint64
	drainAborts uint64
	probes      uint64
	probeWins   uint64
}

// streamProbe 是一次重负载中转探测：从直连 from 切到中转 to，与直连基线吞吐比较。
type streamProbe struct {
	from     *Adjacency
	to       *Adjacency
	baseline float64 // 直连上的确认吞吐（字节/秒）
}

// StreamPathSnapshot 描述一条 stream 当前的选路状态，供运维观测。
type StreamPathSnapshot struct {
	TargetNodeID int64
	StreamID     string
	Epoch        uint64
	ViaNodeID    int64 // 0 为直连；否则为中转节点
	Transport    TransportKind
	Forwarding   bool // 未固定邻接，交给转发引擎
	Heavy        bool
	RateBps      float64
	Switches     uint64
	Drains       uint64
	DrainAborts  uint64
	Draining     bool
	Probing      bool
	ProvenVia    int64 // 探测证实更快的中转节点（有效期内）
	Probes       uint64
	ProbeWins    uint64
}

// sampleAdjacencyTransport 读取邻接底层 TCP 统计并更新质量估计；公告值变化时安排拓扑公告。
func (r *Runtime) sampleAdjacencyTransport(adj *Adjacency) {
	provider, ok := adj.Conn.(TCPInfoProvider)
	if !ok {
		return
	}
	info, ok := provider.TCPInfo()
	if !ok {
		return
	}
	adj.mu.Lock()
	changed := adj.observeTCPInfoLocked(info, time.Now())
	adj.mu.Unlock()
	if changed {
		r.scheduleQualityPublish()
	}
}

// observeTCPInfoLocked requires adj.mu. 返回公告值是否需要更新。
func (adj *Adjacency) observeTCPInfoLocked(info TCPInfo, now time.Time) bool {
	if !adj.tcpSampled || info.SegsOut < adj.tcpSegsOut || info.TotalRetrans < adj.tcpRetrans {
		adj.tcpSegsOut, adj.tcpRetrans, adj.tcpSampled = info.SegsOut, info.TotalRetrans, true
	} else if segs := info.SegsOut - adj.tcpSegsOut; segs >= tcpQualityMinSegs {
		// 报文段太少时累积到下一次，空闲连接上的一两个重传不代表链路质量。
		sample := math.Min(1000, 1000*float64(info.TotalRetrans-adj.tcpRetrans)/float64(segs))
		adj.tcpLossPermille += tcpQualityEWMAWeight * (sample - adj.tcpLossPermille)
		adj.tcpSegsOut, adj.tcpRetrans = info.SegsOut, info.TotalRetrans
	}
	// 容量取实际达成吞吐（确认字节增量/间隔）的衰减最大值：只反映链路近期真正跑出的速度，
	// 空闲链路偏低而不会高估。BBR 在应用受限的连接上停留在启动阶段，pacing 速率会成倍高估，不能采用。
	if !adj.tcpSampledAt.IsZero() && info.BytesAcked >= adj.tcpBytesAcked {
		if dt := now.Sub(adj.tcpSampledAt).Seconds(); dt > 0.2 {
			sample := float64(info.BytesAcked-adj.tcpBytesAcked) / dt
			adj.tcpCapacityBps = math.Max(adj.tcpCapacityBps*tcpCapacityDecay, sample)
		}
	}
	adj.tcpBytesAcked, adj.tcpSampledAt = info.BytesAcked, now

	loss := uint32(math.Round(adj.tcpLossPermille/tcpLossAdvertiseStep)) * tcpLossAdvertiseStep
	capacity := uint32(math.Min(adj.tcpCapacityBps*8/1000, math.MaxUint32))
	if !adj.qualityAdvertised.IsZero() && now.Sub(adj.qualityAdvertised) < tcpQualityPublishMin {
		return false
	}
	capacityMoved := capacity > 0 && (adj.advertisedCapacity == 0 ||
		float64(capacity) > float64(adj.advertisedCapacity)*tcpCapacityAdvertiseGain ||
		float64(capacity)*tcpCapacityAdvertiseGain < float64(adj.advertisedCapacity))
	if loss == adj.advertisedLoss && !capacityMoved {
		return false
	}
	adj.advertisedLoss = loss
	if capacityMoved {
		adj.advertisedCapacity = capacity
	}
	adj.qualityAdvertised = now
	return true
}

// observeSend 更新 stream 的发送速率估计。
func (e *directStreamAffinityEntry) observeSend(n int, now time.Time) {
	if e.rateStart.IsZero() {
		e.rateStart = now
	}
	e.rateBytes += uint64(n)
	dt := now.Sub(e.rateStart)
	if dt < streamRateWindow {
		return
	}
	sample := float64(e.rateBytes) / dt.Seconds()
	if dt >= streamRateIdleReset {
		e.rateBps = sample
	} else {
		e.rateBps = 0.5*e.rateBps + 0.5*sample
	}
	e.rateBytes, e.rateStart = 0, now
}

// updateMode 按发送速率切换轻/重负载，带迟滞避免抖动。
func (e *directStreamAffinityEntry) updateMode(now time.Time) {
	rate := e.rateBps
	if !e.rateStart.IsZero() && now.Sub(e.rateStart) >= streamRateIdleReset {
		rate = 0 // 长时间无发送
	}
	if !e.heavy && rate >= streamHeavyEnterBps {
		e.heavy = true
	} else if e.heavy && rate < streamHeavyExitBps {
		e.heavy = false
	}
}

type streamPathCandidate struct {
	adj         *Adjacency
	transit     bool
	bestDirect  bool // 直连中按 TCP 优先与评分选出的首选
	stabilityMs int64
	capacityBps float64 // 0 表示未知
}

func streamLossPenalty(permille float64) int64 {
	return int64(permille * streamLossPenaltyMillis)
}

// combinedLossPermille 合成两段独立丢包。
func combinedLossPermille(a, b float64) float64 {
	return 1000 - (1000-a)*(1000-b)/1000
}

// streamPathCandidatesLocked requires r.mu. 返回到目标的直连候选（首选直连，以及仍可用的当前直连）
// 与经各邻居中转的候选；中转要求邻居允许 stream 中转，且其到目标的公告链路与首跳同一传输。
func (r *Runtime) streamPathCandidatesLocked(targetNodeID int64, direct, current *Adjacency, snapshot TopologySnapshot, now time.Time) []streamPathCandidate {
	candidates := make([]streamPathCandidate, 0, 4)
	addDirect := func(adj *Adjacency, best bool) {
		adj.mu.Lock()
		defer adj.mu.Unlock()
		if adj.established {
			candidates = append(candidates, streamPathCandidate{
				adj:         adj,
				bestDirect:  best,
				stabilityMs: r.directStreamScoreLocked(adj, now) + streamLossPenalty(adj.tcpLossPermille),
				capacityBps: adj.tcpCapacityBps,
			})
		}
	}
	if direct != nil {
		addDirect(direct, true)
	}
	if current != nil && current != direct && current.RemoteNodeID == targetNodeID {
		addDirect(current, direct == nil)
	}
	if r.planner == nil {
		return candidates
	}
	seen := make(map[int64]struct{}, len(r.adjByRoute))
	for key := range r.adjByRoute {
		via := key.nodeID
		if via == targetNodeID || via == r.localNodeID {
			continue
		}
		if _, ok := seen[via]; ok {
			continue
		}
		seen[via] = struct{}{}
		if !streamTransitAllowed(snapshot, via) {
			continue
		}
		first := r.bestDirectStreamAdjacencyLocked(via, TransportUnspecified)
		if first == nil {
			continue
		}
		link, ok := streamTransitLink(snapshot, via, targetNodeID, first.Transport)
		if !ok {
			continue
		}
		node, _ := snapshot.Node(via)
		first.mu.Lock()
		score := r.directStreamScoreLocked(first, now)
		loss := combinedLossPermille(first.tcpLossPermille, float64(link.LossPermille))
		capacity := 0.0
		if first.tcpCapacityBps > 0 && link.CapacityKbps > 0 {
			capacity = math.Min(first.tcpCapacityBps, float64(link.CapacityKbps)*1000/8)
		}
		first.mu.Unlock()
		candidates = append(candidates, streamPathCandidate{
			adj:     first,
			transit: true,
			stabilityMs: score + link.CostMs + link.JitterMs + streamLossPenalty(loss) +
				transitPenalty(node.ForwardingPolicy, TrafficPointToPointStream) + streamTransitExtraMillis,
			capacityBps: capacity,
		})
	}
	return candidates
}

// streamTransitAllowed 与规划器对远端中转节点的判断一致：开启转发且不拒绝 stream 流量。
func streamTransitAllowed(snapshot TopologySnapshot, via int64) bool {
	node, ok := snapshot.Node(via)
	if !ok || node.ForwardingPolicy == nil || !node.ForwardingPolicy.TransitEnabled {
		return false
	}
	return DispositionForTraffic(node.ForwardingPolicy, TrafficPointToPointStream) != DispositionDeny
}

// streamTransitLink 查找中转节点 via 到目标、与首跳同一传输的已建立公告链路。
// 中转节点不桥接传输，入站与出站必须是同一传输。
func streamTransitLink(snapshot TopologySnapshot, via, targetNodeID int64, transport TransportKind) (LinkState, bool) {
	var best LinkState
	found := false
	for _, link := range snapshot.outgoing(via, transport) {
		if link.ToNodeID != targetNodeID || !link.Established || link.OriginNodeID != via {
			continue
		}
		if !found || link.CostMs+link.JitterMs < best.CostMs+best.JitterMs {
			best, found = link, true
		}
	}
	return best, found
}

// preferredStreamPathLocked 按负载模式在候选中选路，返回 nil 表示没有候选（保持当前）。
func preferredStreamPathLocked(candidates []streamPathCandidate, current *Adjacency, heavy bool) *Adjacency {
	if len(candidates) == 0 {
		return nil
	}
	if heavy {
		var cur, direct, bestTransit *streamPathCandidate
		for i := range candidates {
			c := &candidates[i]
			if c.adj == current {
				cur = c
			}
			if c.bestDirect {
				direct = c
			}
			if c.transit && c.capacityBps > 0 && (bestTransit == nil || c.capacityBps > bestTransit.capacityBps) {
				bestTransit = c
			}
		}
		if choice := heavyStreamTransit(cur, direct, bestTransit); choice != nil {
			return choice.adj
		}
		// 重负载走直连：在直连候选之间仍按稳定性与换路余量选择。
		directOnly := make([]streamPathCandidate, 0, len(candidates))
		for _, c := range candidates {
			if !c.transit {
				directOnly = append(directOnly, c)
			}
		}
		if len(directOnly) > 0 {
			return stablestStreamPath(directOnly, current)
		}
	}
	return stablestStreamPath(candidates, current)
}

// stablestStreamPath 选稳定性代价最低的候选；当前路径只在被明显超过时才更换。
func stablestStreamPath(candidates []streamPathCandidate, current *Adjacency) *Adjacency {
	best := &candidates[0]
	var cur *streamPathCandidate
	for i := range candidates {
		c := &candidates[i]
		if c.adj == current {
			cur = c
		}
		if c.stabilityMs < best.stabilityMs {
			best = c
		}
	}
	if cur == nil {
		return best.adj
	}
	margin := int64(directStreamReselectMarginMillis)
	if cur.stabilityMs/5 > margin {
		margin = cur.stabilityMs / 5
	}
	if best != cur && best.stabilityMs+margin < cur.stabilityMs {
		return best.adj
	}
	return cur.adj
}

// heavyStreamTransit 返回重负载应走的中转候选，nil 表示走直连。只在中转容量估计明显
// 高于直连时走中转；直连容量未知时留在直连，保证重负载不会因中转降低带宽。
func heavyStreamTransit(cur, direct, bestTransit *streamPathCandidate) *streamPathCandidate {
	if direct == nil {
		if bestTransit != nil && cur != nil && cur.transit && cur.capacityBps*streamHeavyTransitKeep >= bestTransit.capacityBps {
			return cur
		}
		return bestTransit
	}
	if bestTransit == nil || direct.capacityBps <= 0 {
		return nil
	}
	if cur != nil && cur.transit && cur.capacityBps >= direct.capacityBps*streamHeavyTransitKeep {
		if bestTransit != cur && bestTransit.capacityBps >= cur.capacityBps*tcpCapacityAdvertiseGain {
			return bestTransit
		}
		return cur
	}
	if bestTransit.capacityBps >= direct.capacityBps*streamHeavyTransitGain {
		return bestTransit
	}
	return nil
}

// resetWindow 在换路或进入重负载时重新开始当前路径的确认吞吐测量。
func (e *directStreamAffinityEntry) resetWindow(now time.Time) {
	e.windowStart, e.windowAcked = now, e.ackedEnd
}

// windowRate 返回当前路径测量窗口内的确认吞吐与窗口时长。
func (e *directStreamAffinityEntry) windowRate(now time.Time) (float64, time.Duration) {
	if e.windowStart.IsZero() || e.ackedEnd < e.windowAcked {
		return 0, 0
	}
	dt := now.Sub(e.windowStart)
	if dt <= 0 {
		return 0, 0
	}
	return float64(e.ackedEnd-e.windowAcked) / dt.Seconds(), dt
}

// streamProbeDecisionLocked requires r.mu. 重负载中转探测状态机：返回 handled=true 时
// desired 即本次选路结果（可能为当前路径），否则交给常规规则。targetNodeID 用于区分直连与中转。
func streamProbeDecisionLocked(e *directStreamAffinityEntry, candidates []streamPathCandidate, targetNodeID int64, now time.Time) (*Adjacency, bool) {
	if !e.heavy {
		e.probe = nil
		return nil, false
	}
	cur := e.adj
	onDirect := cur != nil && cur.RemoteNodeID == targetNodeID
	if p := e.probe; p != nil {
		if cur != p.to {
			// 换路尚未完成（或被放弃），仍在直连上：放弃本次探测。
			if onDirect {
				e.probe = nil
				e.nextProbe = now.Add(streamProbeRetryAfter)
				return nil, false
			}
			return cur, true
		}
		rate, dt := e.windowRate(now)
		if dt < streamProbeDuration {
			return cur, true
		}
		e.probe = nil
		if rate >= p.baseline*streamProbeWinGain {
			e.stats.probeWins++
			e.provenVia, e.provenUntil = p.to, now.Add(streamProbeValidFor)
			return cur, true
		}
		e.nextProbe = now.Add(streamProbeRetryAfter)
		return p.from, true
	}
	if e.provenVia != nil && now.Before(e.provenUntil) && cur == e.provenVia {
		return cur, true
	}
	if !onDirect || now.Before(e.nextProbe) {
		return nil, false
	}
	baseline, dt := e.windowRate(now)
	if dt < streamProbeBaselineMin {
		return nil, false
	}
	var direct, best *streamPathCandidate
	for i := range candidates {
		c := &candidates[i]
		if c.adj == cur {
			direct = c
		}
		if c.transit && (best == nil || c.stabilityMs < best.stabilityMs) {
			best = c
		}
	}
	if direct == nil || best == nil {
		return nil, false
	}
	// 只在中转明显更稳定（直连丢包或抖动重）时探测，避免在相近路径间来回试。
	margin := int64(directStreamReselectMarginMillis)
	if direct.stabilityMs/5 > margin {
		margin = direct.stabilityMs / 5
	}
	if best.stabilityMs+margin >= direct.stabilityMs {
		return nil, false
	}
	e.probe = &streamProbe{from: cur, to: best.adj, baseline: baseline}
	e.stats.probes++
	return best.adj, true
}

// streamHopUsableLocked requires r.mu. 直连要求邻接已建立；中转还要求中转节点到目标的公告链路仍在。
func streamHopUsableLocked(adj *Adjacency, targetNodeID int64, snapshot TopologySnapshot) bool {
	adj.mu.Lock()
	established := adj.established
	adj.mu.Unlock()
	if !established {
		return false
	}
	if adj.RemoteNodeID == targetNodeID {
		return true
	}
	_, ok := streamTransitLink(snapshot, adj.RemoteNodeID, targetNodeID, adj.Transport)
	return ok
}

// startStreamDrainLocked requires r.mu. 启动排空协程。
func (r *Runtime) startStreamDrainLocked(key directStreamAffinityKey, drain *streamDrain) {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r.wg.Add(1)
	go r.runStreamDrain(ctx, key, drain)
}

// runStreamDrain 等待旧路径数据全部确认（或超时、溢出），决定换路或放弃，然后按序发出排队的帧。
func (r *Runtime) runStreamDrain(ctx context.Context, key directStreamAffinityKey, drain *streamDrain) {
	defer r.wg.Done()
	timer := time.NewTimer(streamPathDrainTimeout)
	select {
	case <-drain.done:
	case <-timer.C:
	case <-ctx.Done():
	}
	timer.Stop()

	snapshot := r.store.Snapshot()
	snapshot.ensureOutgoingLinks()
	r.mu.Lock()
	entry, ok := r.directStreamAffinity[key]
	if r.closed || !ok || entry.drain != drain {
		// 流已关闭或被新 epoch 取代，排队的旧帧由端点的恢复流程重传。
		r.mu.Unlock()
		return
	}
	now := time.Now()
	if entry.quiescent() && !drain.overflow && streamHopUsableLocked(drain.to, key.targetNodeID, snapshot) {
		entry.adj = drain.to
		entry.stats.switches++
		entry.stats.drains++
		entry.resetWindow(now)
	} else {
		entry.stats.drainAborts++
		entry.backoffUntil = now.Add(streamPathSwitchBackoff)
		if entry.probe != nil && entry.probe.to == drain.to {
			entry.probe = nil
			entry.nextProbe = now.Add(streamProbeRetryAfter)
		}
	}
	adj := entry.adj
	r.directStreamAffinity[key] = entry
	r.mu.Unlock()

	for {
		r.mu.Lock()
		entry, ok := r.directStreamAffinity[key]
		if r.closed || !ok || entry.drain != drain {
			r.mu.Unlock()
			return
		}
		if len(drain.queue) == 0 {
			entry.drain = nil
			r.directStreamAffinity[key] = entry
			r.mu.Unlock()
			return
		}
		item := drain.queue[0]
		drain.queue[0] = streamQueuedFrame{}
		drain.queue = drain.queue[1:]
		drain.queuedBytes -= len(item.frame.Payload)
		recordDirectStreamSend(&entry, item.frame)
		r.directStreamAffinity[key] = entry
		r.mu.Unlock()

		sendCtx, cancel := context.WithTimeout(ctx, r.helloTimeout)
		err := r.sendStreamFrameOn(sendCtx, adj, key.targetNodeID, item.envelope)
		cancel()
		if item.frame.Kind == streamFrameKindClose {
			r.clearDirectStreamAffinity(key, item.frame.Epoch)
			return
		}
		if err != nil {
			// 发送失败时丢弃剩余排队帧，端点按停滞检测恢复。
			r.mu.Lock()
			if entry, ok := r.directStreamAffinity[key]; ok && entry.drain == drain {
				entry.drain = nil
				r.directStreamAffinity[key] = entry
			}
			r.mu.Unlock()
			return
		}
	}
}

// sendStreamFrameOn 按首跳发送 stream 信封：直连邻接直接发送，中转邻居包装为转发包，
// 没有固定邻接时交给转发引擎。
func (r *Runtime) sendStreamFrameOn(ctx context.Context, adj *Adjacency, targetNodeID int64, envelope *ClusterEnvelope) error {
	if adj == nil {
		return r.forwardEnvelope(ctx, targetNodeID, TrafficPointToPointStream, envelope)
	}
	if adj.RemoteNodeID != targetNodeID {
		return r.sendStreamViaTransit(ctx, adj, targetNodeID, envelope)
	}
	return r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout)
}

// signalStreamDrainLocked requires r.mu. 排空中的流全部确认后唤醒等待的发送方。
func signalStreamDrainLocked(entry *directStreamAffinityEntry) {
	if entry.drain != nil && !entry.drain.signaled && entry.quiescent() {
		entry.drain.signaled = true
		close(entry.drain.done)
	}
}

// sendStreamViaTransit 把 stream 信封包装为 ForwardedPacket 交给中转邻居，由其转发到目标。
func (r *Runtime) sendStreamViaTransit(ctx context.Context, adj *Adjacency, targetNodeID int64, envelope *ClusterEnvelope) error {
	payload, err := r.codec.Encode(envelope)
	if err != nil {
		return err
	}
	packet := &ForwardedPacket{
		PacketId:           r.packetID.Add(1),
		SourceNodeId:       r.localNodeID,
		SourceRuntimeEpoch: r.localRuntimeEpoch,
		TargetNodeId:       targetNodeID,
		TrafficClass:       TrafficPointToPointStream,
		LastHopNodeId:      r.localNodeID,
		IngressTransport:   adj.Transport,
		TtlHops:            DefaultTTLHops - 1,
		Payload:            payload,
	}
	// 与引擎出站一致先标记已见，绕回本节点的副本会被去重丢弃。
	if !r.engine.markSeen(packet) {
		return fmt.Errorf("mesh: duplicate transit stream packet: %w", ErrDuplicatePacket)
	}
	return r.sendEnvelopeCtx(ctx, adj.Conn, &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: packet}}, r.helloTimeout)
}

// StreamPaths 返回各 stream 的选路状态。
func (r *Runtime) StreamPaths() []StreamPathSnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StreamPathSnapshot, 0, len(r.directStreamAffinity))
	now := time.Now()
	for key, entry := range r.directStreamAffinity {
		item := StreamPathSnapshot{
			TargetNodeID: key.targetNodeID,
			StreamID:     fmt.Sprintf("%x", key.streamID),
			Epoch:        entry.epoch,
			Forwarding:   entry.adj == nil,
			Heavy:        entry.heavy,
			RateBps:      entry.rateBps,
			Switches:     entry.stats.switches,
			Drains:       entry.stats.drains,
			DrainAborts:  entry.stats.drainAborts,
			Draining:     entry.drain != nil,
			Probing:      entry.probe != nil,
			Probes:       entry.stats.probes,
			ProbeWins:    entry.stats.probeWins,
		}
		if entry.provenVia != nil && now.Before(entry.provenUntil) {
			item.ProvenVia = entry.provenVia.RemoteNodeID
		}
		if !entry.rateStart.IsZero() && now.Sub(entry.rateStart) >= streamRateIdleReset {
			item.RateBps = 0
		}
		if entry.adj != nil {
			item.Transport = entry.adj.Transport
			if entry.adj.RemoteNodeID != key.targetNodeID {
				item.ViaNodeID = entry.adj.RemoteNodeID
			}
		}
		out = append(out, item)
	}
	return out
}
