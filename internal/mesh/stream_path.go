package mesh

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// Direct stream path selection.
//
// A logical stream epoch is pinned to one physical adjacency because the SDK
// receiver drops Data that arrives out of order. The pin may still move:
//
//   - When every ordered frame this node sent (Open, Resume, Data) has been
//     acknowledged, the next frame may start on another adjacency directly.
//   - While Data is in flight, a move drains first: new Data is held here until
//     the remote endpoint acknowledges everything sent on the old adjacency,
//     then the held frames are written in order on the new one. A drain that
//     does not finish in time flushes the held frames on the old adjacency.
//
// Paths are compared by measured goodput: while a stream keeps at least
// streamBusyBytes in flight, its cumulative acknowledgements are sampled and
// recorded on the adjacency as a windowed maximum delivery rate. Ping RTT and
// jitter cannot rank paths reliably: a loaded adjacency's pings queue behind
// its own data, and congestion loss on a low-latency path only appears under
// load. Without fresh measurements the RTT+jitter score picks the path. A busy
// stream on a measured adjacency periodically probes an unmeasured alternative
// so that each path's goodput stays known.
const (
	// A quiescent stream moves by RTT+jitter only when the other path is
	// cheaper by this margin (or a fifth of the current cost, if larger).
	directStreamReselectMarginMillis = 25

	streamBusyBytes             = 128 << 10
	streamGoodputSampleInterval = 500 * time.Millisecond
	streamGoodputWindow         = 10 * time.Second
	// Open and Resume may use estimates up to this old.
	streamGoodputStaleAfter = 90 * time.Second
	// Moving a stream that has Data in flight needs fresher evidence: a path
	// the stream left keeps its old peak, which must not pull it back.
	streamGoodputFreshFor = 30 * time.Second
	// A measured path replaces the current one only when it delivers this
	// much more, so similar paths do not trade a stream back and forth.
	streamSwitchGain       = 1.3
	streamDecisionInterval = time.Second
	// After a move the stream stays put long enough to measure the new path;
	// each failed drain doubles the wait up to streamSwitchBackoffMax.
	streamSwitchHold       = 10 * time.Second
	streamSwitchBackoffMax = 5 * time.Minute
	streamProbeInterval    = 5 * time.Minute
	// A slow old path needs time to deliver up to a full stream window.
	streamDrainTimeout = 10 * time.Second
	// Held Data is bounded by the sender's stream window in practice; this
	// cap only guards against a misbehaving client.
	streamQueueLimitBytes = 8 << 20
)

type directStreamAffinityEntry struct {
	epoch uint64
	// A nil adjacency pins this epoch to the forwarding path selected by Open.
	adj *Adjacency
	// Ordered frames this node sent for the epoch and the remote endpoint's
	// acknowledgements of them. Only Open, Resume and Data need ordering;
	// OpenAck and cumulative Ack tolerate reordering.
	awaitOpenAck bool
	awaitResume  bool
	sentEnd      uint64
	ackedEnd     uint64

	// Goodput sample on sampleAdj, started while the stream was busy.
	sampleAdj   *Adjacency
	sampleStart time.Time
	sampleAcked uint64

	lastDecision time.Time
	lastSwitch   time.Time
	// holdFor is the minimum stay since lastSwitch; failed drains double it.
	holdFor time.Duration
	// draining holds new Data until the old path is acknowledged; flushing
	// writes held frames in order. Either way later Data joins the queue.
	draining   bool
	flushing   bool
	closing    bool // Close arrived while frames were held
	switchTo   *Adjacency
	switchSeq  uint64
	queue      []*ClusterEnvelope
	queueBytes int
}

func (e *directStreamAffinityEntry) quiescent() bool {
	return !e.awaitOpenAck && !e.awaitResume && e.ackedEnd >= e.sentEnd
}

func (e *directStreamAffinityEntry) busy() bool {
	return e.sentEnd-e.ackedEnd >= streamBusyBytes
}

func (e *directStreamAffinityEntry) holding() bool {
	return e.draining || e.flushing
}

// observeGoodputLocked records a delivery rate sample (bytes/s) in a windowed
// maximum over one to two windows. The caller holds adj.mu.
func (adj *Adjacency) observeGoodputLocked(rate float64, now time.Time) {
	if adj.goodputCurStart.IsZero() || now.Sub(adj.goodputCurStart) >= streamGoodputWindow {
		adj.goodputPrevOK = !adj.goodputCurStart.IsZero() && now.Sub(adj.goodputCurStart) < 2*streamGoodputWindow
		adj.goodputPrev = adj.goodputCur
		adj.goodputCur = rate
		adj.goodputCurStart = now
	} else if rate > adj.goodputCur {
		adj.goodputCur = rate
	}
	adj.goodputLast = now
}

// goodputLocked returns the measured goodput, or false without a recent
// sample. The caller holds adj.mu.
func (adj *Adjacency) goodputLocked(now time.Time) (float64, bool) {
	return adj.goodputWithinLocked(now, streamGoodputStaleAfter)
}

// goodputWithinLocked is goodputLocked with a caller-chosen maximum age.
func (adj *Adjacency) goodputWithinLocked(now time.Time, maxAge time.Duration) (float64, bool) {
	if adj.goodputLast.IsZero() || now.Sub(adj.goodputLast) > maxAge {
		return 0, false
	}
	rate := adj.goodputCur
	if adj.goodputPrevOK && now.Sub(adj.goodputCurStart) < streamGoodputWindow && adj.goodputPrev > rate {
		rate = adj.goodputPrev
	}
	return rate, true
}

func adjacencyGoodput(adj *Adjacency, now time.Time) (float64, bool) {
	return adjacencyGoodputWithin(adj, now, streamGoodputStaleAfter)
}

func adjacencyGoodputWithin(adj *Adjacency, now time.Time, maxAge time.Duration) (float64, bool) {
	adj.mu.Lock()
	defer adj.mu.Unlock()
	return adj.goodputWithinLocked(now, maxAge)
}

// directStreamAdjacency returns the adjacency for this stream frame. queued
// reports that the frame is held behind a path drain and will be sent later.
// Resume and its Ack response may advance an existing affinity; all other
// frames must match the pinned epoch exactly.
func (r *Runtime) directStreamAdjacency(key directStreamAffinityKey, frame *StreamFrame, envelope *ClusterEnvelope) (*Adjacency, bool, error) {
	var decision RouteDecision
	var directRoute bool
	if r.planner != nil {
		decision, directRoute = r.planner.Compute(r.store.Snapshot(), key.targetNodeID, TrafficPointToPointStream, TransportUnspecified)
		directRoute = directRoute && decision.NextHopNodeID == key.targetNodeID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, ErrRuntimeClosed
	}
	now := r.pathClock()

	entry := r.directStreamAffinity[key]
	canAdvanceEpoch := frame.Kind == streamFrameKindResume || frame.Kind == streamFrameKindAck
	if entry != nil && canAdvanceEpoch && frame.Epoch > entry.epoch {
		// A stalled path delivers nothing; record that before the new epoch
		// forgets it, so the replacement does not pick the same path.
		r.sampleDirectStreamLocked(entry, now)
		delete(r.directStreamAffinity, key)
		entry = nil
	}
	if entry != nil {
		if entry.epoch != frame.Epoch {
			return nil, false, fmt.Errorf("mesh: direct stream path epoch %d does not match affinity epoch %d: %w", frame.Epoch, entry.epoch, ErrNoRoute)
		}
		ordered := frame.Kind == streamFrameKindData || frame.Kind == streamFrameKindClose
		if entry.holding() && ordered {
			r.holdDirectStreamFrameLocked(key, entry, envelope, frame)
			return nil, true, nil
		}
		if entry.adj != nil {
			entry.adj.mu.Lock()
			established := entry.adj.established
			entry.adj.mu.Unlock()
			if !established {
				return nil, false, fmt.Errorf("mesh: direct stream affinity is no longer established: %w", ErrNoRoute)
			}
		}
		mayReselect := frame.Kind == streamFrameKindData || frame.Kind == streamFrameKindAck || frame.Kind == streamFrameKindOpenAck
		if mayReselect && entry.quiescent() {
			if better := r.betterDirectStreamAdjacencyLocked(entry.adj, key.targetNodeID, decision, directRoute, now); better != nil {
				r.moveDirectStreamLocked(entry, better, now)
				r.streamPathStats.quiescentMoves.Add(1)
			}
		} else if frame.Kind == streamFrameKindData && entry.adj != nil {
			if target := r.directStreamSwitchTargetLocked(key.targetNodeID, entry, decision, directRoute, now); target != nil {
				r.streamPathStats.drainsStarted.Add(1)
				entry.draining = true
				entry.switchTo = target
				entry.switchSeq++
				seq, epoch := entry.switchSeq, entry.epoch
				time.AfterFunc(r.streamDrainTimeout, func() { r.abortDirectStreamDrain(key, epoch, seq) })
				r.holdDirectStreamFrameLocked(key, entry, envelope, frame)
				return nil, true, nil
			}
		}
		recordDirectStreamSend(entry, frame)
		return entry.adj, false, nil
	}

	if len(r.directStreamAffinity) >= directStreamAffinityLimit {
		return nil, false, fmt.Errorf("mesh: direct stream affinity capacity reached: %w", ErrNoRoute)
	}
	entry = &directStreamAffinityEntry{epoch: frame.Epoch, adj: r.initialDirectStreamAdjacencyLocked(key.targetNodeID, decision, directRoute, now), lastSwitch: now}
	if frame.Kind == streamFrameKindResume {
		// Data after Resume restarts at the resumed offset; earlier bytes are
		// either acknowledged or retransmitted in this epoch.
		entry.sentEnd, entry.ackedEnd = frame.Offset, frame.Offset
	}
	recordDirectStreamSend(entry, frame)
	r.directStreamAffinity[key] = entry
	return entry.adj, false, nil
}

// directStreamCandidatesLocked requires r.mu. A stream may use the planner's
// transport when it routes directly; otherwise TCP adjacencies keep priority.
func (r *Runtime) directStreamCandidatesLocked(targetNodeID int64, decision RouteDecision, directRoute bool) []*Adjacency {
	if directRoute && decision.OutboundTransport == TransportUnspecified {
		return nil
	}
	var all, tcp []*Adjacency
	for key, candidates := range r.adjByRoute {
		if key.nodeID != targetNodeID || (directRoute && key.transport != decision.OutboundTransport) {
			continue
		}
		for _, adj := range candidates {
			if adj == nil {
				continue
			}
			adj.mu.Lock()
			established := adj.established
			adj.mu.Unlock()
			if !established {
				continue
			}
			all = append(all, adj)
			if adj.Transport == TransportTCPMTLS {
				tcp = append(tcp, adj)
			}
		}
	}
	if !directRoute && len(tcp) > 0 {
		return tcp
	}
	return all
}

// selectDirectStreamAdjacencyLocked requires r.mu. It ranks by RTT+jitter; a
// nil result keeps the stream on the forwarding path.
func (r *Runtime) selectDirectStreamAdjacencyLocked(targetNodeID int64, decision RouteDecision, directRoute bool) *Adjacency {
	if directRoute {
		return r.bestAdjacencyLocked(targetNodeID, decision.OutboundTransport)
	}
	// A directly registered adjacency may precede its topology snapshot
	// during startup. Preserve the established direct path in that window.
	return r.bestDirectAdjacencyLocked(targetNodeID)
}

// initialDirectStreamAdjacencyLocked requires r.mu. Open and Resume take the
// best measured path when one exists, otherwise the RTT+jitter choice.
func (r *Runtime) initialDirectStreamAdjacencyLocked(targetNodeID int64, decision RouteDecision, directRoute bool, now time.Time) *Adjacency {
	var best *Adjacency
	var bestRate float64
	for _, adj := range r.directStreamCandidatesLocked(targetNodeID, decision, directRoute) {
		if rate, ok := adjacencyGoodput(adj, now); ok && (best == nil || rate > bestRate) {
			best, bestRate = adj, rate
		}
	}
	if best != nil {
		return best
	}
	return r.selectDirectStreamAdjacencyLocked(targetNodeID, decision, directRoute)
}

// measuredBetterDirectStreamAdjacencyLocked requires r.mu. It returns the
// candidate whose measured goodput beats the measured current path by
// streamSwitchGain, or nil.
func (r *Runtime) measuredBetterDirectStreamAdjacencyLocked(current *Adjacency, candidates []*Adjacency, now time.Time, maxAge time.Duration) *Adjacency {
	currentRate, ok := adjacencyGoodputWithin(current, now, maxAge)
	if !ok {
		return nil
	}
	var best *Adjacency
	bestRate := currentRate * streamSwitchGain
	for _, candidate := range candidates {
		if candidate == current {
			continue
		}
		if rate, ok := adjacencyGoodputWithin(candidate, now, maxAge); ok && rate > bestRate {
			best, bestRate = candidate, rate
		}
	}
	return best
}

// betterDirectStreamAdjacencyLocked requires r.mu and picks a new path for a
// quiescent stream: by measured goodput when the current path is measured,
// otherwise by RTT+jitter. A forwarding pin moves to any direct adjacency.
func (r *Runtime) betterDirectStreamAdjacencyLocked(current *Adjacency, targetNodeID int64, decision RouteDecision, directRoute bool, now time.Time) *Adjacency {
	if current == nil {
		return r.initialDirectStreamAdjacencyLocked(targetNodeID, decision, directRoute, now)
	}
	if _, measured := adjacencyGoodput(current, now); measured {
		return r.measuredBetterDirectStreamAdjacencyLocked(current, r.directStreamCandidatesLocked(targetNodeID, decision, directRoute), now, streamGoodputStaleAfter)
	}
	candidate := r.selectDirectStreamAdjacencyLocked(targetNodeID, decision, directRoute)
	if candidate == nil || candidate == current {
		return nil
	}
	wall := time.Now()
	current.mu.Lock()
	currentScore := r.adjacencyCostLocked(current, wall) + int64(current.jitterEWMA)
	current.mu.Unlock()
	candidate.mu.Lock()
	candidateScore := r.adjacencyCostLocked(candidate, wall) + int64(candidate.jitterEWMA)
	candidate.mu.Unlock()
	margin := int64(directStreamReselectMarginMillis)
	if currentScore/5 > margin {
		margin = currentScore / 5
	}
	if candidateScore+margin >= currentScore {
		return nil
	}
	return candidate
}

// directStreamSwitchTargetLocked requires r.mu. A stream with Data in flight
// moves only on measured goodput, or to probe an unmeasured alternative while
// it is busy enough to measure it; RTT+jitter alone never forces a drain.
func (r *Runtime) directStreamSwitchTargetLocked(targetNodeID int64, entry *directStreamAffinityEntry, decision RouteDecision, directRoute bool, now time.Time) *Adjacency {
	if now.Sub(entry.lastDecision) < streamDecisionInterval || now.Sub(entry.lastSwitch) < entry.hold() {
		return nil
	}
	entry.lastDecision = now
	r.sampleDirectStreamLocked(entry, now)
	candidates := r.directStreamCandidatesLocked(targetNodeID, decision, directRoute)
	if target := r.measuredBetterDirectStreamAdjacencyLocked(entry.adj, candidates, now, streamGoodputFreshFor); target != nil {
		return target
	}
	if !entry.busy() {
		return nil
	}
	if _, ok := adjacencyGoodputWithin(entry.adj, now, streamGoodputFreshFor); !ok {
		return nil
	}
	if last, ok := r.streamProbeAt[targetNodeID]; ok && now.Sub(last) < streamProbeInterval {
		return nil
	}
	var probe *Adjacency
	var probeCost int64
	wall := time.Now()
	for _, candidate := range candidates {
		if candidate == entry.adj {
			continue
		}
		if _, measured := adjacencyGoodputWithin(candidate, now, streamGoodputFreshFor); measured {
			continue
		}
		candidate.mu.Lock()
		cost := r.adjacencyCostLocked(candidate, wall) + int64(candidate.jitterEWMA)
		candidate.mu.Unlock()
		if probe == nil || cost < probeCost {
			probe, probeCost = candidate, cost
		}
	}
	if probe != nil {
		r.streamProbeAt[targetNodeID] = now
		r.streamPathStats.probes.Add(1)
	}
	return probe
}

func (e *directStreamAffinityEntry) hold() time.Duration {
	if e.holdFor < streamSwitchHold {
		return streamSwitchHold
	}
	return e.holdFor
}

// moveDirectStreamLocked requires r.mu.
func (r *Runtime) moveDirectStreamLocked(entry *directStreamAffinityEntry, adj *Adjacency, now time.Time) {
	entry.adj = adj
	entry.lastSwitch = now
	entry.sampleStart = time.Time{}
}

// sampleDirectStreamLocked requires r.mu. Busy intervals of at least
// streamGoodputSampleInterval become goodput samples on the pinned adjacency.
func (r *Runtime) sampleDirectStreamLocked(entry *directStreamAffinityEntry, now time.Time) {
	if entry.adj == nil || entry.holding() {
		entry.sampleStart = time.Time{}
		return
	}
	if !entry.sampleStart.IsZero() && entry.sampleAdj == entry.adj {
		elapsed := now.Sub(entry.sampleStart)
		if elapsed < streamGoodputSampleInterval {
			return
		}
		rate := float64(entry.ackedEnd-entry.sampleAcked) / elapsed.Seconds()
		entry.adj.mu.Lock()
		entry.adj.observeGoodputLocked(rate, now)
		entry.adj.mu.Unlock()
	}
	entry.sampleStart = time.Time{}
	if entry.busy() {
		entry.sampleAdj, entry.sampleStart, entry.sampleAcked = entry.adj, now, entry.ackedEnd
	}
}

// holdDirectStreamFrameLocked requires r.mu and a draining or flushing entry.
func (r *Runtime) holdDirectStreamFrameLocked(key directStreamAffinityKey, entry *directStreamAffinityEntry, envelope *ClusterEnvelope, frame *StreamFrame) {
	entry.queue = append(entry.queue, envelope)
	entry.queueBytes += len(frame.Payload)
	if entry.draining && entry.queueBytes > streamQueueLimitBytes {
		r.finishDirectStreamDrainLocked(key, entry, false)
	}
}

// finishDirectStreamDrainLocked requires r.mu. It ends a drain, on the new
// path when switch is set, and writes the held frames in order.
func (r *Runtime) finishDirectStreamDrainLocked(key directStreamAffinityKey, entry *directStreamAffinityEntry, switchPath bool) {
	target := entry.switchTo
	entry.draining = false
	entry.switchTo = nil
	now := r.pathClock()
	moved := false
	if switchPath && target != nil {
		target.mu.Lock()
		established := target.established
		target.mu.Unlock()
		if established {
			r.moveDirectStreamLocked(entry, target, now)
			moved = true
		}
	}
	if moved {
		entry.holdFor = streamSwitchHold
		r.streamPathStats.drainsCompleted.Add(1)
	} else {
		// A failed drain paused the stream for nothing; do not retry soon.
		entry.lastSwitch = now
		entry.holdFor = 2 * entry.hold()
		if entry.holdFor > streamSwitchBackoffMax {
			entry.holdFor = streamSwitchBackoffMax
		}
		r.streamPathStats.drainsAborted.Add(1)
	}
	if entry.flushing {
		return
	}
	entry.flushing = true
	go r.flushDirectStream(key, entry.epoch)
}

func (r *Runtime) abortDirectStreamDrain(key directStreamAffinityKey, epoch, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.directStreamAffinity[key]
	if r.closed || entry == nil || entry.epoch != epoch || entry.switchSeq != seq || !entry.draining {
		return
	}
	r.finishDirectStreamDrainLocked(key, entry, false)
}

// flushDirectStream writes held frames in order on the entry's adjacency.
// Frames arriving meanwhile are queued behind them, so the stream returns to
// direct sends only once the queue is empty.
func (r *Runtime) flushDirectStream(key directStreamAffinityKey, epoch uint64) {
	for {
		r.mu.Lock()
		entry := r.directStreamAffinity[key]
		if r.closed || entry == nil || entry.epoch != epoch {
			r.mu.Unlock()
			return
		}
		batch := entry.queue
		entry.queue, entry.queueBytes = nil, 0
		if len(batch) == 0 {
			entry.flushing = false
			if entry.closing {
				delete(r.directStreamAffinity, key)
			}
			r.mu.Unlock()
			return
		}
		adj := entry.adj
		for _, envelope := range batch {
			recordDirectStreamSend(entry, envelope.GetStreamFrame())
		}
		ctx := r.ctx
		r.mu.Unlock()
		if ctx == nil {
			ctx = context.Background()
		}
		if adj == nil {
			// Drains start only on a direct adjacency and never clear it.
			continue
		}
		for _, envelope := range batch {
			// A failed write leaves a gap the receiver will not accept; the
			// sender's ACK stall detection resumes the stream in a new epoch.
			_ = r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout)
		}
	}
}

func recordDirectStreamSend(entry *directStreamAffinityEntry, frame *StreamFrame) {
	switch frame.Kind {
	case streamFrameKindOpen:
		entry.awaitOpenAck = true
	case streamFrameKindResume:
		entry.awaitResume = true
	case streamFrameKindData:
		if end := frame.Offset + uint64(len(frame.Payload)); end > entry.sentEnd {
			entry.sentEnd = end
		}
	}
}

// observeInboundStreamFrame records the remote endpoint's acknowledgements of
// frames this node sent: they sample goodput, complete drains and make the
// stream eligible for reselection.
func (r *Runtime) observeInboundStreamFrame(sourceNodeID int64, frame *StreamFrame) {
	if frame == nil || len(frame.StreamId) != 16 || (frame.Kind != streamFrameKindOpenAck && frame.Kind != streamFrameKindAck) {
		return
	}
	key := directStreamAffinityKey{targetNodeID: sourceNodeID, streamID: string(frame.StreamId)}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.directStreamAffinity[key]
	if entry == nil || entry.epoch != frame.Epoch {
		return
	}
	if frame.Kind == streamFrameKindOpenAck {
		entry.awaitOpenAck = false
	} else {
		entry.awaitResume = false
		if frame.Offset > entry.ackedEnd {
			entry.ackedEnd = frame.Offset
		}
	}
	if entry.draining {
		if entry.quiescent() {
			r.finishDirectStreamDrainLocked(key, entry, true)
		}
		return
	}
	r.sampleDirectStreamLocked(entry, r.pathClock())
}

func (r *Runtime) clearDirectStreamAffinity(key directStreamAffinityKey, epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.directStreamAffinity[key]
	if entry == nil || entry.epoch != epoch {
		return
	}
	if entry.holding() {
		// Close is queued behind held Data; the flusher removes the entry.
		entry.closing = true
		return
	}
	delete(r.directStreamAffinity, key)
}

type streamPathCounters struct {
	drainsStarted   atomic.Uint64
	drainsCompleted atomic.Uint64
	drainsAborted   atomic.Uint64
	probes          atomic.Uint64
	quiescentMoves  atomic.Uint64
}

// StreamPathStats counts direct stream path moves since the runtime started.
type StreamPathStats struct {
	DrainsStarted   uint64
	DrainsCompleted uint64
	DrainsAborted   uint64
	Probes          uint64
	QuiescentMoves  uint64
}

// StreamPathStats returns the direct stream path move counters.
func (r *Runtime) StreamPathStats() StreamPathStats {
	return StreamPathStats{
		DrainsStarted:   r.streamPathStats.drainsStarted.Load(),
		DrainsCompleted: r.streamPathStats.drainsCompleted.Load(),
		DrainsAborted:   r.streamPathStats.drainsAborted.Load(),
		Probes:          r.streamPathStats.probes.Load(),
		QuiescentMoves:  r.streamPathStats.quiescentMoves.Load(),
	}
}
