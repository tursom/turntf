package mesh

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrNoRoute = errors.New("mesh: no route")
var ErrTTLExceeded = errors.New("mesh: ttl exhausted")
var ErrDuplicatePacket = errors.New("mesh: duplicate packet")
var ErrLoopDetected = errors.New("mesh: forwarding loop detected")

type PacketSender interface {
	SendPacket(ctx context.Context, nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) error
}

type LocalPacketHandler func(ctx context.Context, packet *ForwardedPacket) error

type ForwardingObservation struct {
	TrafficClass       TrafficClass
	PathClass          PathClass
	EstimatedCost      int64
	PayloadBytes       int
	TargetNodeID       int64
	TopologyGeneration uint64
	NoPath             bool
}

type ForwardingObserver func(observation ForwardingObservation)

type Engine struct {
	localNodeID int64
	snapshotFn  func() TopologySnapshot
	planner     RoutePlanner
	sender      PacketSender
	handler     LocalPacketHandler
	observer    ForwardingObserver

	mu               sync.Mutex
	seen             map[seenKey]time.Time
	seenTTL          time.Duration
	seenSweepInterval time.Duration
	nextSeenSweepAt  time.Time
	now              func() time.Time
}

type seenKey struct {
	sourceNodeID int64
	packetID     uint64
}

func NewEngine(localNodeID int64, snapshotFn func() TopologySnapshot, planner RoutePlanner, sender PacketSender, handler LocalPacketHandler, observer ForwardingObserver) *Engine {
	return &Engine{
		localNodeID: localNodeID,
		snapshotFn:  snapshotFn,
		planner:     planner,
		sender:      sender,
		handler:     handler,
		observer:    observer,
		seen:             make(map[seenKey]time.Time),
		seenTTL:          30 * time.Second,
		seenSweepInterval: time.Second,
		now:              func() time.Time { return time.Now().UTC() },
	}
}

func (e *Engine) Forward(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, TransportUnspecified, true)
}

func (e *Engine) HandleInbound(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, packet.GetIngressTransport(), false)
}

func (e *Engine) forward(ctx context.Context, packet *ForwardedPacket, ingress TransportKind, outbound bool) error {
	if e == nil || packet == nil {
		return fmt.Errorf("mesh: forwarded packet cannot be nil")
	}
	if packet.SourceNodeId <= 0 || packet.TargetNodeId <= 0 {
		return fmt.Errorf("mesh: packet source and target are required")
	}
	if packet.PacketId == 0 {
		return fmt.Errorf("mesh: packet id is required")
	}
	if packet.TrafficClass == TrafficClassUnspecified {
		packet.TrafficClass = TrafficTransientInteractive
	}
	if packet.TtlHops == 0 {
		packet.TtlHops = DefaultTTLHops
	}
	seenMarked := false
	if !outbound {
		if !e.markSeen(packet) {
			return ErrDuplicatePacket
		}
		seenMarked = true
	}
	if packet.TargetNodeId == e.localNodeID {
		if outbound {
			if !e.markSeen(packet) {
				return ErrDuplicatePacket
			}
			seenMarked = true
		}
		return e.deliverLocal(ctx, packet, seenMarked)
	}
	if packet.TtlHops <= 1 {
		return ErrTTLExceeded
	}
	snapshot := e.snapshotFn()
	decision, ok := e.planner.Compute(snapshot, packet.TargetNodeId, packet.TrafficClass, ingress)
	if !ok {
		e.observeNoPath(packet, snapshot.TopologyGeneration)
		return ErrNoRoute
	}
	if decision.NextHopNodeID == 0 || decision.OutboundTransport == TransportUnspecified {
		e.observeNoPath(packet, snapshot.TopologyGeneration)
		return ErrNoRoute
	}
	if packet.LastHopNodeId != 0 && decision.NextHopNodeID == packet.LastHopNodeId {
		return ErrLoopDetected
	}
	next := cloneForwardedPacket(packet)
	next.LastHopNodeId = e.localNodeID
	next.IngressTransport = decision.OutboundTransport
	next.TtlHops--
	if next.TtlHops == 0 {
		return ErrTTLExceeded
	}
	if outbound {
		if !e.markSeen(packet) {
			return ErrDuplicatePacket
		}
		seenMarked = true
	}
	if err := e.sender.SendPacket(ctx, decision.NextHopNodeID, decision.OutboundTransport, next); err != nil {
		if outbound && seenMarked {
			e.unmarkSeen(packet)
		}
		if errors.Is(err, ErrNoRoute) {
			e.observeNoPath(packet, snapshot.TopologyGeneration)
		}
		return err
	}
	e.observeForward(packet, ingress, decision)
	return nil
}

func (e *Engine) deliverLocal(ctx context.Context, packet *ForwardedPacket, seenMarked bool) error {
	if !seenMarked && !e.markSeen(packet) {
		return ErrDuplicatePacket
	}
	if e.handler == nil {
		return nil
	}
	return e.handler(ctx, packet)
}

func (e *Engine) markSeen(packet *ForwardedPacket) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.now == nil {
		e.now = func() time.Time { return time.Now().UTC() }
	}
	if e.seenTTL <= 0 {
		e.seenTTL = 30 * time.Second
	}
	if e.seenSweepInterval <= 0 {
		e.seenSweepInterval = time.Second
	}
	now := e.now()
	if e.nextSeenSweepAt.IsZero() || !now.Before(e.nextSeenSweepAt) {
		e.sweepSeenLocked(now)
		e.nextSeenSweepAt = now.Add(e.seenSweepInterval)
	}
	key := seenKey{sourceNodeID: packet.SourceNodeId, packetID: packet.PacketId}
	if _, ok := e.seen[key]; ok {
		return false
	}
	e.seen[key] = now
	return true
}

func (e *Engine) unmarkSeen(packet *ForwardedPacket) {
	if e == nil || packet == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.seen, seenKey{sourceNodeID: packet.SourceNodeId, packetID: packet.PacketId})
}

func cloneForwardedPacket(packet *ForwardedPacket) *ForwardedPacket {
	if packet == nil {
		return nil
	}
	return &ForwardedPacket{
		PacketId:         packet.PacketId,
		SourceNodeId:     packet.SourceNodeId,
		TargetNodeId:     packet.TargetNodeId,
		TrafficClass:     packet.TrafficClass,
		LastHopNodeId:    packet.LastHopNodeId,
		IngressTransport: packet.IngressTransport,
		TtlHops:          packet.TtlHops,
		Payload:          packet.Payload,
		TraceId:          packet.TraceId,
	}
}

func (e *Engine) sweepSeenLocked(now time.Time) {
	for key, ts := range e.seen {
		if now.Sub(ts) > e.seenTTL {
			delete(e.seen, key)
		}
	}
}

func (e *Engine) observeForward(packet *ForwardedPacket, ingress TransportKind, decision RouteDecision) {
	if e == nil || e.observer == nil || packet == nil {
		return
	}
	e.observer(ForwardingObservation{
		TrafficClass:       packet.TrafficClass,
		PathClass:          observedPathClass(packet, ingress, decision),
		EstimatedCost:      decision.EstimatedCost,
		PayloadBytes:       len(packet.Payload),
		TargetNodeID:       packet.TargetNodeId,
		TopologyGeneration: decision.TopologyGeneration,
	})
}

func (e *Engine) observeNoPath(packet *ForwardedPacket, generation uint64) {
	if e == nil || e.observer == nil || packet == nil {
		return
	}
	e.observer(ForwardingObservation{
		TrafficClass:       packet.TrafficClass,
		PathClass:          PathClassUnspecified,
		TargetNodeID:       packet.TargetNodeId,
		TopologyGeneration: generation,
		NoPath:             true,
	})
}

func observedPathClass(packet *ForwardedPacket, ingress TransportKind, decision RouteDecision) PathClass {
	if packet == nil {
		return PathClassUnspecified
	}
	if ingress != TransportUnspecified && decision.OutboundTransport != ingress {
		return PathClassCrossTransportBridge
	}
	if decision.NextHopNodeID == packet.TargetNodeId {
		if decision.PathClass == PathClassNativeRelay {
			return PathClassNativeRelay
		}
		return PathClassDirect
	}
	if ingress == TransportUnspecified && decision.PathClass == PathClassNativeRelay {
		return PathClassNativeRelay
	}
	return PathClassSameTransportForward
}
