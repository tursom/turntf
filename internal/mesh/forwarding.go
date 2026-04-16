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

type Engine struct {
	localNodeID int64
	snapshotFn  func() TopologySnapshot
	planner     RoutePlanner
	sender      PacketSender
	handler     LocalPacketHandler

	mu      sync.Mutex
	seen    map[string]time.Time
	seenTTL time.Duration
	now     func() time.Time
}

func NewEngine(localNodeID int64, snapshotFn func() TopologySnapshot, planner RoutePlanner, sender PacketSender, handler LocalPacketHandler) *Engine {
	return &Engine{
		localNodeID: localNodeID,
		snapshotFn:  snapshotFn,
		planner:     planner,
		sender:      sender,
		handler:     handler,
		seen:        make(map[string]time.Time),
		seenTTL:     30 * time.Second,
		now:         func() time.Time { return time.Now().UTC() },
	}
}

func (e *Engine) Forward(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, TransportUnspecified, true)
}

func (e *Engine) HandleInbound(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, packet.GetIngressTransport(), false)
}

func (e *Engine) forward(ctx context.Context, packet *ForwardedPacket, ingress TransportKind, markSeen bool) error {
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
	if markSeen {
		if !e.markSeen(packet) {
			return ErrDuplicatePacket
		}
	}
	if packet.TargetNodeId == e.localNodeID {
		return e.deliverLocal(ctx, packet)
	}
	if packet.TtlHops <= 1 {
		return ErrTTLExceeded
	}
	decision, ok := e.planner.Compute(e.snapshotFn(), packet.TargetNodeId, packet.TrafficClass, ingress)
	if !ok {
		return ErrNoRoute
	}
	if decision.NextHopNodeID == 0 || decision.OutboundTransport == TransportUnspecified {
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
	return e.sender.SendPacket(ctx, decision.NextHopNodeID, decision.OutboundTransport, next)
}

func (e *Engine) deliverLocal(ctx context.Context, packet *ForwardedPacket) error {
	if !e.markSeen(packet) {
		return ErrDuplicatePacket
	}
	if e.handler == nil {
		return nil
	}
	return e.handler(ctx, cloneForwardedPacket(packet))
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
	now := e.now()
	for key, ts := range e.seen {
		if now.Sub(ts) > e.seenTTL {
			delete(e.seen, key)
		}
	}
	key := fmt.Sprintf("%d:%d", packet.SourceNodeId, packet.PacketId)
	if _, ok := e.seen[key]; ok {
		return false
	}
	e.seen[key] = now
	return true
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
		Payload:          append([]byte(nil), packet.Payload...),
		TraceId:          packet.TraceId,
	}
}
