package mesh

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNoRoute 表示规划器无法找到通往目标的路径。
var ErrNoRoute = errors.New("mesh: no route")

// ErrTTLExceeded 表示数据包的跳数已达零，被丢弃。
var ErrTTLExceeded = errors.New("mesh: ttl exhausted")

// ErrDuplicatePacket 表示数据包已在去重表中记录（重复包）。
var ErrDuplicatePacket = errors.New("mesh: duplicate packet")

// ErrLoopDetected 表示下一跳与上一跳节点相同，构成转发循环。
var ErrLoopDetected = errors.New("mesh: forwarding loop detected")

// PacketSender 将转发数据包发送到下一跳节点。
// Runtime.SendPacket 提供默认实现。
type PacketSender interface {
	SendPacket(ctx context.Context, nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) error
}

// LocalPacketHandler 处理目标为本地节点的转发数据包。
type LocalPacketHandler func(ctx context.Context, packet *ForwardedPacket) error

// ForwardingObservation 记录每次转发决策的指标，供观察者消费。
type ForwardingObservation struct {
	TrafficClass       TrafficClass // 数据包的流量类别。
	PathClass          PathClass    // 选择路径的分类。
	EstimatedCost      int64        // 路径预估的往返时间（毫秒）。
	PayloadBytes       int          // 载荷大小（字节）。
	TargetNodeID       int64        // 原始目标节点 ID。
	TopologyGeneration uint64       // 路由计算时使用的拓扑世代号。
	NoPath             bool         // 是否未找到路径。
}

// ForwardingObserver 接收每次转发决策的指标记录。
type ForwardingObserver func(observation ForwardingObservation)

// Engine 是 ForwardingEngine 的实现，提供去重、路由、TTL 管理和本地投递功能。
// 去重机制：以 (source, packetID) 为键，定期清理超过 seenTTL 的过期条目。
type Engine struct {
	localNodeID int64
	snapshotFn  func() TopologySnapshot
	planner     RoutePlanner
	sender      PacketSender
	handler     LocalPacketHandler
	observer    ForwardingObserver

	mu                sync.Mutex
	seen              map[seenKey]time.Time
	seenTTL           time.Duration
	seenSweepInterval time.Duration
	nextSeenSweepAt   time.Time
	now               func() time.Time
}

type seenKey struct {
	sourceNodeID int64
	packetID     uint64
}

func validateForwardedPacket(packet *ForwardedPacket) error {
	if packet == nil {
		return fmt.Errorf("mesh: forwarded packet cannot be nil")
	}
	switch packet.TrafficClass {
	case TrafficTransientInteractive:
		if packet.GetTransientPacket() == nil {
			return fmt.Errorf("mesh: transient forwarded packet must carry transient_packet")
		}
		if len(packet.GetPayload()) != 0 {
			return fmt.Errorf("mesh: transient forwarded packet must not carry payload bytes")
		}
	default:
		if packet.GetTransientPacket() != nil {
			return fmt.Errorf("mesh: non-transient forwarded packet must not carry transient_packet")
		}
		if len(packet.GetPayload()) == 0 {
			return fmt.Errorf("mesh: non-transient forwarded packet must carry payload bytes")
		}
	}
	return nil
}

func forwardedPacketPayloadBytes(packet *ForwardedPacket) int {
	if packet == nil {
		return 0
	}
	if transient := packet.GetTransientPacket(); transient != nil {
		return len(transient.GetBody())
	}
	return len(packet.GetPayload())
}

// NewEngine 创建转发引擎。
// snapshotFn 返回最新拓扑快照；planner 用于路由计算；
// sender 用于发送数据包；handler 处理本地投递；observer 接收转发指标。
func NewEngine(localNodeID int64, snapshotFn func() TopologySnapshot, planner RoutePlanner, sender PacketSender, handler LocalPacketHandler, observer ForwardingObserver) *Engine {
	return &Engine{
		localNodeID:       localNodeID,
		snapshotFn:        snapshotFn,
		planner:           planner,
		sender:            sender,
		handler:           handler,
		observer:          observer,
		seen:              make(map[seenKey]time.Time),
		seenTTL:           30 * time.Second,
		seenSweepInterval: time.Second,
		now:               func() time.Time { return time.Now().UTC() },
	}
}

// Forward 将出站数据包注入转发管道。
// 自动设置默认 TTL 和流量类别（如未设置）。
// 可能返回 ErrDuplicatePacket、ErrNoRoute、ErrTTLExceeded 或 ErrLoopDetected。
func (e *Engine) Forward(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, TransportUnspecified, true)
}

// HandleInbound 处理来自邻接节点的入站转发数据包。
// 与 Forward 类似，但入站数据包在管道开始时立即标记为已见。
func (e *Engine) HandleInbound(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, packet.GetIngressTransport(), false)
}

// forward 是统一的转发管道：
// 验证 → 去重标记（入站立即标记）→ 本地投递 → TTL 检查 → 路由计算 → 回环检测 → 发送。
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
	if err := validateForwardedPacket(packet); err != nil {
		return err
	}
	if packet.TtlHops == 0 {
		packet.TtlHops = DefaultTTLHops
	}

	// 入站数据包立即标记为已见，防止重复处理。
	seenMarked := false
	if !outbound {
		if !e.markSeen(packet) {
			return ErrDuplicatePacket
		}
		seenMarked = true
	}

	// 本地投递：目标为自身时直接分发到本地处理器。
	if packet.TargetNodeId == e.localNodeID {
		if outbound {
			if !e.markSeen(packet) {
				return ErrDuplicatePacket
			}
			seenMarked = true
		}
		return e.deliverLocal(ctx, packet, seenMarked)
	}

	// TTL 耗尽：已达最大跳数，丢弃。
	if packet.TtlHops <= 1 {
		return ErrTTLExceeded
	}

	// 使用最新拓扑快照计算路由。
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

	// 回环检测：如果下一跳等于上一跳，则存在转发循环。
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

	// 出站数据包在发送前标记为已见。
	if outbound {
		if !e.markSeen(packet) {
			return ErrDuplicatePacket
		}
		seenMarked = true
	}

	// 发送到下一跳。
	if err := e.sender.SendPacket(ctx, decision.NextHopNodeID, decision.OutboundTransport, next); err != nil {
		// 发送失败时回滚去重标记，允许后续重试。
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

// deliverLocal 将数据包投递到本地处理器。如果处理器未设置则静默丢弃。
func (e *Engine) deliverLocal(ctx context.Context, packet *ForwardedPacket, seenMarked bool) error {
	if !seenMarked && !e.markSeen(packet) {
		return ErrDuplicatePacket
	}
	if e.handler == nil {
		return nil
	}
	return e.handler(ctx, packet)
}

// markSeen 将数据包加入去重表（以 source + packetID 为键）。
// 如果已存在则返回 false。定期清理超过 seenTTL 的过期条目。
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

// unmarkSeen 用于发送失败时的回滚：从去重表中移除数据包，允许后续重试。
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
		TransientPacket:  packet.TransientPacket,
	}
}

// sweepSeenLocked 删除所有存在时间超过 seenTTL 的过期去重条目。
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
		PayloadBytes:       forwardedPacketPayloadBytes(packet),
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
