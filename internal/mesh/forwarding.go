package mesh

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ---------------- 转发引擎的错误常量 ----------------

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
	// SendPacket 将 packet 通过指定传输发送到 nextHopNodeID。
	// ctx 用于超时控制；transport 指明出站传输类型。
	SendPacket(ctx context.Context, nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) error
}

// LocalPacketHandler 处理目标为本地节点的转发数据包。
// 如果未设置，本地投递将静默丢弃数据包。
type LocalPacketHandler func(ctx context.Context, packet *ForwardedPacket) error

// ForwardingObservation 记录每次转发决策的指标，供观察者消费。
type ForwardingObservation struct {
	TrafficClass       TrafficClass // 数据包的流量类别。
	PathClass          PathClass    // 选择路径的分类。
	EstimatedCost      int64        // 路径预估的往返时间（毫秒）。
	PayloadBytes       int          // 载荷大小（字节）。
	TargetNodeID       int64        // 原始目标节点 ID。
	TopologyGeneration uint64       // 路由计算时使用的拓扑世代号。
	NoPath             bool         // 是否未找到路径（无可用路由）。
}

// ForwardingObserver 接收每次转发决策的指标记录。
// 注册到 Engine.observer 后，每次转发或路由失败时被调用。
type ForwardingObserver func(observation ForwardingObservation)

// Engine 是 ForwardingEngine 的实现，提供去重、路由、TTL 管理和本地投递功能。
//
// 去重机制：以 (source, packetID) 为键的 seen 表，定期清理超过 seenTTL 的过期条目。
// 去重有两个目的：
//   - 入站时防止同一数据包被多次接收（因多路径或重传导致）。
//   - 出站时防止本地注入的重复数据包。
//
// 转发管道的执行顺序：
//
//	验证 → 去重标记 → 本地投递判定 → TTL 检查 → 路由计算 → 回环检测 → 发送。
type Engine struct {
	// localNodeID 本地节点 ID，用于本地投递判定和 LastHop 设置。
	localNodeID int64
	// snapshotFn 返回当前最新的拓扑快照，每次转发时调用以保证路由时效性。
	snapshotFn func() TopologySnapshot
	// planner 路由规划器，用于计算到目标节点的最佳路径。
	planner RoutePlanner
	// sender 数据包发送接口，将包发往下一跳节点。
	sender PacketSender
	// handler 本地投递处理器，目标节点为本地时调用。
	handler LocalPacketHandler
	// observer 转发观察者，每次转发或路由失败时记录指标。
	observer ForwardingObserver

	// mu 保护以下 seen 相关字段的并发访问。
	mu sync.Mutex
	// seen 去重表，键为 (源节点 ID, 数据包 ID)，值为记录时间戳。
	seen map[seenKey]time.Time
	// seenTTL 去重条目在 seen 表中的生存时间，超时后会被清理。默认 30s。
	seenTTL time.Duration
	// seenSweepInterval 去重表过期条目清理的间隔时间。默认 1s。
	seenSweepInterval time.Duration
	// nextSeenSweepAt 下一次清理操作的计划执行时间。
	nextSeenSweepAt time.Time
	// now 返回当前时间，可注入用于测试。
	now func() time.Time
}

// seenKey 是去重表的键，由源节点 ID 和数据包 ID 组成。
type seenKey struct {
	sourceNodeID int64  // 数据包的源节点。
	packetID     uint64 // 数据包的唯一标识符。
}

// validateForwardedPacket 验证转发数据包的格式合法性：
//   - 瞬时交互（TransientInteractive）必须携带 TransientPacket 且不能有 Payload。
//   - 非瞬时交互必须携带 Payload 且不能有 TransientPacket。
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

// forwardedPacketPayloadBytes 返回数据包的载荷字节数。
// 瞬时交互包从 TransientPacket.Body 获取大小，普通包从 Payload 获取。
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
// 去重表默认 TTL 为 30 秒，清理间隔为 1 秒。
func NewEngine(
	localNodeID int64,
	snapshotFn func() TopologySnapshot,
	planner RoutePlanner,
	sender PacketSender,
	handler LocalPacketHandler,
	observer ForwardingObserver,
) *Engine {
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
// 由本地产生（或从应用层收到）的数据包调用此方法。
// 自动设置默认 TTL 和流量类别（如未设置）。
// 在发送成功之前不标记去重，以便发送失败后可重试。
// 可能返回 ErrDuplicatePacket、ErrNoRoute、ErrTTLExceeded 或 ErrLoopDetected。
func (e *Engine) Forward(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, TransportUnspecified, true)
}

// HandleInbound 处理来自邻接节点的入站转发数据包。
// 由 Runtime 接收到远端发来的 ForwardedPacket 后调用此方法。
// 与 Forward 的关键区别：入站数据包在管道开始时立即标记为已见，
// 防止同一数据包从多个入站连接重复到达。
func (e *Engine) HandleInbound(ctx context.Context, packet *ForwardedPacket) error {
	return e.forward(ctx, packet, packet.GetIngressTransport(), false)
}

// forward 是统一的转发管道核心，outbound 为 true 表示由本地产生，false 表示由远端入站。
// 管道执行顺序（带版本/去重保护）：
//
//  1. 基本字段验证（来源、目标、ID、流量类别、载荷格式）
//  2. 入站数据包立即标记去重（防止多路径重复接收）
//  3. 本地投递：目标为本节点则直接交付，无需继续转发
//  4. TTL 检查：达到最大跳数则丢弃
//  5. 路由计算：使用最新拓扑快照查询最佳下一跳
//  6. 回环检测：下一跳等于上一跳则丢弃
//  7. 克隆数据包，更新 LastHop 和 TTL，出站标记去重后发送
//  8. 发送失败时回滚去重标记，允许后续重试
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

// deliverLocal 将数据包投递到本地处理器（目标为本节点的数据包）。
// 如果数据包尚未标记去重（seenMarked == false），则先标记去重；
// 若已存在则返回 ErrDuplicatePacket。
// 如果处理器未设置（handler == nil）则静默丢弃，不返回错误。
func (e *Engine) deliverLocal(ctx context.Context, packet *ForwardedPacket, seenMarked bool) error {
	if !seenMarked && !e.markSeen(packet) {
		return ErrDuplicatePacket
	}
	if e.handler == nil {
		return nil
	}
	return e.handler(ctx, packet)
}

// markSeen 将数据包加入去重表，键为 (SourceNodeID, PacketID)。
// 如果键已存在（重复包）则返回 false，否则插入当前时间戳并返回 true。
// 在每次插入前会检查是否需要执行过期条目清理（seenSweepInterval 间隔）。
// 本方法内部加锁，线程安全。
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

// unmarkSeen 从去重表中移除指定数据包，用于发送失败时的回滚操作。
// 这允许后续重试同一数据包而不被去重拦截。nil 安全。
func (e *Engine) unmarkSeen(packet *ForwardedPacket) {
	if e == nil || packet == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.seen, seenKey{sourceNodeID: packet.SourceNodeId, packetID: packet.PacketId})
}

// cloneForwardedPacket 深度复制 ForwardedPacket。
// 转发时必须克隆原包再修改 LastHopNodeId、IngressTransport、TtlHops 等字段，
// 以避免修改原始数据包（可能在上层仍被引用）。
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

// sweepSeenLocked 遍历去重表，删除所有存在时间超过 seenTTL 的过期条目。
// 必须在持有 e.mu 锁的情况下调用。
// 这是防止去重表无限增长的关键清理机制。
func (e *Engine) sweepSeenLocked(now time.Time) {
	for key, ts := range e.seen {
		if now.Sub(ts) > e.seenTTL {
			delete(e.seen, key)
		}
	}
}

// observeForward 构造转发成功的观察指标并通知 observer。
// 其中 PathClass 根据入站/出站传输比较和决策信息动态判定。
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

// observeNoPath 构造路由失败的观察指标（NoPath = true）并通知 observer。
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

// observedPathClass 根据入站传输和路由决策推断实际使用的路径分类：
//   - 入站传输与出站传输不同 → CrossTransportBridge（跨传输桥接）。
//   - 下一跳就是目标节点且是中继路径 → NativeRelay。
//   - 下一跳就是目标节点 → Direct（直连）。
//   - 无入站传输且决策为中继 → NativeRelay。
//   - 其他 → SameTransportForward（同传输多跳转发）。
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
