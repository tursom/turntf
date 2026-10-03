package mesh

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func testCandidate(name string, transit bool, stability int64, capacity float64) streamPathCandidate {
	return streamPathCandidate{adj: &Adjacency{RemoteHint: name}, transit: transit, bestDirect: !transit, stabilityMs: stability, capacityBps: capacity}
}

func TestPreferredStreamPathLightPrefersStableTransit(t *testing.T) {
	direct := testCandidate("direct", false, 149+240, 6e6) // 8% 丢包的直连
	transit := testCandidate("transit", true, 137+27+30, 3e6)
	candidates := []streamPathCandidate{direct, transit}
	if got := preferredStreamPathLocked(candidates, direct.adj, false); got != transit.adj {
		t.Fatalf("light load stayed on lossy direct path: %s", got.RemoteHint)
	}
	// 差距在余量内时保持当前路径，避免抖动。
	close := testCandidate("transit", true, 149+240-30, 3e6)
	if got := preferredStreamPathLocked([]streamPathCandidate{direct, close}, direct.adj, false); got != direct.adj {
		t.Fatalf("light load switched within margin: %s", got.RemoteHint)
	}
}

func TestPreferredStreamPathHeavyKeepsBandwidth(t *testing.T) {
	cases := []struct {
		name      string
		directCap float64
		transit   float64
		current   string
		want      string
	}{
		{"transit slower stays direct", 6e6, 3e6, "direct", "direct"},
		{"transit slightly faster stays direct", 6e6, 7e6, "direct", "direct"},
		{"transit much faster moves", 2e6, 3.5e6, "direct", "transit"},
		{"direct capacity unknown stays direct", 0, 9e6, "direct", "direct"},
		{"transit capacity unknown stays direct", 6e6, 0, "direct", "direct"},
		{"heavy on transit falls back when slower", 6e6, 3e6, "transit", "direct"},
		{"heavy on transit keeps when comparable", 6e6, 6.7e6, "transit", "transit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 直连稳定性更差（丢包），轻负载会选中转；重负载只看容量。
			direct := testCandidate("direct", false, 400, c.directCap)
			transit := testCandidate("transit", true, 200, c.transit)
			current := direct.adj
			if c.current == "transit" {
				current = transit.adj
			}
			got := preferredStreamPathLocked([]streamPathCandidate{direct, transit}, current, true)
			if got.RemoteHint != c.want {
				t.Fatalf("heavy chose %s, want %s", got.RemoteHint, c.want)
			}
		})
	}
}

func TestStreamModeHysteresis(t *testing.T) {
	var e directStreamAffinityEntry
	now := time.Now()
	e.observeSend(0, now)
	e.observeSend(1<<20, now.Add(time.Second)) // 1 MiB/s
	e.updateMode(now.Add(time.Second))
	if !e.heavy {
		t.Fatalf("1 MiB/s did not enter heavy mode: rate=%.0f", e.rateBps)
	}
	e.observeSend(200<<10, now.Add(2*time.Second)) // EWMA 降到约 600 KiB/s，仍高于退出阈值
	e.updateMode(now.Add(2 * time.Second))
	if !e.heavy {
		t.Fatalf("left heavy mode above exit threshold: rate=%.0f", e.rateBps)
	}
	e.updateMode(now.Add(8 * time.Second)) // 长时间无发送
	if e.heavy {
		t.Fatal("idle stream stayed in heavy mode")
	}
}

func TestObserveTCPInfoEstimatesLossAndCapacity(t *testing.T) {
	adj := &Adjacency{}
	now := time.Now()
	// 首个样本只建立基线。
	if adj.observeTCPInfoLocked(TCPInfo{SegsOut: 1000, TotalRetrans: 100, BytesAcked: 1e6}, now) {
		t.Fatal("baseline sample advertised")
	}
	// 2 秒确认 8 MB：4 MB/s，首个容量估计立即公告；报文段不足，不计重传率。
	if !adj.observeTCPInfoLocked(TCPInfo{SegsOut: 1010, TotalRetrans: 105, BytesAcked: 9e6}, now.Add(2*time.Second)) || adj.advertisedCapacity != 32000 {
		t.Fatalf("first capacity not advertised: cap=%.0f advertised=%d", adj.tcpCapacityBps, adj.advertisedCapacity)
	}
	if adj.tcpLossPermille != 0 {
		t.Fatalf("loss updated from too few segments: %.1f", adj.tcpLossPermille)
	}
	// 1000 段中重传 80：80‰，EWMA 首次 0.3×80=24‰；公告在限频期内推迟。空闲样本不拉低容量。
	if adj.observeTCPInfoLocked(TCPInfo{SegsOut: 2000, TotalRetrans: 180, BytesAcked: 9.1e6}, now.Add(4*time.Second)) {
		t.Fatal("quality republished within rate limit")
	}
	if adj.tcpLossPermille < 23 || adj.tcpLossPermille > 25 {
		t.Fatalf("loss EWMA = %.1f, want about 24", adj.tcpLossPermille)
	}
	if adj.tcpCapacityBps < 3.9e6 {
		t.Fatalf("idle sample collapsed capacity: %.0f", adj.tcpCapacityBps)
	}
	if !adj.observeTCPInfoLocked(TCPInfo{SegsOut: 2010, TotalRetrans: 180, BytesAcked: 9.2e6}, now.Add(12*time.Second)) || adj.advertisedLoss != 25 || adj.advertisedCapacity != 32000 {
		t.Fatalf("advertised loss=%d cap=%d", adj.advertisedLoss, adj.advertisedCapacity)
	}
	// 2 秒确认 12 MB：6 MB/s；限频期过后实际吞吐提升超过 25% 时重新公告。
	adj.observeTCPInfoLocked(TCPInfo{SegsOut: 2020, TotalRetrans: 180, BytesAcked: 21.2e6}, now.Add(14*time.Second))
	if !adj.observeTCPInfoLocked(TCPInfo{SegsOut: 2030, TotalRetrans: 180, BytesAcked: 21.3e6}, now.Add(24*time.Second)) || adj.advertisedCapacity < 47000 {
		t.Fatalf("capacity increase not advertised: cap=%.0f advertised=%d", adj.tcpCapacityBps, adj.advertisedCapacity)
	}
	// 连接重建（计数回退）只重建基线。
	adj.observeTCPInfoLocked(TCPInfo{SegsOut: 5, BytesAcked: 100}, now.Add(26*time.Second))
	if adj.tcpCapacityBps < 5.9e6 {
		t.Fatalf("counter reset corrupted capacity: %.0f", adj.tcpCapacityBps)
	}
}

// drainCounters 记录排空测试中两条邻接的发送；排队帧由后台协程发出，需原子计数。
type drainCounters struct{ primary, alternate atomic.Int64 }

func (h *reselectHarness) countDrainSends() *drainCounters {
	c := &drainCounters{}
	h.primary.Conn.(*fakeConn).mu.Lock()
	h.primary.Conn.(*fakeConn).sendHook = func([]byte) error { c.primary.Add(1); return nil }
	h.primary.Conn.(*fakeConn).mu.Unlock()
	h.alternate.Conn.(*fakeConn).mu.Lock()
	h.alternate.Conn.(*fakeConn).sendHook = func([]byte) error { c.alternate.Add(1); return nil }
	h.alternate.Conn.(*fakeConn).mu.Unlock()
	return c
}

func waitCount(t *testing.T, v *atomic.Int64, want int64, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for v.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %d sends, want %d", what, v.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDirectStreamDrainSwitchesAfterAck(t *testing.T) {
	h := newReselectHarness(t)
	sends := h.countDrainSends()
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	waitCount(t, &sends.primary, 2, "initial frames on primary")

	h.scores(100, 1)
	h.allowDrainNow()
	// 排空中的帧立即返回（不阻塞会话），在 core 内排队且不得越过未确认的数据。
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	h.send(testStreamFrame(streamFrameKindData, 1, 11, "third"))
	time.Sleep(100 * time.Millisecond)
	if sends.alternate.Load() != 0 || sends.primary.Load() != 2 {
		t.Fatalf("queued data sent before drain: primary=%d alternate=%d", sends.primary.Load(), sends.alternate.Load())
	}
	h.inbound(testStreamFrame(streamFrameKindAck, 1, 5, ""))
	waitCount(t, &sends.alternate, 2, "queued frames on new path")
	h.send(testStreamFrame(streamFrameKindData, 1, 16, "fourth"))
	waitCount(t, &sends.alternate, 3, "data after drain")
	paths := h.runtime.StreamPaths()
	if len(paths) != 1 || paths[0].Drains != 1 || paths[0].Switches != 1 || paths[0].Draining {
		t.Fatalf("stream path stats: %+v", paths)
	}
}

func TestDirectStreamDrainTimeoutKeepsPath(t *testing.T) {
	old := streamPathDrainTimeout
	streamPathDrainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { streamPathDrainTimeout = old })
	h := newReselectHarness(t)
	sends := h.countDrainSends()
	h.send(testStreamFrame(streamFrameKindOpen, 1, 0, ""))
	h.inbound(testStreamFrame(streamFrameKindOpenAck, 1, 0, ""))
	h.send(testStreamFrame(streamFrameKindData, 1, 0, "first"))
	h.scores(100, 1)
	h.allowDrainNow()
	h.send(testStreamFrame(streamFrameKindData, 1, 5, "second"))
	time.Sleep(100 * time.Millisecond)
	if sends.primary.Load() != 2 {
		t.Fatalf("queued frame sent before drain timeout: %d", sends.primary.Load())
	}
	// 未确认导致超时：放弃换路，排队帧按序发回旧路径。
	waitCount(t, &sends.primary, 3, "queued frame after drain timeout")
	paths := h.runtime.StreamPaths()
	if len(paths) != 1 || paths[0].DrainAborts != 1 || paths[0].Draining || sends.alternate.Load() != 0 {
		t.Fatalf("stream path stats: %+v alternate=%d", paths, sends.alternate.Load())
	}
	// 放弃后退避：有在途数据时不再发起排空。
	h.send(testStreamFrame(streamFrameKindData, 1, 11, "third"))
	waitCount(t, &sends.primary, 4, "data during backoff")
}

// allowDrainNow 让下一帧立即参与排空评估（跳过评估间隔）。
func (h *reselectHarness) allowDrainNow() {
	h.runtime.mu.Lock()
	defer h.runtime.mu.Unlock()
	for key, entry := range h.runtime.directStreamAffinity {
		entry.nextEval = time.Time{}
		h.runtime.directStreamAffinity[key] = entry
	}
}

func TestRuntimeStreamUsesTransitAroundLossyDirect(t *testing.T) {
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	adapterC := newFakeAdapter(TransportLibP2P)
	delivered := make(chan *ForwardedPacket, 4)
	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)
	runtimeC := newTestRuntime(t, 3, adapterC, func(opts *RuntimeOptions) {
		opts.EnvelopeHandler = func(_ context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error {
			if envelope.GetStreamFrame() != nil {
				delivered <- packet
			}
			return nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, runtime := range []*Runtime{runtimeA, runtimeB, runtimeC} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatalf("start runtime: %v", err)
		}
		defer runtime.Close()
	}
	connAB, connBA := newFakeConnPair(TransportLibP2P, "A", "B")
	connBC, connCB := newFakeConnPair(TransportLibP2P, "B", "C")
	connAC, connCA := newFakeConnPair(TransportLibP2P, "A", "C")
	adapterA.accept <- connAB
	adapterB.accept <- connBA
	adapterB.accept <- connBC
	adapterC.accept <- connCB
	adapterA.accept <- connAC
	adapterC.accept <- connCA
	waitForNodes(t, runtimeA, []int64{1, 2, 3}, 3*time.Second)
	// 等 B 公告的 B→C 链路进入 A 的拓扑。
	deadline := time.Now().Add(3 * time.Second)
	for {
		snapshot := runtimeA.store.Snapshot()
		snapshot.ensureOutgoingLinks()
		if _, ok := streamTransitLink(snapshot, 2, 3, TransportLibP2P); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("transit link B->C not advertised to A")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A→C 直连发送侧丢包 8%。
	runtimeA.mu.Lock()
	for _, adj := range runtimeA.adjByConn {
		if adj.RemoteNodeID == 3 {
			adj.mu.Lock()
			adj.tcpLossPermille = 80
			adj.mu.Unlock()
		}
	}
	runtimeA.mu.Unlock()

	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: testStreamFrame(streamFrameKindOpen, 1, 0, "")}}
	if err := runtimeA.RouteEnvelope(ctx, 3, envelope); err != nil {
		t.Fatalf("route stream: %v", err)
	}
	select {
	case packet := <-delivered:
		if packet.SourceNodeId != 1 || packet.TargetNodeId != 3 || packet.LastHopNodeId != 2 {
			t.Fatalf("stream did not arrive through transit B: %+v", packet)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for transit stream delivery")
	}
	paths := runtimeA.StreamPaths()
	if len(paths) != 1 || paths[0].ViaNodeID != 2 {
		t.Fatalf("stream path snapshot: %+v", paths)
	}
}

// probeFixture：直连到目标 9，中转经节点 7；直连丢包使中转明显更稳定。
func probeFixture() (*directStreamAffinityEntry, []streamPathCandidate, *Adjacency, *Adjacency, time.Time) {
	direct := &Adjacency{RemoteNodeID: 9}
	transit := &Adjacency{RemoteNodeID: 7}
	candidates := []streamPathCandidate{
		{adj: direct, bestDirect: true, stabilityMs: 400},
		{adj: transit, transit: true, stabilityMs: 200},
	}
	now := time.Now()
	e := &directStreamAffinityEntry{adj: direct, heavy: true}
	e.resetWindow(now)
	return e, candidates, direct, transit, now
}

func TestStreamProbeKeepsFasterTransit(t *testing.T) {
	e, candidates, _, transit, now := probeFixture()
	// 基线不足 8 秒时不探测。
	if _, handled := streamProbeDecisionLocked(e, candidates, 9, now.Add(5*time.Second)); handled {
		t.Fatal("probed before baseline window")
	}
	e.ackedEnd = 8 << 20 // 直连 8 秒确认 8 MiB：1 MiB/s
	got, handled := streamProbeDecisionLocked(e, candidates, 9, now.Add(8*time.Second))
	if !handled || got != transit || e.probe == nil || e.stats.probes != 1 {
		t.Fatalf("probe not started: got=%v handled=%v probe=%+v", got, handled, e.probe)
	}
	// 排空完成后在中转上测 10 秒，确认 15 MiB：1.5 MiB/s ≥ 1.2 倍基线。
	start := now.Add(9 * time.Second)
	e.adj = transit
	e.resetWindow(start)
	if got, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(5*time.Second)); !handled || got != transit {
		t.Fatal("probe left transit before its window ended")
	}
	e.ackedEnd += 15 << 20
	if got, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(10*time.Second)); !handled || got != transit || e.provenVia != transit || e.stats.probeWins != 1 {
		t.Fatalf("faster transit not kept: got=%v proven=%v wins=%d", got, e.provenVia, e.stats.probeWins)
	}
	// 结论有效期内重负载留在中转，不交给容量规则。
	if got, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(5*time.Minute)); !handled || got != transit {
		t.Fatal("proven transit not kept within validity")
	}
	if _, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(11*time.Minute)); handled {
		t.Fatal("expired proof still overrides heavy rule")
	}
}

func TestStreamProbeRevertsSlowerTransit(t *testing.T) {
	e, candidates, direct, transit, now := probeFixture()
	e.ackedEnd = 8 << 20
	streamProbeDecisionLocked(e, candidates, 9, now.Add(8*time.Second))
	start := now.Add(9 * time.Second)
	e.adj = transit
	e.resetWindow(start)
	e.ackedEnd += 10 << 20 // 1.0 MiB/s，未达 1.2 倍
	got, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(10*time.Second))
	if !handled || got != direct || e.probe != nil {
		t.Fatalf("slower transit not reverted: got=%v probe=%+v", got, e.probe)
	}
	// 切回直连后 10 分钟内不再探测。
	e.adj = direct
	e.resetWindow(start.Add(11 * time.Second))
	e.ackedEnd += 20 << 20
	if _, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(time.Minute)); handled {
		t.Fatal("re-probed within retry interval")
	}
	if got, handled := streamProbeDecisionLocked(e, candidates, 9, start.Add(11*time.Minute)); !handled || got != transit {
		t.Fatal("did not re-probe after retry interval")
	}
}

func TestStreamProbeSkipsWhenNotUseful(t *testing.T) {
	e, candidates, _, _, now := probeFixture()
	e.ackedEnd = 8 << 20
	// 中转不明显更稳定：不探测。
	candidates[1].stabilityMs = 360
	if _, handled := streamProbeDecisionLocked(e, candidates, 9, now.Add(8*time.Second)); handled {
		t.Fatal("probed a transit within the stability margin")
	}
	// 轻负载：清除探测并交给常规规则。
	candidates[1].stabilityMs = 200
	e.probe = &streamProbe{}
	e.heavy = false
	if _, handled := streamProbeDecisionLocked(e, candidates, 9, now.Add(9*time.Second)); handled || e.probe != nil {
		t.Fatal("light stream kept probe state")
	}
}
