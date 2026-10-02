package mesh

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type testPathClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testPathClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testPathClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// pathHarness has two WebSocket adjacencies to node 2, like home and kiwi
// (direct plus CDN). primary wins on RTT+jitter until goodput says otherwise.
type pathHarness struct {
	t               *testing.T
	runtime         *Runtime
	clock           *testPathClock
	primary         *Adjacency
	alternate       *Adjacency
	primaryRemote   *fakeConn
	alternateRemote *fakeConn
}

func newPathHarness(t *testing.T) *pathHarness {
	t.Helper()
	h := &pathHarness{t: t, runtime: newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket)), clock: &testPathClock{now: time.Unix(1_000_000, 0)}}
	h.runtime.pathClock = h.clock.Now
	h.runtime.streamDrainTimeout = time.Hour
	primary, primaryRemote := newFakeConnPair(TransportWebSocket, "primary", "target-primary")
	alternate, alternateRemote := newFakeConnPair(TransportWebSocket, "alternate", "target-alternate")
	h.primary = registerTestAdjacency(h.runtime, primary, 2, TransportWebSocket)
	h.alternate = registerTestAdjacency(h.runtime, alternate, 2, TransportWebSocket)
	h.primaryRemote, h.alternateRemote = primaryRemote, alternateRemote
	setTestAdjacencyScore(h.primary, 5, 0)
	setTestAdjacencyScore(h.alternate, 50, 0)
	return h
}

func (h *pathHarness) goodput(adj *Adjacency, bytesPerSecond float64) {
	adj.mu.Lock()
	adj.observeGoodputLocked(bytesPerSecond, h.clock.Now())
	adj.mu.Unlock()
}

func (h *pathHarness) route(id string, kind uint32, offset uint64, size int) {
	h.t.Helper()
	frame := &StreamFrame{StreamId: []byte(id), Kind: kind, Epoch: 1, Offset: offset, Payload: []byte(strings.Repeat("x", size))}
	if err := h.runtime.RouteEnvelope(context.Background(), 2, &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: frame}}); err != nil {
		h.t.Fatalf("route kind %d offset %d: %v", kind, offset, err)
	}
}

func (h *pathHarness) ack(id string, kind uint32, offset uint64) {
	h.runtime.observeInboundStreamFrame(2, &StreamFrame{StreamId: []byte(id), Kind: kind, Epoch: 1, Offset: offset})
}

// expectFrames reads frames from remote and checks their kinds and offsets.
func (h *pathHarness) expectFrames(remote *fakeConn, want ...[2]uint64) {
	h.t.Helper()
	for _, w := range want {
		frame := receiveTestEnvelope(h.t, remote).GetStreamFrame()
		if frame == nil || uint64(frame.Kind) != w[0] || frame.Offset != w[1] {
			h.t.Fatalf("frame = %+v, want kind %d offset %d", frame, w[0], w[1])
		}
	}
}

func (h *pathHarness) expectQuiet(remote *fakeConn) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if raw, err := remote.Receive(ctx); err == nil {
		envelope, _ := (protoCodec{}).Decode(raw)
		h.t.Fatalf("unexpected frame %+v", envelope.GetStreamFrame())
	}
}

const kib = 1 << 10

var streamA = strings.Repeat("a", 16)

func TestAdjacencyGoodputWindow(t *testing.T) {
	adj := &Adjacency{}
	start := time.Unix(1_000_000, 0)
	if _, ok := adj.goodputLocked(start); ok {
		t.Fatal("goodput reported before any sample")
	}
	adj.observeGoodputLocked(100, start)
	adj.observeGoodputLocked(40, start.Add(time.Second))
	if got, _ := adj.goodputLocked(start.Add(time.Second)); got != 100 {
		t.Fatalf("goodput within window = %v, want max 100", got)
	}
	// A degraded path drops out of the maximum after two windows.
	adj.observeGoodputLocked(30, start.Add(streamGoodputWindow))
	if got, _ := adj.goodputLocked(start.Add(streamGoodputWindow)); got != 100 {
		t.Fatalf("goodput in adjacent window = %v, want 100", got)
	}
	adj.observeGoodputLocked(20, start.Add(2*streamGoodputWindow))
	if got, _ := adj.goodputLocked(start.Add(2 * streamGoodputWindow)); got != 30 {
		t.Fatalf("goodput after expiry = %v, want 30", got)
	}
	if _, ok := adj.goodputLocked(start.Add(2*streamGoodputWindow + streamGoodputStaleAfter + time.Second)); ok {
		t.Fatal("stale goodput still reported")
	}
}

func TestDirectStreamSamplesGoodputOnlyWhileBusy(t *testing.T) {
	h := newPathHarness(t)
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.ack(streamA, streamFrameKindOpenAck, 0)
	for offset := uint64(0); offset < 192*kib; offset += 64 * kib {
		h.route(streamA, streamFrameKindData, offset, 64*kib)
	}
	h.ack(streamA, streamFrameKindAck, 64*kib) // 128 KiB still in flight: busy
	h.clock.Advance(time.Second)
	h.ack(streamA, streamFrameKindAck, 192*kib)
	if got, ok := adjacencyGoodput(h.primary, h.clock.Now()); !ok || got != 128*kib {
		t.Fatalf("primary goodput = %v (ok=%v), want %d bytes/s", got, ok, 128*kib)
	}
	if _, ok := adjacencyGoodput(h.alternate, h.clock.Now()); ok {
		t.Fatal("unused alternate has goodput")
	}

	// An application-limited stream (little in flight) measures its demand,
	// not the path, and records nothing.
	h2 := newPathHarness(t)
	h2.route(streamA, streamFrameKindOpen, 0, 0)
	h2.ack(streamA, streamFrameKindOpenAck, 0)
	h2.route(streamA, streamFrameKindData, 0, 16*kib)
	h2.ack(streamA, streamFrameKindAck, 8*kib)
	h2.clock.Advance(time.Second)
	h2.ack(streamA, streamFrameKindAck, 16*kib)
	if _, ok := adjacencyGoodput(h2.primary, h2.clock.Now()); ok {
		t.Fatal("application-limited stream recorded goodput")
	}
}

func TestDirectStreamDrainsBeforeMovingToFasterPath(t *testing.T) {
	h := newPathHarness(t)
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.ack(streamA, streamFrameKindOpenAck, 0)
	h.route(streamA, streamFrameKindData, 0, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindOpen), 0}, [2]uint64{uint64(streamFrameKindData), 0})

	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 4<<20)
	h.clock.Advance(streamSwitchHold)
	// The first Data frame is still unacknowledged: the move must drain.
	h.route(streamA, streamFrameKindData, 64*kib, 64*kib)
	h.route(streamA, streamFrameKindData, 128*kib, 64*kib)
	h.expectQuiet(h.primaryRemote)
	h.expectQuiet(h.alternateRemote)

	h.ack(streamA, streamFrameKindAck, 64*kib)
	h.expectFrames(h.alternateRemote, [2]uint64{uint64(streamFrameKindData), 64 * kib}, [2]uint64{uint64(streamFrameKindData), 128 * kib})
	h.route(streamA, streamFrameKindData, 192*kib, 64*kib)
	h.expectFrames(h.alternateRemote, [2]uint64{uint64(streamFrameKindData), 192 * kib})
	h.expectQuiet(h.primaryRemote)
}

func TestDirectStreamDrainTimeoutKeepsOldPath(t *testing.T) {
	h := newPathHarness(t)
	h.runtime.streamDrainTimeout = 30 * time.Millisecond
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.ack(streamA, streamFrameKindOpenAck, 0)
	h.route(streamA, streamFrameKindData, 0, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindOpen), 0}, [2]uint64{uint64(streamFrameKindData), 0})
	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 4<<20)
	h.clock.Advance(streamSwitchHold)
	h.route(streamA, streamFrameKindData, 64*kib, 64*kib)
	h.route(streamA, streamFrameKindData, 128*kib, 64*kib)

	// No acknowledgement arrives: held frames go out on the old path in order.
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindData), 64 * kib}, [2]uint64{uint64(streamFrameKindData), 128 * kib})
	h.expectQuiet(h.alternateRemote)
}

func TestDirectStreamProbesUnmeasuredPathOncePerInterval(t *testing.T) {
	h := newPathHarness(t)
	streamB := strings.Repeat("b", 16)
	for _, id := range []string{streamA, streamB} {
		h.route(id, streamFrameKindOpen, 0, 0)
		h.ack(id, streamFrameKindOpenAck, 0)
		h.route(id, streamFrameKindData, 0, 64*kib)
		h.route(id, streamFrameKindData, 64*kib, 64*kib)
	}
	h.goodput(h.primary, 1<<20)
	h.clock.Advance(streamSwitchHold)

	// A busy stream on a measured path probes the unmeasured alternative.
	h.route(streamA, streamFrameKindData, 128*kib, 64*kib)
	h.ack(streamA, streamFrameKindAck, 128*kib)
	// Another busy stream to the same node waits for the next probe interval.
	h.route(streamB, streamFrameKindData, 128*kib, 64*kib)

	readUntil := func(remote *fakeConn, id string, offset uint64) {
		t.Helper()
		for {
			frame := receiveTestEnvelope(t, remote).GetStreamFrame()
			if string(frame.StreamId) == id && frame.Kind == streamFrameKindData && frame.Offset == offset {
				return
			}
		}
	}
	readUntil(h.alternateRemote, streamA, 128*kib)
	readUntil(h.primaryRemote, streamB, 128*kib)
	h.expectQuiet(h.alternateRemote)
}

func TestDirectStreamCloseWhileDrainingFollowsHeldData(t *testing.T) {
	h := newPathHarness(t)
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.ack(streamA, streamFrameKindOpenAck, 0)
	h.route(streamA, streamFrameKindData, 0, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindOpen), 0}, [2]uint64{uint64(streamFrameKindData), 0})
	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 4<<20)
	h.clock.Advance(streamSwitchHold)
	h.route(streamA, streamFrameKindData, 64*kib, 64*kib)
	h.route(streamA, streamFrameKindClose, 128*kib, 0)

	h.ack(streamA, streamFrameKindAck, 64*kib)
	h.expectFrames(h.alternateRemote, [2]uint64{uint64(streamFrameKindData), 64 * kib}, [2]uint64{uint64(streamFrameKindClose), 128 * kib})
	deadline := time.Now().Add(time.Second)
	for {
		h.runtime.mu.Lock()
		remaining := len(h.runtime.directStreamAffinity)
		h.runtime.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("affinity not cleared after held Close was flushed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDirectStreamOpenPrefersMeasuredPath(t *testing.T) {
	h := newPathHarness(t)
	// RTT+jitter favours primary, but measurements show alternate delivers more.
	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 3<<20)
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.expectFrames(h.alternateRemote, [2]uint64{uint64(streamFrameKindOpen), 0})
}

func TestDirectStreamFailedDrainBacksOff(t *testing.T) {
	h := newPathHarness(t)
	h.runtime.streamDrainTimeout = 20 * time.Millisecond
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.ack(streamA, streamFrameKindOpenAck, 0)
	h.route(streamA, streamFrameKindData, 0, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindOpen), 0}, [2]uint64{uint64(streamFrameKindData), 0})
	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 4<<20)
	h.clock.Advance(streamSwitchHold)
	h.route(streamA, streamFrameKindData, 64*kib, 64*kib) // starts a drain that times out
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindData), 64 * kib})

	// Retrying right after a failed drain would pause the stream again.
	h.clock.Advance(streamSwitchHold)
	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 4<<20)
	h.route(streamA, streamFrameKindData, 128*kib, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindData), 128 * kib})

	h.clock.Advance(streamSwitchHold)
	h.goodput(h.primary, 1<<20)
	h.goodput(h.alternate, 4<<20)
	h.runtime.mu.Lock()
	h.runtime.streamDrainTimeout = time.Hour // let this drain complete
	h.runtime.mu.Unlock()
	h.route(streamA, streamFrameKindData, 192*kib, 64*kib) // doubled hold has passed
	h.expectQuiet(h.primaryRemote)
	h.ack(streamA, streamFrameKindAck, 192*kib)
	h.expectFrames(h.alternateRemote, [2]uint64{uint64(streamFrameKindData), 192 * kib})

	stats := h.runtime.StreamPathStats()
	if stats.DrainsStarted != 2 || stats.DrainsAborted != 1 || stats.DrainsCompleted != 1 {
		t.Fatalf("stream path stats = %+v, want 2 started, 1 aborted, 1 completed", stats)
	}
}

func TestDirectStreamIgnoresOldPeakOfLeftPath(t *testing.T) {
	h := newPathHarness(t)
	h.route(streamA, streamFrameKindOpen, 0, 0)
	h.ack(streamA, streamFrameKindOpenAck, 0)
	h.route(streamA, streamFrameKindData, 0, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindOpen), 0}, [2]uint64{uint64(streamFrameKindData), 0})
	// alternate was fast a while ago; primary is measured now.
	h.goodput(h.alternate, 8<<20)
	h.clock.Advance(streamGoodputFreshFor + time.Second)
	h.goodput(h.primary, 1<<20)
	h.streamProbeRecently()
	h.route(streamA, streamFrameKindData, 64*kib, 64*kib)
	h.expectFrames(h.primaryRemote, [2]uint64{uint64(streamFrameKindData), 64 * kib})
	if stats := h.runtime.StreamPathStats(); stats.DrainsStarted != 0 {
		t.Fatalf("old peak started a drain: %+v", stats)
	}
}

// streamProbeRecently spends the probe budget so a test sees only measured moves.
func (h *pathHarness) streamProbeRecently() {
	h.runtime.mu.Lock()
	h.runtime.streamProbeAt[2] = h.clock.Now()
	h.runtime.mu.Unlock()
}

func TestAdjacencySnapshotReportsGoodput(t *testing.T) {
	h := newPathHarness(t)
	h.goodput(h.alternate, 3<<20)
	h.clock.Advance(2 * time.Second)
	for _, snapshot := range h.runtime.Adjacencies() {
		switch snapshot.RemoteHint {
		case h.alternate.RemoteHint:
			if snapshot.GoodputBps != 3<<20 || snapshot.GoodputAge != 2*time.Second {
				t.Fatalf("alternate snapshot goodput = %v age %v", snapshot.GoodputBps, snapshot.GoodputAge)
			}
		case h.primary.RemoteHint:
			if snapshot.GoodputAge >= 0 {
				t.Fatalf("unmeasured snapshot age = %v, want negative", snapshot.GoodputAge)
			}
		}
	}
}
