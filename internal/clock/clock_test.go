package clock

import "testing"

func TestTimestampStringRoundTrip(t *testing.T) {
	t.Parallel()

	original := Timestamp{
		WallTimeMs: 1740000000000,
		Logical:    7,
		NodeID:     42,
	}

	parsed, err := ParseTimestamp(original.String())
	if err != nil {
		t.Fatalf("parse timestamp: %v", err)
	}
	if parsed != original {
		t.Fatalf("timestamp mismatch: got %+v want %+v", parsed, original)
	}
}

func TestClockNowMonotonic(t *testing.T) {
	t.Parallel()

	clock := NewClock(9)
	first := clock.Now()
	second := clock.Now()

	if first.Compare(second) >= 0 {
		t.Fatalf("expected second timestamp to be greater, first=%s second=%s", first, second)
	}
}

func TestClockObserveRemote(t *testing.T) {
	t.Parallel()

	clock := NewClock(11)
	local := clock.Now()
	remote := Timestamp{
		WallTimeMs: local.WallTimeMs + 10,
		Logical:    3,
		NodeID:     3,
	}

	observed := clock.Observe(remote)
	if observed.Compare(remote) <= 0 {
		t.Fatalf("expected observed timestamp to advance past remote, observed=%s remote=%s", observed, remote)
	}
	if observed.NodeID != 11 {
		t.Fatalf("expected local node id, got %d", observed.NodeID)
	}
}

func TestClockNowAppliesOffset(t *testing.T) {
	t.Parallel()

	nowMs := int64(1740000000000)
	clock := NewClockWithSource(5, func() int64 {
		return nowMs
	})
	clock.SetOffsetMs(250)

	ts := clock.Now()
	if ts.WallTimeMs != nowMs+250 {
		t.Fatalf("unexpected wall time: got=%d want=%d", ts.WallTimeMs, nowMs+250)
	}
}

func TestClockRemainsMonotonicWhenOffsetMovesBackward(t *testing.T) {
	t.Parallel()

	nowMs := int64(1740000000000)
	clock := NewClockWithSource(6, func() int64 {
		return nowMs
	})
	clock.SetOffsetMs(400)
	first := clock.Now()

	clock.SetOffsetMs(-200)
	second := clock.Now()
	if second.Compare(first) <= 0 {
		t.Fatalf("expected monotonic clock after offset change, first=%s second=%s", first, second)
	}
}

func TestIDGeneratorMonotonicAndUnique(t *testing.T) {
	t.Parallel()

	gen, err := NewIDGenerator(21)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}

	last := int64(-1)
	seen := make(map[int64]struct{})
	for range 1024 {
		next := gen.Next()
		if next <= last {
			t.Fatalf("ids must increase: last=%d next=%d", last, next)
		}
		if _, ok := seen[next]; ok {
			t.Fatalf("duplicate id %d", next)
		}
		seen[next] = struct{}{}
		last = next
	}
}
