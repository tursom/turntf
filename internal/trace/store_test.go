package trace

import (
	"testing"
	"time"
)

func TestStoreBoundsAndExpiry(t *testing.T) {
	s := NewStore()
	now := time.Now()
	s.now = func() time.Time { return now }
	id, err := NewID()
	if err != nil || !ValidID(id) {
		t.Fatalf("invalid generated trace id %q: %v", id, err)
	}
	for i := 0; i < maxEventsPerTrace+4; i++ {
		s.Add(Event{TraceID: id, NodeID: 1, Stage: "forwarded"})
	}
	events := s.Get(id)
	if len(events) != maxEventsPerTrace || events[0].Sequence != 5 {
		t.Fatalf("expected bounded, ordered trace, got %d events, first %+v", len(events), events[0])
	}
	now = now.Add(Retention)
	if got := s.Get(id); len(got) != 0 {
		t.Fatalf("expired trace retained: %+v", got)
	}
	s.Add(Event{TraceID: "untrusted", NodeID: 1})
	if len(s.byID) != 0 {
		t.Fatal("untrusted trace id was retained")
	}
}
