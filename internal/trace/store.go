package trace

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

const (
	Retention         = 10 * time.Minute
	maxTraces         = 512
	maxEventsPerTrace = 128
)

// Event contains only routing metadata; message bodies and session credentials are never stored.
type Event struct {
	TraceID            string    `json:"trace_id"`
	Kind               string    `json:"kind"`
	Stage              string    `json:"stage"`
	NodeID             int64     `json:"node_id"`
	SourceNodeID       int64     `json:"source_node_id,omitempty"`
	TargetNodeID       int64     `json:"target_node_id,omitempty"`
	PeerNodeID         int64     `json:"peer_node_id,omitempty"`
	MessageNodeID      int64     `json:"message_node_id,omitempty"`
	MessageSeq         int64     `json:"message_seq,omitempty"`
	RecipientNodeID    int64     `json:"recipient_node_id,omitempty"`
	RecipientUserID    int64     `json:"recipient_user_id,omitempty"`
	EventID            int64     `json:"event_id,omitempty"`
	PacketID           uint64    `json:"packet_id,omitempty"`
	SourceRuntimeEpoch uint64    `json:"source_runtime_epoch,omitempty"`
	Transport          string    `json:"transport,omitempty"`
	PathClass          string    `json:"path_class,omitempty"`
	EstimatedCost      int64     `json:"estimated_cost,omitempty"`
	DurationMs         int64     `json:"duration_ms,omitempty"`
	TopologyGeneration uint64    `json:"topology_generation,omitempty"`
	Reason             string    `json:"reason,omitempty"`
	Path               string    `json:"path,omitempty"`
	At                 time.Time `json:"at"`
	Sequence           uint64    `json:"sequence"`
}

type bucket struct {
	events []Event
	last   time.Time
}

// Store is bounded per node. Trace expiry does not affect message delivery.
type Store struct {
	mu       sync.Mutex
	byID     map[string]*bucket
	sequence uint64
	now      func() time.Time
}

func NewStore() *Store {
	return &Store{byID: make(map[string]*bucket), now: time.Now}
}

func NewID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func ValidID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func (s *Store) Add(event Event) {
	if s == nil || !ValidID(event.TraceID) || event.NodeID <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	s.prune(now)
	b := s.byID[event.TraceID]
	if b == nil {
		if len(s.byID) >= maxTraces {
			var oldestID string
			var oldest time.Time
			for id, candidate := range s.byID {
				if oldestID == "" || candidate.last.Before(oldest) {
					oldestID, oldest = id, candidate.last
				}
			}
			delete(s.byID, oldestID)
		}
		b = &bucket{}
		s.byID[event.TraceID] = b
	}
	if event.At.IsZero() {
		event.At = now
	}
	s.sequence++
	event.Sequence = s.sequence
	b.last = now
	if len(b.events) >= maxEventsPerTrace {
		copy(b.events, b.events[1:])
		b.events[len(b.events)-1] = event
	} else {
		b.events = append(b.events, event)
	}
}

func (s *Store) Get(id string) []Event {
	if s == nil || !ValidID(id) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(s.now().UTC())
	if b := s.byID[id]; b != nil {
		out := append([]Event(nil), b.events...)
		sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
		return out
	}
	return nil
}

func (s *Store) prune(now time.Time) {
	for id, b := range s.byID {
		if now.Sub(b.last) >= Retention {
			delete(s.byID, id)
		}
	}
}
