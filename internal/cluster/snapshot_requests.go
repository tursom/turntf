package cluster

import (
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

const (
	snapshotRequestTimeout = 30 * time.Second
	snapshotRetryBase      = time.Second
	snapshotRetryMax       = 30 * time.Second
)

// One entry per partition retains deduplication during both response waiting
// and retry backoff. Attempt tokens prevent a late send failure from changing
// a newer request. The wire format has no request ID: any valid full response
// can complete its partition, whose application must therefore remain monotone.
type snapshotRequestState struct {
	kind     internalproto.SnapshotPartitionKind
	attempt  uint64
	token    uint64
	inFlight bool
	retryAt  time.Time
}

type snapshotRequestAttempt struct {
	partition string
	kind      internalproto.SnapshotPartitionKind
	token     uint64
}

func snapshotRetryDelay(attempt uint64) time.Duration {
	delay := snapshotRetryBase
	for i := uint64(1); i < attempt && delay < snapshotRetryMax; i++ {
		delay *= 2
	}
	if delay > snapshotRetryMax {
		return snapshotRetryMax
	}
	return delay
}

func (s *session) beginSnapshotRequestAt(partition string, kind internalproto.SnapshotPartitionKind, now time.Time) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.pendingSnapshotParts[partition] != nil {
		return 0, false
	}
	if s.pendingSnapshotParts == nil {
		s.pendingSnapshotParts = make(map[string]*snapshotRequestState)
	}
	s.nextSnapshotRequestID++
	s.pendingSnapshotParts[partition] = &snapshotRequestState{kind: kind, attempt: 1, token: s.nextSnapshotRequestID, inFlight: true, retryAt: now.Add(snapshotRequestTimeout)}
	return s.nextSnapshotRequestID, true
}

func (s *session) snapshotRequestToken(partition string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.pendingSnapshotParts[partition]; state != nil {
		return state.token
	}
	return 0
}

func (s *session) failSnapshotRequest(partition string, token uint64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.pendingSnapshotParts[partition]; state != nil && state.token == token && state.inFlight {
		state.inFlight = false
		state.retryAt = now.Add(snapshotRetryDelay(state.attempt))
	}
}

func (s *session) hasSnapshotRequests() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingSnapshotParts) > 0
}

// takeSnapshotRetries reserves due retries under the session lock, but sends
// outside it. A timeout first enters backoff so repeated digests cannot turn a
// dropped response into a tight full-snapshot retransmission loop.
func (s *session) takeSnapshotRetries(now time.Time) ([]snapshotRequestAttempt, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0
	}
	var due []snapshotRequestAttempt
	expired := 0
	for partition, state := range s.pendingSnapshotParts {
		if now.Before(state.retryAt) {
			continue
		}
		if state.inFlight {
			state.inFlight = false
			state.retryAt = now.Add(snapshotRetryDelay(state.attempt))
			expired++
			continue
		}
		state.attempt++
		s.nextSnapshotRequestID++
		state.token = s.nextSnapshotRequestID
		state.inFlight = true
		state.retryAt = now.Add(snapshotRequestTimeout)
		due = append(due, snapshotRequestAttempt{partition: partition, kind: state.kind, token: state.token})
	}
	return due, expired
}
