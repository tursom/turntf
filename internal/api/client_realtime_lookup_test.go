package api

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func queueSessionLookup(t *testing.T, s *clientWSSession, c *realtimeTestConn, id uint64) {
	t.Helper()
	queueRealtimeEnvelope(t, c, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_ResolveUserSessions{ResolveUserSessions: &internalproto.ResolveUserSessionsRequest{RequestId: id, User: &internalproto.UserRef{NodeId: s.principal.User.NodeID, UserId: s.principal.User.ID}}}})
}

func TestRealtimeLookupAndDataDoNotBlockEachOther(t *testing.T) {
	for _, firstLookup := range []bool{false, true} {
		t.Run(map[bool]string{false: "data_first", true: "lookup_first"}[firstLookup], func(t *testing.T) {
			s, c, r := realtimeFixture(t, true)
			startRealtimeLoop(t, s, c)
			if firstLookup {
				queueSessionLookup(t, s, c, 1)
			} else {
				enqueueRealtime(t, s, c, r, 1)
			}
			select {
			case <-r.entered:
			case <-time.After(time.Second):
				t.Fatal("first request not started")
			}
			if firstLookup {
				enqueueRealtime(t, s, c, r, 2)
			} else {
				queueSessionLookup(t, s, c, 2)
			}
			select {
			case <-r.entered:
			case <-time.After(time.Second):
				t.Fatal("independent request blocked by earlier RPC")
			}
			close(r.release)
			ids := map[uint64]bool{}
			for i := 0; i < 2; i++ {
				e := nextRealtimeResponse(t, c)
				id := e.GetSendMessageResponse().GetRequestId()
				if e.GetResolveUserSessionsResponse() != nil {
					id = e.GetResolveUserSessionsResponse().GetRequestId()
				}
				ids[id] = true
			}
			if !ids[1] || !ids[2] {
				t.Fatalf("response correlation lost: %v", ids)
			}
		})
	}
}

func TestRealtimeLookupSharesBoundAndCancels(t *testing.T) {
	s, c, r := realtimeFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.readLoop(ctx) }()
	for i := 0; i < clientRealtimeSendConcurrency+1; i++ {
		if i%2 == 0 {
			queueSessionLookup(t, s, c, uint64(i+1))
		} else {
			enqueueRealtime(t, s, c, r, uint64(i+1))
		}
	}
	for i := 0; i < clientRealtimeSendConcurrency; i++ {
		select {
		case <-r.entered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("mixed RPC concurrency not reached")
		}
	}
	select {
	case <-r.entered:
		cancel()
		t.Fatal("shared concurrency limit exceeded")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation stranded lookup workers")
	}
}

func TestOrdinarySessionLookupRemainsSequential(t *testing.T) {
	s, c, r := realtimeFixture(t, false)
	startRealtimeLoop(t, s, c)
	queueSessionLookup(t, s, c, 1)
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("lookup not started")
	}
	queueSessionLookup(t, s, c, 2)
	select {
	case <-r.entered:
		t.Fatal("ordinary connection became concurrent")
	case <-time.After(30 * time.Millisecond):
	}
	close(r.release)
	for id := uint64(1); id <= 2; id++ {
		if got := nextRealtimeResponse(t, c).GetResolveUserSessionsResponse().GetRequestId(); got != id {
			t.Fatalf("response %d, want %d", got, id)
		}
	}
}
