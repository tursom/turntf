package mesh

import (
	"math"
	"testing"
	"time"
)

func TestTimeSyncObserverRejectsImpossibleProcessingTime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		t2, t3 int64
		want   int
	}{
		{"within_round_trip", 10000, 10002, 1},
		{"millisecond_quantization", 10000, 10003, 1},
		{"beyond_tolerance", 10000, 10004, 0},
		{"symmetric_forgery", 1, 20001, 0},
		{"extreme_processing", 1, math.MaxInt64, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.UnixMilli(10000)
			calls := 0
			runtime := &Runtime{now: func() time.Time { return start.Add(2 * time.Millisecond) }, timeSyncObserver: func(TimeSyncObservation) { calls++ }}
			adj := &Adjacency{RemoteNodeID: 2, inflightPings: map[uint64]time.Time{7: start}, rttEWMA: 2, samples: 1}
			response := &TimeSyncResponse{RequestId: 7, ClientSendTimeMs: 10000, ServerReceiveTimeMs: tc.t2, ServerSendTimeMs: tc.t3}
			runtime.handleTimeSyncResponse(adj, response)
			runtime.handleTimeSyncResponse(adj, response)
			if calls != tc.want || adj.samples != 1+tc.want {
				t.Fatalf("observer calls=%d samples=%d want accepted=%d", calls, adj.samples, tc.want)
			}
		})
	}
}
func TestTimeSyncObserverRequiresMatchedTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name       string
		id         uint64
		t1, t2, t3 int64
		want       int
	}{
		{"valid", 7, 10000, 10000, 10000, 1},
		{"unknown_request", 8, 10000, 10000, 10000, 0},
		{"rewritten_client_time", 7, 9000, 10000, 10000, 0},
		{"missing_server_time", 7, 10000, 0, 10000, 0},
		{"backward_server_time", 7, 10000, 10000, 9999, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.UnixMilli(10000)
			calls := 0
			runtime := &Runtime{now: func() time.Time { return now }, timeSyncObserver: func(o TimeSyncObservation) {
				calls++
				if o.ClientSendTimeMs != 10000 {
					t.Fatal("unmatched T1")
				}
			}}
			adj := &Adjacency{RemoteNodeID: 2, inflightPings: map[uint64]time.Time{7: now}}
			response := &TimeSyncResponse{RequestId: tc.id, ClientSendTimeMs: tc.t1, ServerReceiveTimeMs: tc.t2, ServerSendTimeMs: tc.t3}
			runtime.handleTimeSyncResponse(adj, response)
			runtime.handleTimeSyncResponse(adj, response)
			if calls != tc.want {
				t.Fatalf("observer calls=%d want=%d", calls, tc.want)
			}
		})
	}
}
