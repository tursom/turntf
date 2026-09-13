package mesh

import (
	"context"
	"testing"
	"time"
)

func TestTimeSyncResponseDoesNotBlockIngressAndCoalesces(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	a, b := newFakeConnPair(TransportWebSocket, "a", "b")
	slow := &stalledTopologyConn{TransportConn: a, entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer close(slow.release)
	adj := r.registerAdjacency(slow, TransportWebSocket, &NodeHello{NodeId: 2}, false)
	returned := make(chan struct{})
	go func() {
		r.handleTimeSyncRequest(context.Background(), adj, &TimeSyncRequest{RequestId: 1, ClientSendTimeMs: 100})
		close(returned)
	}()
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("response not sent")
	}
	select {
	case <-returned:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("time sync reply blocks ingress")
	}
	for id := uint64(2); id <= 100; id++ {
		r.handleTimeSyncRequest(context.Background(), adj, &TimeSyncRequest{RequestId: id, ClientSendTimeMs: int64(id)})
	}
	r.mu.Lock()
	latest := adj.pendingTimeSyncReply.RequestId
	r.mu.Unlock()
	if latest != 100 {
		t.Fatalf("pending reply is not latest: %d", latest)
	}
	// Release each of the two expected writes without allowing future writes.
	slow.release <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	raw, err := b.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.codec.Decode(raw)
	if err != nil || first.GetTimeSyncResponse().RequestId != 1 {
		t.Fatal("first response changed")
	}
	slow.release <- struct{}{}
	raw, err = b.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	latestEnv, err := r.codec.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	reply := latestEnv.GetTimeSyncResponse()
	if reply.RequestId != 100 || reply.ClientSendTimeMs != 100 || reply.ServerSendTimeMs < reply.ServerReceiveTimeMs || reply.ServerReceiveTimeMs <= 0 {
		t.Fatalf("invalid latest timestamps: %+v", reply)
	}
}
