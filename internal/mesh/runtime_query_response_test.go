package mesh

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockedQueryResponseConn struct {
	TransportConn
	codec   EnvelopeCodec
	entered chan struct{}
	release chan struct{}
}

func (c *blockedQueryResponseConn) Send(ctx context.Context, p []byte) error {
	env, err := c.codec.Decode(p)
	if err == nil && env.GetForwardedPacket() != nil {
		inner, err := c.codec.Decode(env.GetForwardedPacket().Payload)
		if err == nil && inner.GetQueryResponse() != nil {
			select {
			case c.entered <- struct{}{}:
			default:
			}
			select {
			case <-c.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return c.TransportConn.Send(ctx, p)
}
func TestSlowQueryResponseDoesNotBlockNextInboundQuery(t *testing.T) {
	a, b := newFakeAdapter(TransportWebSocket), newFakeAdapter(TransportWebSocket)
	handled := make(chan struct{}, 2)
	delivered := make(chan struct{}, 2)
	source := newTestRuntime(t, 1, a, func(o *RuntimeOptions) {
		o.QueryHandler = func(_ context.Context, _ *ForwardedPacket, e *ClusterEnvelope) error {
			if e.GetQueryResponse() != nil {
				delivered <- struct{}{}
			}
			return nil
		}
	})
	var target *Runtime
	target = newTestRuntime(t, 2, b, func(o *RuntimeOptions) {
		o.QueryHandler = func(ctx context.Context, p *ForwardedPacket, e *ClusterEnvelope) error {
			handled <- struct{}{}
			return target.RouteQueryResponse(ctx, p.SourceNodeId, &ClusterEnvelope{Body: &ClusterEnvelope_QueryResponse{QueryResponse: &QueryResponse{RequestId: e.GetQueryRequest().RequestId, Payload: []byte("snapshot")}}})
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, r := range []*Runtime{source, target} {
		if err := r.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer r.Close()
	}
	ab, ba := newFakeConnPair(TransportWebSocket, "a", "b")
	blocked := &blockedQueryResponseConn{TransportConn: ba, codec: target.codec, entered: make(chan struct{}, 2), release: make(chan struct{})}
	defer close(blocked.release)
	a.accept <- ab
	b.accept <- blocked
	waitForNodes(t, source, []int64{1, 2}, time.Second)
	waitForNodes(t, target, []int64{1, 2}, time.Second)
	for id := uint64(1); id <= 2; id++ {
		if err := source.RouteEnvelope(ctx, 2, &ClusterEnvelope{Body: &ClusterEnvelope_QueryRequest{QueryRequest: &QueryRequest{RequestId: id, Kind: "test"}}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-handled:
		case <-time.After(150 * time.Millisecond):
			t.Fatal("outbound response blocks later inbound query")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-blocked.entered:
		case <-time.After(time.Second):
			t.Fatal("response not dispatched")
		}
	}
	// Release I/O and require both replies to reach the origin.
	blocked.release <- struct{}{}
	blocked.release <- struct{}{}
	for i := 0; i < 2; i++ {
		select {
		case <-delivered:
		case <-time.After(time.Second):
			t.Fatal("admitted response not delivered")
		}
	}
}

func TestQueryResponseAdmissionBoundAndCancellation(t *testing.T) {
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket))
	defer r.Close()
	r.queryResponseSlots = make(chan struct{}, 1)
	r.queryResponseSlots <- struct{}{}
	env := &ClusterEnvelope{Body: &ClusterEnvelope_QueryResponse{QueryResponse: &QueryResponse{RequestId: 1}}}
	if err := r.RouteQueryResponse(context.Background(), 2, env); !errors.Is(err, ErrQueryResponseBusy) {
		t.Fatalf("unbounded admission: %v", err)
	}
	<-r.queryResponseSlots
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.RouteQueryResponse(ctx, 2, env); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query admitted: %v", err)
	}
	if len(r.queryResponseSlots) != 0 {
		t.Fatal("canceled request leaked a slot")
	}
}

func TestQueryResponseWorkerStopsWithRuntime(t *testing.T) {
	entered := make(chan struct{}, 1)
	r := newTestRuntime(t, 1, newFakeAdapter(TransportWebSocket), func(o *RuntimeOptions) {
		o.QueryHandler = func(ctx context.Context, _ *ForwardedPacket, _ *ClusterEnvelope) error {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	env := &ClusterEnvelope{Body: &ClusterEnvelope_QueryResponse{QueryResponse: &QueryResponse{RequestId: 1}}}
	if err := r.RouteQueryResponse(context.Background(), 1, env); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("response worker not started")
	}
	done := make(chan struct{})
	go func() { _ = r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runtime close stranded response worker")
	}
	if len(r.queryResponseSlots) != 0 {
		t.Fatal("closed worker leaked response slot")
	}
	if err := r.RouteQueryResponse(context.Background(), 1, env); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("closed runtime admitted work: %v", err)
	}
}
