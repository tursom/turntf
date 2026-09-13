package mesh

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
)

var ErrQueryResponseBusy = errors.New("mesh: query response send budget exhausted")

// RouteQueryResponse validates and admits a response without blocking the
// ingress reader on outbound I/O. Query evaluation stays synchronous at the
// caller. Admission is not delivery confirmation; the origin's existing query
// deadline remains authoritative if the asynchronous send fails.
func (r *Runtime) RouteQueryResponse(ctx context.Context, targetNodeID int64, envelope *ClusterEnvelope) error {
	if r == nil {
		return ErrRuntimeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if targetNodeID <= 0 || envelope == nil || envelope.GetQueryResponse() == nil {
		return errors.New("mesh: invalid query response")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	if r.queryResponseSlots == nil {
		r.queryResponseSlots = make(chan struct{}, 128)
	}
	select {
	case r.queryResponseSlots <- struct{}{}:
	default:
		r.mu.Unlock()
		return ErrQueryResponseBusy
	}
	lifetime := r.ctx
	if lifetime == nil {
		lifetime = ctx
	}
	r.wg.Add(1)
	r.mu.Unlock()
	owned := proto.Clone(envelope).(*ClusterEnvelope)
	sendCtx, cancel := context.WithTimeout(lifetime, r.helloTimeout)
	stop := context.AfterFunc(ctx, cancel)
	go func() {
		defer r.wg.Done()
		defer func() { <-r.queryResponseSlots }()
		defer cancel()
		defer stop()
		if err := r.RouteEnvelope(sendCtx, targetNodeID, owned); err != nil && r.forwardingObserver != nil {
			r.forwardingObserver(ForwardingObservation{TrafficClass: TrafficControlQuery, SourceNodeID: r.localNodeID, TargetNodeID: targetNodeID, DropReason: "query_response_send_failed"})
		}
	}()
	return nil
}
