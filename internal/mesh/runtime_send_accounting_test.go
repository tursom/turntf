package mesh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

// 不依赖真实传输的 select 顺序，以便确定性模拟成功发送与取消/关闭竞态。
type accountingResultConn struct {
	TransportConn
	send func(context.Context, []byte) error
}

func (c *accountingResultConn) Send(ctx context.Context, raw []byte) error {
	return c.send(ctx, raw)
}

type accountingOwnedConn struct{ *accountingResultConn }

func (c *accountingOwnedConn) SendOwned(ctx context.Context, raw []byte) error {
	return c.send(ctx, raw)
}

func TestEnvelopeSendAccountingBranches(t *testing.T) {
	frames := []struct {
		name        string
		envelope    *ClusterEnvelope
		activity    bool
		qualitySend bool
	}{
		{"stream", testStreamEnvelope(), true, true},
		{"forwarded", &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: &ForwardedPacket{PacketId: 1}}}, true, true},
		{"control", &ClusterEnvelope{Body: &ClusterEnvelope_PullRequest{PullRequest: &PullRequest{OriginNodeId: 2}}}, true, false},
		{"hello", &ClusterEnvelope{Body: &ClusterEnvelope_NodeHello{NodeHello: &NodeHello{NodeId: 2}}}, false, false},
		{"topology", &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: &TopologyUpdate{OriginNodeId: 2}}}, false, false},
		{"ping", &ClusterEnvelope{Body: &ClusterEnvelope_TimeSyncRequest{TimeSyncRequest: &TimeSyncRequest{RequestId: 1}}}, false, false},
		{"pong", &ClusterEnvelope{Body: &ClusterEnvelope_TimeSyncResponse{TimeSyncResponse: &TimeSyncResponse{RequestId: 1}}}, false, false},
	}
	results := []struct {
		name           string
		err            error
		cancelParent   bool
		closeRuntime   bool
		removeConn     bool
		skipAccounting bool
	}{
		{name: "success"},
		{name: "failure", err: io.ErrClosedPipe},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "send_canceled", err: fmt.Errorf("write: %w", context.Canceled), skipAccounting: true},
		{name: "parent_canceled_success", cancelParent: true, skipAccounting: true},
		{name: "parent_canceled_failure", cancelParent: true, err: io.ErrClosedPipe, skipAccounting: true},
		{name: "closed_success", closeRuntime: true, skipAccounting: true},
		{name: "closed_failure", closeRuntime: true, err: io.ErrClosedPipe, skipAccounting: true},
		{name: "removed_success", removeConn: true, skipAccounting: true},
		{name: "removed_failure", removeConn: true, err: io.ErrClosedPipe, skipAccounting: true},
	}
	for _, owned := range []bool{false, true} {
		for _, frame := range frames {
			for _, result := range results {
				t.Run(fmt.Sprintf("owned=%t/%s/%s", owned, frame.name, result.name), func(t *testing.T) {
					r := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
					defer r.Close()
					base, _ := newFakeConnPair(TransportLibP2P, "source", "peer")
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var conn TransportConn
					var sentBytes int
					stub := &accountingResultConn{TransportConn: base}
					stub.send = func(_ context.Context, raw []byte) error {
						sentBytes = len(raw)
						if result.cancelParent {
							cancel()
						}
						if result.closeRuntime {
							_ = r.Close()
						}
						if result.removeConn {
							r.mu.Lock()
							delete(r.adjByConn, conn)
							r.mu.Unlock()
						}
						return result.err
					}
					conn = stub
					if owned {
						conn = &accountingOwnedConn{stub}
					}
					adj := r.registerAdjacency(conn, TransportLibP2P, &NodeHello{NodeId: 2}, false)
					adj.lastBusinessActivity.Store(123)
					adj.activity.Store(100)
					adj.sendFailures = 2
					err := r.sendEnvelopeCtx(ctx, conn, frame.envelope, time.Second)
					if !errors.Is(err, result.err) {
						t.Fatalf("send error = %v, want %v", err, result.err)
					}
					wantActivity := frame.activity && result.err == nil && !result.removeConn
					if got := adj.lastBusinessActivity.Load() != 123; got != wantActivity {
						t.Fatalf("business activity updated = %t, want %t", got, wantActivity)
					}
					wantFailures, wantBytes := uint8(2), uint64(100)
					wantQuality := frame.qualitySend && !result.skipAccounting
					if wantQuality {
						if result.err == nil {
							wantFailures = 0
							wantBytes += uint64(sentBytes)
						} else {
							wantFailures = 3
						}
					}
					if adj.sendFailures != wantFailures || adj.activity.Load() != wantBytes || r.qualityDirty.Load() != wantQuality {
						t.Fatalf("accounting: failures=%d bytes=%d dirty=%t; want %d/%d/%t", adj.sendFailures, adj.activity.Load(), r.qualityDirty.Load(), wantFailures, wantBytes, wantQuality)
					}
					if got := len(r.qualitySignal); got != map[bool]int{false: 0, true: 1}[wantQuality] {
						t.Fatalf("quality signal count = %d", got)
					}
				})
			}
		}
	}
}
