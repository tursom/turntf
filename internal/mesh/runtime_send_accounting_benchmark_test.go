package mesh

import (
	"context"
	"io"
	"testing"
	"time"
)

// 本文件可原样复制到优化前工作树，仅使用既有 runtime 入口。
// 仅测编码、签名（默认 noop）、超时上下文及发送后记账，不包含真实 I/O。
// serial 用于观察无争用开销；parallel 让多个发送者争用同一 runtime.mu。
// 每个发送者使用同一个只读 envelope，丢弃连接无锁，避免掩盖 runtime 争用。
type sendAccountingBenchmarkConn struct{}

func (sendAccountingBenchmarkConn) Send(context.Context, []byte) error      { return nil }
func (sendAccountingBenchmarkConn) SendOwned(context.Context, []byte) error { return nil }
func (sendAccountingBenchmarkConn) Receive(context.Context) ([]byte, error) {
	return nil, io.EOF
}
func (sendAccountingBenchmarkConn) Close() error             { return nil }
func (sendAccountingBenchmarkConn) RemoteNodeHint() string   { return "send-accounting-bench" }
func (sendAccountingBenchmarkConn) Transport() TransportKind { return TransportLibP2P }

func BenchmarkMeshSendAccounting(b *testing.B) {
	frames := []struct {
		name     string
		envelope *ClusterEnvelope
	}{
		{"stream", &ClusterEnvelope{Body: &ClusterEnvelope_StreamFrame{StreamFrame: &StreamFrame{Payload: make([]byte, 256)}}}},
		{"forwarded", &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: &ForwardedPacket{PacketId: 1, Payload: make([]byte, 256)}}}},
		{"control", &ClusterEnvelope{Body: &ClusterEnvelope_PullRequest{PullRequest: &PullRequest{OriginNodeId: 2}}}},
		{"measurement", &ClusterEnvelope{Body: &ClusterEnvelope_TimeSyncRequest{TimeSyncRequest: &TimeSyncRequest{RequestId: 1}}}},
	}
	for _, frame := range frames {
		for _, parallel := range []bool{false, true} {
			mode := "serial"
			if parallel {
				mode = "parallel"
			}
			b.Run(frame.name+"/"+mode, func(b *testing.B) {
				r := newTestRuntime(b, 1, newFakeAdapter(TransportLibP2P))
				defer r.Close()
				conn := sendAccountingBenchmarkConn{}
				r.registerAdjacency(conn, TransportLibP2P, &NodeHello{NodeId: 2}, false)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if err := r.sendEnvelopeCtx(ctx, conn, frame.envelope, time.Second); err != nil {
								b.Error(err)
								return
							}
						}
					})
				} else {
					for i := 0; i < b.N; i++ {
						if err := r.sendEnvelopeCtx(ctx, conn, frame.envelope, time.Second); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
			})
		}
	}
}
