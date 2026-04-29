package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

const clusterBenchmarkWarmupPasses = 1

func BenchmarkMeshReplicationPebbleLinear3Nodes(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, tc := range []struct {
				name        string
				payloadSize int
			}{
				{name: "256B", payloadSize: 256},
				{name: "4KiB", payloadSize: 4 << 10},
				{name: "16KiB", payloadSize: 16 << 10},
			} {
				b.Run(tc.name, func(b *testing.B) {
					benchmarkMeshReplicationPebbleLinear(b, mode, tc.payloadSize)
				})
			}
		})
	}
}

func BenchmarkMeshQueryLoggedInUsersPebbleLinear(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, nodeCount := range []int{3, 7} {
				for _, userCount := range []int{1, 100} {
					b.Run(fmt.Sprintf("%d-nodes/%d-users", nodeCount, userCount), func(b *testing.B) {
						benchmarkMeshQueryLoggedInUsersPebbleLinear(b, mode, nodeCount, userCount)
					})
				}
			}
		})
	}
}

func BenchmarkMeshTransientRoutePebbleLinear(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, nodeCount := range []int{3, 7} {
				for _, tc := range []struct {
					name        string
					payloadSize int
				}{
					{name: "256B", payloadSize: 256},
					{name: "4KiB", payloadSize: 4 << 10},
				} {
					b.Run(fmt.Sprintf("%d-nodes/%s", nodeCount, tc.name), func(b *testing.B) {
						benchmarkMeshTransientRoutePebbleLinear(b, mode, nodeCount, tc.payloadSize)
					})
				}
			}
		})
	}
}

func benchmarkMeshReplicationPebbleLinear(b *testing.B, mode benchroot.Mode, payloadSize int) {
	ackTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	ctx := context.Background()
	b.StopTimer()
	silenceClusterLogs(b)
	managers := startLinearWebSocketManagersWithStoreFactory(b, 3, newBenchmarkPebbleLinearStoreFactory(mode))
	source := managers[0]
	target := managers[2]
	targetNodeID := testNodeID(3)

	waitForMeshRoute(b, source, targetNodeID, mesh.TrafficReplicationStream)
	waitForMeshRoute(b, target, testNodeID(1), mesh.TrafficControlCritical)

	recipient, userEvent, err := source.store.CreateUser(ctx, store.CreateUserParams{
		Username:     "bench-recipient",
		PasswordHash: "bench-hash",
	})
	if err != nil {
		b.Fatalf("create benchmark recipient: %v", err)
	}
	source.Publish(userEvent)

	waitForBenchmark(b, ackTimeout, func() bool {
		replicated, err := target.store.GetUser(ctx, recipient.Key())
		return err == nil && replicated.Username == recipient.Username
	})
	waitForBenchmark(b, ackTimeout, func() bool {
		cursor, err := source.store.GetPeerAckCursor(ctx, targetNodeID, testNodeID(1))
		return err == nil && cursor.AckedEventID >= userEvent.EventID
	})

	payload := bytes.Repeat([]byte("m"), payloadSize)
	runClusterBenchmarkWarmup(b, func() {
		_ = mustBenchmarkMeshReplicationOnce(b, ctx, source, target, recipient.Key(), targetNodeID, payload, ackTimeout)
	})
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()

	var totalAck time.Duration
	for i := 0; i < b.N; i++ {
		totalAck += mustBenchmarkMeshReplicationOnce(b, ctx, source, target, recipient.Key(), targetNodeID, payload, ackTimeout)
	}

	b.StopTimer()
	reportAverageLatencyMetric(b, totalAck, "ack_ms/op")
	b.ReportMetric(float64(payloadSize), "bytes/op")
}

func benchmarkMeshQueryLoggedInUsersPebbleLinear(b *testing.B, mode benchroot.Mode, nodeCount, userCount int) {
	ctx := context.Background()
	targetNodeID := testNodeID(uint16(nodeCount))

	b.StopTimer()
	silenceClusterLogs(b)
	managers := startLinearWebSocketManagersWithStoreFactory(b, nodeCount, newBenchmarkPebbleLinearStoreFactory(mode))
	source := managers[0]
	target := managers[nodeCount-1]
	expected := benchmarkLoggedInUsers(targetNodeID, userCount)
	target.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return expected, nil
	})

	waitForMeshRouteDecision(b, source, targetNodeID, mesh.TrafficControlQuery, testNodeID(2), mesh.TransportWebSocket)
	waitForMeshRouteDecision(b, target, testNodeID(1), mesh.TrafficControlQuery, testNodeID(uint16(nodeCount-1)), mesh.TransportWebSocket)

	runClusterBenchmarkWarmup(b, func() {
		_ = mustBenchmarkMeshQueryLoggedInUsersOnce(b, ctx, source, targetNodeID, expected, userCount)
	})
	b.ResetTimer()
	b.StartTimer()

	var totalQuery time.Duration
	for i := 0; i < b.N; i++ {
		totalQuery += mustBenchmarkMeshQueryLoggedInUsersOnce(b, ctx, source, targetNodeID, expected, userCount)
	}

	b.StopTimer()
	reportAverageLatencyMetric(b, totalQuery, "query_ms/op")
}

func benchmarkMeshTransientRoutePebbleLinear(b *testing.B, mode benchroot.Mode, nodeCount, payloadSize int) {
	deliveryTimeout := benchmarkTimeout(mode, 5*time.Second, 30*time.Second)

	ctx := context.Background()
	targetNodeID := testNodeID(uint16(nodeCount))
	expectedTTL := int32(defaultPacketTTLHops - (nodeCount - 1))

	b.StopTimer()
	silenceClusterLogs(b)
	managers := startLinearWebSocketManagersWithStoreFactory(b, nodeCount, newBenchmarkPebbleLinearStoreFactory(mode))
	source := managers[0]
	target := managers[nodeCount-1]
	delivered := make(chan store.TransientPacket, 1)
	target.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	waitForMeshRouteDecision(b, source, targetNodeID, mesh.TrafficTransientInteractive, testNodeID(2), mesh.TransportWebSocket)

		payload := bytes.Repeat([]byte("t"), payloadSize)
		runClusterBenchmarkWarmup(b, func() {
			warmupPacket := store.TransientPacket{
				PacketID:     uint64(1) << 62,
				SourceNodeID: testNodeID(1),
				TargetNodeID: targetNodeID,
				Recipient:    store.UserKey{NodeID: targetNodeID, UserID: 99},
			Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
			Body:         payload,
			DeliveryMode: store.DeliveryModeBestEffort,
			TTLHops:      defaultPacketTTLHops,
		}
		_ = mustBenchmarkMeshTransientRouteOnce(b, ctx, source, delivered, warmupPacket, payload, expectedTTL, deliveryTimeout)
	})
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.StartTimer()

	var totalDelivery time.Duration
	for i := 0; i < b.N; i++ {
		packet := store.TransientPacket{
			PacketID:     uint64(i + 1),
			SourceNodeID: testNodeID(1),
			TargetNodeID: targetNodeID,
			Recipient:    store.UserKey{NodeID: targetNodeID, UserID: 99},
			Sender:       store.UserKey{NodeID: testNodeID(1), UserID: 100},
			Body:         payload,
			DeliveryMode: store.DeliveryModeBestEffort,
			TTLHops:      defaultPacketTTLHops,
		}

		totalDelivery += mustBenchmarkMeshTransientRouteOnce(b, ctx, source, delivered, packet, payload, expectedTTL, deliveryTimeout)
	}

	b.StopTimer()
	reportAverageLatencyMetric(b, totalDelivery, "delivery_ms/op")
	b.ReportMetric(float64(payloadSize), "bytes/op")
}

func newBenchmarkPebbleLinearStoreFactory(mode benchroot.Mode) linearWebSocketStoreFactory {
	return func(tb testing.TB, nodeID string, slot uint16) *store.Store {
		tb.Helper()

		st, closeStore := openBenchmarkClusterStore(tb, mode, nodeID, slot, store.DefaultMessageWindowSize, store.DefaultEventLogMaxEventsPerOrigin)
		tb.Cleanup(closeStore)
		return st
	}
}

func benchmarkTimeout(mode benchroot.Mode, tmpTimeout, diskTimeout time.Duration) time.Duration {
	if mode.Name() == benchroot.ModeDisk {
		return diskTimeout
	}
	return tmpTimeout
}

func runClusterBenchmarkWarmup(tb testing.TB, fn func()) {
	tb.Helper()
	for i := 0; i < clusterBenchmarkWarmupPasses; i++ {
		fn()
	}
}

func mustBenchmarkMeshReplicationOnce(tb testing.TB, ctx context.Context, source, target *Manager, recipient store.UserKey, targetNodeID int64, payload []byte, ackTimeout time.Duration) time.Duration {
	tb.Helper()

	start := time.Now()
	message, event, err := source.store.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: recipient,
		Sender:  recipient,
		Body:    payload,
	})
	if err != nil {
		tb.Fatalf("create benchmark message: %v", err)
	}
	source.Publish(event)

	waitForBenchmark(tb, ackTimeout, func() bool {
		messages, err := target.store.ListMessagesByUser(ctx, recipient, 1)
		return err == nil &&
			len(messages) == 1 &&
			messages[0].Recipient == recipient &&
			messages[0].NodeID == message.NodeID &&
			messages[0].Seq == message.Seq &&
			bytes.Equal(messages[0].Body, payload)
	})
	waitForBenchmark(tb, ackTimeout, func() bool {
		cursor, err := source.store.GetPeerAckCursor(ctx, targetNodeID, testNodeID(1))
		return err == nil && cursor.AckedEventID >= event.EventID
	})
	return time.Since(start)
}

func mustBenchmarkMeshQueryLoggedInUsersOnce(tb testing.TB, ctx context.Context, source *Manager, targetNodeID int64, expected []app.LoggedInUserSummary, userCount int) time.Duration {
	tb.Helper()

	start := time.Now()
	users, err := source.QueryLoggedInUsers(ctx, targetNodeID)
	if err != nil {
		tb.Fatalf("query logged-in users: %v", err)
	}
	if len(users) != userCount {
		tb.Fatalf("unexpected logged-in user count: got=%d want=%d", len(users), userCount)
	}
	if len(users) > 0 {
		if users[0] != expected[0] || users[len(users)-1] != expected[len(expected)-1] {
			tb.Fatalf("unexpected logged-in users payload: first=%+v last=%+v", users[0], users[len(users)-1])
		}
	}
	return time.Since(start)
}

func mustBenchmarkMeshTransientRouteOnce(tb testing.TB, ctx context.Context, source *Manager, delivered <-chan store.TransientPacket, packet store.TransientPacket, payload []byte, expectedTTL int32, deliveryTimeout time.Duration) time.Duration {
	tb.Helper()

	start := time.Now()
	if err := source.RouteTransientPacket(ctx, packet); err != nil {
		tb.Fatalf("route transient packet: %v", err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID ||
			got.SourceNodeID != packet.SourceNodeID ||
			got.TargetNodeID != packet.TargetNodeID ||
			got.Recipient != packet.Recipient ||
			got.Sender != packet.Sender ||
			got.TTLHops != expectedTTL ||
			!bytes.Equal(got.Body, payload) {
			tb.Fatalf("unexpected delivered transient packet: %+v", got)
		}
	case <-time.After(deliveryTimeout):
		tb.Fatalf("timed out waiting for transient delivery")
	}
	return time.Since(start)
}

func benchmarkLoggedInUsers(nodeID int64, count int) []app.LoggedInUserSummary {
	users := make([]app.LoggedInUserSummary, 0, count)
	for i := 0; i < count; i++ {
		users = append(users, app.LoggedInUserSummary{
			NodeID:   nodeID,
			UserID:   int64(1000 + i),
			Username: fmt.Sprintf("bench-user-%03d", i),
		})
	}
	return users
}

func waitForBenchmark(tb testing.TB, timeout time.Duration, fn func() bool) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !fn() {
		tb.Fatalf("benchmark condition not met within %s", timeout)
	}
}

func reportAverageLatencyMetric(b *testing.B, total time.Duration, unit string) {
	if b.N <= 0 {
		return
	}
	b.ReportMetric(float64(total)/float64(time.Millisecond)/float64(b.N), unit)
}
