package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
)

func TestMeshConsensusDirectRoundTrip(t *testing.T) {
	managers := startLinearWebSocketManagers(t, 2)
	a, b := managers[0], managers[1]
	waitForMeshRoute(t, a, b.cfg.NodeID, mesh.TrafficConsensus)
	waitForMeshRoute(t, b, a.cfg.NodeID, mesh.TrafficConsensus)

	type received struct {
		source int64
		group  string
		body   string
	}
	requests := make(chan received, 1)
	responses := make(chan received, 1)
	b.SetConsensusMessageHandler(func(_ context.Context, source int64, message *mesh.ConsensusMessage) error {
		requests <- received{source, message.GetGroupId(), string(message.GetPayload())}
		return nil
	})
	a.SetConsensusMessageHandler(func(_ context.Context, source int64, message *mesh.ConsensusMessage) error {
		responses <- received{source, message.GetGroupId(), string(message.GetPayload())}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.SendConsensusMessage(ctx, b.cfg.NodeID, "kv-test", 1, []byte("request")); err != nil {
		t.Fatalf("send direct consensus request: %v", err)
	}
	select {
	case got := <-requests:
		if got != (received{a.cfg.NodeID, "kv-test", "request"}) {
			t.Fatalf("unexpected consensus request: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("direct consensus request was not delivered")
	}

	if err := b.SendConsensusMessage(ctx, a.cfg.NodeID, "kv-test", 1, []byte("response")); err != nil {
		t.Fatalf("send direct consensus response: %v", err)
	}
	select {
	case got := <-responses:
		if got != (received{b.cfg.NodeID, "kv-test", "response"}) {
			t.Fatalf("unexpected consensus response: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("direct consensus response was not delivered")
	}
}

func TestMeshConsensusAcrossTransit(t *testing.T) {
	a, _, c := startLinearMeshManagers(t)
	waitForMeshRoute(t, a, c.cfg.NodeID, mesh.TrafficConsensus)

	delivered := make(chan *mesh.ConsensusMessage, 1)
	c.SetConsensusMessageHandler(func(_ context.Context, source int64, message *mesh.ConsensusMessage) error {
		if source != a.cfg.NodeID {
			t.Errorf("unexpected source node: %d", source)
		}
		delivered <- message
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.SendConsensusMessage(ctx, c.cfg.NodeID, "kv-test", 2, []byte("transit")); err != nil {
		t.Fatalf("send transit consensus: %v", err)
	}
	select {
	case got := <-delivered:
		if got.GetGroupId() != "kv-test" || got.GetTargetNodeId() != c.cfg.NodeID || string(got.GetPayload()) != "transit" {
			t.Fatalf("unexpected transit consensus message: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("transit consensus message was not delivered")
	}
}
