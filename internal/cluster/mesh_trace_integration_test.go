package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/trace"
)

func hasTraceStage(events []trace.Event, stage string) bool {
	for _, event := range events {
		if event.Stage == stage {
			return true
		}
	}
	return false
}

func TestMeshTransientTraceAcrossThreeNodes(t *testing.T) {
	mgrA, mgrB, mgrC := startLinearMeshManagers(t)
	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficTransientInteractive)
	id, err := trace.NewID()
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan store.TransientPacket, 1)
	mgrC.SetTransientHandler(func(packet store.TransientPacket) bool {
		received <- packet
		return true
	})
	packet := store.TransientPacket{PacketID: 91, SourceNodeID: testNodeID(1), TargetNodeID: testNodeID(3),
		Recipient: store.UserKey{NodeID: testNodeID(3), UserID: 100}, Sender: store.UserKey{NodeID: testNodeID(1), UserID: 101},
		Body: []byte("only packet content"), TraceID: id, TTLHops: 8}
	if err := mgrA.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-received:
		if result.TraceID != id || result.PacketID != packet.PacketID {
			t.Fatalf("trace identity lost: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("target did not receive traced transient packet")
	}
	waitFor(t, 5*time.Second, func() bool {
		return hasTraceStage(mgrA.traceStore.Get(id), "forwarded") &&
			hasTraceStage(mgrB.traceStore.Get(id), "forwarded") &&
			hasTraceStage(mgrC.traceStore.Get(id), "session_queued")
	})
}

func TestMeshPersistentTraceInReplicationBatch(t *testing.T) {
	mgrA, mgrB, mgrC := startLinearMeshManagers(t)
	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficReplicationStream)
	user, event, err := mgrA.store.CreateUser(context.Background(), store.CreateUserParams{Username: "traced-user", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	mgrA.Publish(event)
	waitFor(t, 5*time.Second, func() bool {
		_, err := mgrC.store.GetUser(context.Background(), user.Key())
		return err == nil
	})
	id, err := trace.NewID()
	if err != nil {
		t.Fatal(err)
	}
	message, created, err := mgrA.store.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: user.Key(), Sender: user.Key(), Body: []byte("not in trace"), TraceID: id,
	})
	if err != nil || created.Body.(*internalproto.MessageCreatedEvent).GetTraceId() != id {
		t.Fatalf("message trace not persisted: %v / %+v", err, created)
	}
	mgrA.Publish(created)
	waitFor(t, 5*time.Second, func() bool {
		return hasTraceStage(mgrB.traceStore.Get(id), "forwarded") &&
			hasTraceStage(mgrC.traceStore.Get(id), "replica_event_accepted")
	})
	for _, entry := range mgrC.traceStore.Get(id) {
		if entry.Stage == "replica_event_accepted" && (entry.EventID != created.EventID || entry.MessageSeq != message.Seq) {
			t.Fatalf("wrong replication correlation: %+v", entry)
		}
	}
}
