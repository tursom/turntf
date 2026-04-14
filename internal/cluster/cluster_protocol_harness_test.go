package cluster

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type clusterProtocolHarness struct {
	t       *testing.T
	manager *Manager
	session *session
}

func newClusterProtocolHarness(t *testing.T) *clusterProtocolHarness {
	t.Helper()

	st := newReplicationTestStore(t, "protocol-harness", 2)
	mgr := newReplicationTestManager(t, st)
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	return &clusterProtocolHarness{
		t:       t,
		manager: mgr,
		session: sess,
	}
}

func (h *clusterProtocolHarness) Handle(envelope *internalproto.Envelope) error {
	h.t.Helper()

	switch envelope.Body.(type) {
	case *internalproto.Envelope_PullEvents:
		return h.manager.handlePullEvents(h.session, envelope)
	case *internalproto.Envelope_Ack:
		return h.manager.handleAck(h.session, envelope)
	default:
		h.t.Fatalf("unsupported protocol harness envelope: %T", envelope.Body)
		return nil
	}
}

func (h *clusterProtocolHarness) NextSent(timeout time.Duration) *internalproto.Envelope {
	h.t.Helper()

	select {
	case envelope := <-h.session.send:
		return envelope
	case <-time.After(timeout):
		h.t.Fatalf("expected protocol harness output within %s", timeout)
		return nil
	}
}

func TestProtocolHarnessDirectHandlerQueue(t *testing.T) {
	harness := newClusterProtocolHarness(t)
	user, _, err := harness.manager.store.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "protocol-harness-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create protocol harness user: %v", err)
	}

	if err := harness.Handle(&internalproto.Envelope{
		NodeId: harness.session.peerID,
		Body: &internalproto.Envelope_PullEvents{
			PullEvents: &internalproto.PullEvents{
				OriginNodeId: user.NodeID,
				AfterEventId: 0,
				Limit:        10,
				RequestId:    42,
			},
		},
	}); err != nil {
		t.Fatalf("handle pull events through protocol harness: %v", err)
	}

	envelope := harness.NextSent(time.Second)
	batch := envelope.GetEventBatch()
	if batch == nil {
		t.Fatalf("expected event batch response, got %+v", envelope)
	}
	if batch.GetPullRequestId() != 42 {
		t.Fatalf("unexpected pull request id: got=%d want=42", batch.GetPullRequestId())
	}
	if batch.GetOriginNodeId() != user.NodeID {
		t.Fatalf("unexpected origin node id: got=%d want=%d", batch.GetOriginNodeId(), user.NodeID)
	}
	if len(batch.GetEvents()) != 1 || batch.GetEvents()[0].GetUserCreated() == nil {
		t.Fatalf("unexpected protocol harness events: %+v", batch.GetEvents())
	}
}
