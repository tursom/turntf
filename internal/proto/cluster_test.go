package proto

import (
	"testing"

	gproto "google.golang.org/protobuf/proto"
)

func TestEnvelopeBatchRoundTrip(t *testing.T) {
	t.Parallel()

	batch := &EventBatch{
		Events: []*ReplicatedEvent{
			{
				EventId:       1001,
				Kind:          "user.created",
				AggregateType: "user",
				AggregateId:   2002,
				Hlc:           "1740000000000-00001-00001",
				OriginNodeId:  "node-a",
				Payload:       []byte(`{"user":{"id":2002}}`),
			},
		},
	}
	payload, err := gproto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	envelope := &Envelope{
		NodeId:      "node-a",
		Sequence:    7,
		SentAtHlc:   "1740000000000-00002-00001",
		MessageType: string(MessageTypeEventBatch),
		Payload:     payload,
	}
	data, err := gproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	var decoded Envelope
	if err := gproto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if decoded.GetNodeId() != "node-a" || decoded.GetSequence() != 7 {
		t.Fatalf("unexpected envelope: %+v", decoded)
	}

	var decodedBatch EventBatch
	if err := gproto.Unmarshal(decoded.GetPayload(), &decodedBatch); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if len(decodedBatch.GetEvents()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(decodedBatch.GetEvents()))
	}
	if decodedBatch.GetEvents()[0].GetEventId() != 1001 {
		t.Fatalf("unexpected event id: %+v", decodedBatch.GetEvents()[0])
	}
}
