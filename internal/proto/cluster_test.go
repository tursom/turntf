package proto

import (
	"testing"

	gproto "google.golang.org/protobuf/proto"
)

const testNodeID = int64(4096)

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
				OriginNodeId:  testNodeID,
				Payload:       []byte(`{"user":{"id":2002}}`),
			},
		},
	}
	envelope := &Envelope{
		NodeId:    testNodeID,
		Sequence:  7,
		SentAtHlc: "1740000000000-00002-00001",
		Body: &Envelope_EventBatch{
			EventBatch: batch,
		},
	}
	data, err := gproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	var decoded Envelope
	if err := gproto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if decoded.GetNodeId() != testNodeID || decoded.GetSequence() != 7 {
		t.Fatalf("unexpected envelope: %+v", decoded)
	}

	decodedBatch := decoded.GetEventBatch()
	if decodedBatch == nil {
		t.Fatalf("expected event batch body")
	}
	if len(decodedBatch.GetEvents()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(decodedBatch.GetEvents()))
	}
	if decodedBatch.GetEvents()[0].GetEventId() != 1001 {
		t.Fatalf("unexpected event id: %+v", decodedBatch.GetEvents()[0])
	}
}

func TestSnapshotMessageRowRoundTripUsesTripleIdentity(t *testing.T) {
	t.Parallel()

	envelope := &Envelope{
		NodeId: testNodeID,
		Body: &Envelope_SnapshotChunk{
			SnapshotChunk: &SnapshotChunk{
				SnapshotVersion: SnapshotVersion,
				Partition:       "messages/4096",
				Kind:            SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES,
				Rows: []*SnapshotRow{
					{
						Body: &SnapshotRow_Message{
							Message: &SnapshotMessageRow{
								UserId:       42,
								NodeId:       testNodeID,
								Seq:          7,
								Sender:       "orders",
								Body:         "hello",
								CreatedAtHlc: "1740000000000-00001-00001",
							},
						},
					},
				},
			},
		},
	}

	data, err := gproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	var decoded Envelope
	if err := gproto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}

	message := decoded.GetSnapshotChunk().GetRows()[0].GetMessage()
	if message.GetUserId() != 42 || message.GetNodeId() != testNodeID || message.GetSeq() != 7 {
		t.Fatalf("unexpected snapshot message identity: %+v", message)
	}
}
