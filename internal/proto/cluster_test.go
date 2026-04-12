package proto

import (
	"testing"

	gproto "google.golang.org/protobuf/proto"
)

const testNodeID = int64(4096)

func TestEnvelopeBatchRoundTrip(t *testing.T) {
	t.Parallel()

	event := &ReplicatedEvent{
		EventId:       1001,
		AggregateType: "user",
		AggregateId:   2002,
		Hlc:           "1740000000000-00001-0000000000000004096",
		OriginNodeId:  testNodeID,
	}
	if err := event.SetTypedBody(&UserCreatedEvent{
		NodeId:              testNodeID,
		UserId:              2002,
		Username:            "alice",
		PasswordHash:        "hash",
		Profile:             "{}",
		Role:                "user",
		SystemReserved:      false,
		CreatedAtHlc:        "1740000000000-00001-0000000000000004096",
		UpdatedAtHlc:        "1740000000000-00001-0000000000000004096",
		VersionUsername:     "1740000000000-00001-0000000000000004096",
		VersionPasswordHash: "1740000000000-00001-0000000000000004096",
		VersionProfile:      "1740000000000-00001-0000000000000004096",
		VersionRole:         "1740000000000-00001-0000000000000004096",
		OriginNodeId:        testNodeID,
	}); err != nil {
		t.Fatalf("set typed body: %v", err)
	}

	batch := &EventBatch{
		Events: []*ReplicatedEvent{event},
	}
	envelope := &Envelope{
		NodeId:    testNodeID,
		Sequence:  7,
		SentAtHlc: "1740000000000-00002-0000000000000004096",
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
	if decodedBatch.GetEvents()[0].GetEventId() != 1001 || EventTypeFromBody(decodedBatch.GetEvents()[0].GetTypedBody()) != "user_created" {
		t.Fatalf("unexpected event id: %+v", decodedBatch.GetEvents()[0])
	}
}

type unsupportedEventBody struct{}

func (unsupportedEventBody) eventType() string { return "unsupported" }

func TestReplicatedEventTypedBodyHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      EventBody
		eventType string
	}{
		{
			name: "user_created",
			body: &UserCreatedEvent{
				NodeId:              testNodeID,
				UserId:              1001,
				Username:            "alice",
				PasswordHash:        "hash",
				Profile:             "{}",
				Role:                "user",
				CreatedAtHlc:        "1740000000000-00001-0000000000000004096",
				UpdatedAtHlc:        "1740000000000-00001-0000000000000004096",
				VersionUsername:     "1740000000000-00001-0000000000000004096",
				VersionPasswordHash: "1740000000000-00001-0000000000000004096",
				VersionProfile:      "1740000000000-00001-0000000000000004096",
				VersionRole:         "1740000000000-00001-0000000000000004096",
				OriginNodeId:        testNodeID,
			},
			eventType: "user_created",
		},
		{
			name: "user_updated",
			body: &UserUpdatedEvent{
				NodeId:              testNodeID,
				UserId:              1001,
				Username:            "alice-updated",
				PasswordHash:        "hash-2",
				Profile:             `{"display_name":"Alice"}`,
				Role:                "admin",
				CreatedAtHlc:        "1740000000000-00001-0000000000000004096",
				UpdatedAtHlc:        "1740000000000-00002-0000000000000004096",
				VersionUsername:     "1740000000000-00002-0000000000000004096",
				VersionPasswordHash: "1740000000000-00002-0000000000000004096",
				VersionProfile:      "1740000000000-00002-0000000000000004096",
				VersionRole:         "1740000000000-00002-0000000000000004096",
				OriginNodeId:        testNodeID,
			},
			eventType: "user_updated",
		},
		{
			name: "user_deleted",
			body: &UserDeletedEvent{
				NodeId:       testNodeID,
				UserId:       1001,
				DeletedAtHlc: "1740000000000-00003-0000000000000004096",
			},
			eventType: "user_deleted",
		},
		{
			name: "message_created",
			body: &MessageCreatedEvent{
				UserNodeId:   testNodeID,
				UserId:       1001,
				NodeId:       testNodeID,
				Seq:          7,
				Sender:       "orders",
				Body:         "hello",
				Metadata:     `{"order_id":"A1001"}`,
				CreatedAtHlc: "1740000000000-00003-0000000000000004096",
			},
			eventType: "message_created",
		},
		{
			name: "channel_subscribed",
			body: &ChannelSubscribedEvent{
				SubscriberNodeId: testNodeID,
				SubscriberUserId: 1001,
				ChannelNodeId:    testNodeID,
				ChannelUserId:    2002,
				SubscribedAtHlc:  "1740000000000-00004-0000000000000004096",
				OriginNodeId:     testNodeID,
			},
			eventType: "channel_subscribed",
		},
		{
			name: "channel_unsubscribed",
			body: &ChannelUnsubscribedEvent{
				SubscriberNodeId: testNodeID,
				SubscriberUserId: 1001,
				ChannelNodeId:    testNodeID,
				ChannelUserId:    2002,
				SubscribedAtHlc:  "1740000000000-00004-0000000000000004096",
				DeletedAtHlc:     "1740000000000-00005-0000000000000004096",
				OriginNodeId:     testNodeID,
			},
			eventType: "channel_unsubscribed",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			event := &ReplicatedEvent{
				EventId:         1,
				AggregateType:   "test",
				AggregateNodeId: testNodeID,
				AggregateId:     99,
				Hlc:             "1740000000000-00010-0000000000000004096",
				OriginNodeId:    testNodeID,
			}
			if err := event.SetTypedBody(tc.body); err != nil {
				t.Fatalf("set typed body: %v", err)
			}

			got := event.GetTypedBody()
			if got == nil {
				t.Fatalf("expected typed body")
			}
			if EventTypeFromBody(got) != tc.eventType {
				t.Fatalf("unexpected event type: got=%q want=%q", EventTypeFromBody(got), tc.eventType)
			}
			if !gproto.Equal(got.(gproto.Message), tc.body.(gproto.Message)) {
				t.Fatalf("typed body mismatch: got=%+v want=%+v", got, tc.body)
			}

			data, err := gproto.Marshal(event)
			if err != nil {
				t.Fatalf("marshal replicated event: %v", err)
			}

			var decoded ReplicatedEvent
			if err := gproto.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("unmarshal replicated event: %v", err)
			}
			decodedBody := decoded.GetTypedBody()
			if decodedBody == nil {
				t.Fatalf("expected decoded typed body")
			}
			if EventTypeFromBody(decodedBody) != tc.eventType {
				t.Fatalf("unexpected decoded event type: got=%q want=%q", EventTypeFromBody(decodedBody), tc.eventType)
			}
			if !gproto.Equal(decodedBody.(gproto.Message), tc.body.(gproto.Message)) {
				t.Fatalf("decoded typed body mismatch: got=%+v want=%+v", decodedBody, tc.body)
			}
		})
	}
}

func TestReplicatedEventTypedBodyHelpersRejectUnsupportedBody(t *testing.T) {
	t.Parallel()

	event := &ReplicatedEvent{}
	if err := event.SetTypedBody(unsupportedEventBody{}); err == nil {
		t.Fatalf("expected unsupported body to fail")
	}
	if EventTypeFromBody(nil) != "" {
		t.Fatalf("expected empty event type for nil body")
	}
	if err := event.SetTypedBody(nil); err != nil {
		t.Fatalf("clear typed body: %v", err)
	}
	if event.GetTypedBody() != nil {
		t.Fatalf("expected cleared body to be nil")
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
								CreatedAtHlc: "1740000000000-00001-0000000000000004096",
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

func TestSnapshotSubscriptionRowRoundTrip(t *testing.T) {
	t.Parallel()

	envelope := &Envelope{
		NodeId: testNodeID,
		Body: &Envelope_SnapshotChunk{
			SnapshotChunk: &SnapshotChunk{
				SnapshotVersion: SnapshotVersion,
				Partition:       "subscriptions/full",
				Kind:            SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_SUBSCRIPTIONS,
				Rows: []*SnapshotRow{
					{
						Body: &SnapshotRow_Subscription{
							Subscription: &SnapshotSubscriptionRow{
								SubscriberNodeId: testNodeID,
								SubscriberUserId: 42,
								ChannelNodeId:    testNodeID,
								ChannelUserId:    99,
								SubscribedAtHlc:  "1740000000000-00001-0000000000000004096",
								OriginNodeId:     testNodeID,
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

	subscription := decoded.GetSnapshotChunk().GetRows()[0].GetSubscription()
	if subscription.GetSubscriberUserId() != 42 || subscription.GetChannelUserId() != 99 {
		t.Fatalf("unexpected snapshot subscription: %+v", subscription)
	}
}
