package proto

type MessageType string

const ProtocolVersion = "v1alpha1"

const (
	MessageTypeHello          MessageType = "HELLO"
	MessageTypeAck            MessageType = "ACK"
	MessageTypeEventBatch     MessageType = "EVENT_BATCH"
	MessageTypePullEvents     MessageType = "PULL_EVENTS"
	MessageTypeSnapshotDigest MessageType = "SNAPSHOT_DIGEST"
	MessageTypeSnapshotChunk  MessageType = "SNAPSHOT_CHUNK"
)

func KnownMessageTypes() []MessageType {
	return []MessageType{
		MessageTypeHello,
		MessageTypeAck,
		MessageTypeEventBatch,
		MessageTypePullEvents,
		MessageTypeSnapshotDigest,
		MessageTypeSnapshotChunk,
	}
}
