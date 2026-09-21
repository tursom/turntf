package store

import "errors"

// ErrStreamSessionUnavailable means the target stream session cannot accept the frame.
var ErrStreamSessionUnavailable = errors.New("stream target session unavailable")

// StreamFrame is a logical point-to-point stream frame. It is deliberately
// separate from TransientPacket: stream delivery is ordered, resumable, and
// governed by epoch/offset rather than packet acceptance.
type StreamFrame struct {
	StreamID      []byte
	Kind          uint32
	Epoch         uint64
	Offset        uint64
	Window        uint64
	Payload       []byte
	Sender        UserKey
	Recipient     UserKey
	TargetSession SessionRef
	SourceSession SessionRef
	TTLHops       int32
}
