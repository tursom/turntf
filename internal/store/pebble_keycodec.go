package store

import (
	"bytes"
	"encoding/binary"

	"github.com/tursom/turntf/internal/clock"
)

const (
	metaEventSequenceTag     byte = 0x01
	metaMessageSequenceTag   byte = 0x02
	metaMessageUserStateTag  byte = 0x03
	metaPeerAckCursorTag     byte = 0x04
	metaOriginCursorTag      byte = 0x05
	metaPendingProjectionTag byte = 0x06

	eventSeqTag    byte = 0x10
	eventOriginTag byte = 0x11

	messageIDTag          byte = 0x20
	messageUserTag        byte = 0x21
	messageProducerTag    byte = 0x22
	messageSessionTag     byte = 0x23
	messageInboxTag       byte = 0x24
	messageInboxSourceTag byte = 0x25
)

// encodeUint64 appends 8-byte big-endian encoding of v to buf.
func encodeUint64(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

// decodeUint64 reads a big-endian uint64 from b[0:8].
func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// encodeUint64Desc appends 8-byte big-endian encoding of ^v to buf,
// so that larger values sort before smaller ones.
func encodeUint64Desc(buf []byte, v uint64) []byte {
	return encodeUint64(buf, ^v)
}

// decodeUint64Desc reads a descending-encoded uint64 from b[0:8].
func decodeUint64Desc(b []byte) uint64 {
	return ^decodeUint64(b)
}

// encodeTimestamp appends 18-byte fixed-width encoding of ts to buf:
//
//	[WallTimeMs:8 BE][Logical:2 BE][NodeID:8 BE]
func encodeTimestamp(buf []byte, ts clock.Timestamp) []byte {
	buf = encodeUint64(buf, uint64(ts.WallTimeMs))
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], ts.Logical)
	buf = append(buf, tmp[:]...)
	return encodeUint64(buf, uint64(ts.NodeID))
}

// decodeTimestamp decodes an 18-byte encoded timestamp.
func decodeTimestamp(b []byte) clock.Timestamp {
	return clock.Timestamp{
		WallTimeMs: int64(decodeUint64(b[0:8])),
		Logical:    binary.BigEndian.Uint16(b[8:10]),
		NodeID:     int64(decodeUint64(b[10:18])),
	}
}

// encodeTimestampDesc appends 18-byte descending encoding of ts to buf
// (every byte bit-flipped so larger timestamps sort first).
func encodeTimestampDesc(buf []byte, ts clock.Timestamp) []byte {
	off := len(buf)
	buf = encodeTimestamp(buf, ts)
	for i := off; i < len(buf); i++ {
		buf[i] ^= 0xff
	}
	return buf
}

// decodeTimestampDesc decodes an 18-byte descending-encoded timestamp.
func decodeTimestampDesc(b []byte) clock.Timestamp {
	flipped := make([]byte, 18)
	for i := range flipped {
		flipped[i] = b[i] ^ 0xff
	}
	return decodeTimestamp(flipped)
}

// prefixUpperBound returns the smallest key greater than every key with
// the given prefix, by incrementing the last byte.  Returns nil if the
// prefix is all \xff bytes (no bounded upper bound).
func prefixUpperBound(prefix []byte) []byte {
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

// encodeInt64 returns an 8-byte big-endian encoding of value.
// Used for value encoding (not keys).
func encodeInt64(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

// decodeInt64 reads a big-endian int64 from b[0:8].
// Used for value decoding (not keys).
func decodeInt64(value []byte) int64 {
	return int64(binary.BigEndian.Uint64(value))
}
