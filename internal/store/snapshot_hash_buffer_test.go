package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	clusterproto "github.com/tursom/turntf/internal/proto"
	gproto "google.golang.org/protobuf/proto"
)

// 使用独立行编码拼接协议定义的长度前缀，核对复用缓冲后摘要字节仍完全兼容。
func snapshotHashReference(rows []*clusterproto.SnapshotRow) ([]byte, error) {
	var wire bytes.Buffer
	for _, row := range rows {
		encoded, err := (gproto.MarshalOptions{Deterministic: true}).Marshal(row)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(&wire, binary.BigEndian, uint64(len(encoded))); err != nil {
			return nil, err
		}
		wire.Write(encoded)
	}
	digest := sha256.Sum256(wire.Bytes())
	return digest[:], nil
}

func TestSnapshotHashBufferPreservesWireDigest(t *testing.T) {
	message := func(body []byte) *clusterproto.SnapshotRow {
		return &clusterproto.SnapshotRow{Body: &clusterproto.SnapshotRow_Message{Message: &clusterproto.SnapshotMessageRow{
			Recipient: &clusterproto.ClusterUserRef{NodeId: 1, UserId: 2}, NodeId: 1, Seq: 3,
			Body: body, CreatedAtHlc: "1700000000000-00000-0000000000000000001",
		}}}
	}
	cases := [][]*clusterproto.SnapshotRow{
		nil,
		{nil, {}},
		{message(bytes.Repeat([]byte("l"), 64<<10)), message([]byte("short")), nil, message(nil)},
		{{Body: &clusterproto.SnapshotRow_Tombstone{Tombstone: &clusterproto.SnapshotTombstoneRow{EntityType: "user"}}}, message([]byte("history"))},
	}
	for i, rows := range cases {
		want, err := snapshotHashReference(rows)
		if err != nil {
			t.Fatal(err)
		}
		got, err := hashSnapshotRows(rows)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("case %d: hash=%x want=%x err=%v", i, got, want, err)
		}
	}
	row := message([]byte("first"))
	before, err := hashSnapshotRows([]*clusterproto.SnapshotRow{row})
	if err != nil {
		t.Fatal(err)
	}
	row.GetMessage().Body = []byte("second")
	after, err := hashSnapshotRows([]*clusterproto.SnapshotRow{row})
	if err != nil || bytes.Equal(before, after) {
		t.Fatalf("content change did not change digest: err=%v", err)
	}
	row.GetMessage().CreatedAtHlc = string([]byte{0xff})
	if _, err := hashSnapshotRows([]*clusterproto.SnapshotRow{row}); err == nil {
		t.Fatal("invalid protobuf UTF-8 was accepted")
	}
}
