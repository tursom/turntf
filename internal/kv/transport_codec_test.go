package kv

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func codecFixtures() []rpcWire {
	h := raft.RPCHeader{ProtocolVersion: 3, ID: []byte("71234567890123456"), Addr: []byte("71234567890123456")}
	return []rpcWire{
		{Type: "append", value: &raft.AppendEntriesRequest{RPCHeader: h, Term: 71, Leader: h.Addr, PrevLogEntry: 345, PrevLogTerm: 70, LeaderCommitIndex: 344}},
		{Type: "append", value: &raft.AppendEntriesRequest{RPCHeader: h, Term: 71, Leader: h.Addr, PrevLogEntry: 345, PrevLogTerm: 70, LeaderCommitIndex: 344, Entries: []*raft.Log{{Index: 346, Term: 71, Type: raft.LogCommand, Data: []byte{0, 255, 10}, Extensions: []byte("meta"), AppendedAt: time.Date(2026, 10, 29, 12, 3, 4, 123456789, time.UTC)}, {Index: 347, Term: 71, Type: raft.LogNoop}}}},
		{Type: "vote", value: &raft.RequestVoteRequest{RPCHeader: h, Term: 71, Candidate: h.Addr, LastLogIndex: 345, LastLogTerm: 70, LeadershipTransfer: true}},
		{Type: "prevote", value: &raft.RequestPreVoteRequest{RPCHeader: h, Term: 72, LastLogIndex: 345, LastLogTerm: 71}},
		{Type: "snapshot", value: &raft.InstallSnapshotRequest{RPCHeader: h, SnapshotVersion: 1, Term: 71, Leader: h.Addr, LastLogIndex: 345, LastLogTerm: 70, Peers: []byte("peers"), Configuration: []byte("configuration"), ConfigurationIndex: 12, Size: 4}, snapshot: []byte{0, 255, 1, 2}},
		{Type: "timeout", value: &raft.TimeoutNowRequest{RPCHeader: h}},
		{Type: "append", response: true, value: &raft.AppendEntriesResponse{RPCHeader: h, Term: 71, LastLog: 345, Success: true, NoRetryBackoff: true}},
		{Type: "vote", response: true, value: &raft.RequestVoteResponse{RPCHeader: h, Term: 71, Peers: []byte("peers"), Granted: true}},
		{Type: "prevote", response: true, value: &raft.RequestPreVoteResponse{RPCHeader: h, Term: 71, Granted: true}},
		{Type: "snapshot", response: true, value: &raft.InstallSnapshotResponse{RPCHeader: h, Term: 71, Success: true}},
		{Type: "timeout", response: true, value: &raft.TimeoutNowResponse{RPCHeader: h}},
	}
}

func TestConsensusCodecRoundTrip(t *testing.T) {
	for _, binary := range []bool{false, true} {
		for i, w := range codecFixtures() {
			w.ID, w.Source, w.binary = uint64(i+1), "71234567890123456", binary
			if w.response {
				w.Error = "test error"
			}
			encoded, err := encodeRPC(w)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeRPC(encoded)
			if err != nil {
				t.Fatalf("binary=%v %s: %v", binary, w.Type, err)
			}
			if got.ID != w.ID || got.Source != w.Source || got.Type != w.Type || got.Error != w.Error || got.response != w.response || got.binary != binary || !reflect.DeepEqual(got.value, w.value) {
				t.Fatalf("binary=%v %s value mismatch: got %#v want %#v", binary, w.Type, got.value, w.value)
			}
			if binary && !bytes.Equal(got.snapshot, w.snapshot) {
				t.Fatal("snapshot mismatch")
			}
		}
	}
}

func TestConsensusCodecRejectsMalformed(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"id":1,"type":"vote","source":"1"}`),
		[]byte(`{"id":1,"type":"vote","source":"1","request":{},"response":{}}`),
		[]byte(`{"id":1,"type":"vote","source":"1","request":null}`),
		[]byte(`{"id":0,"type":"vote","source":"1","request":{}}`),
		[]byte(`{"id":1,"type":"unknown","source":"1","request":{}}`),
		[]byte(`{"id":1,"type":"vote","request":{}}`),
		[]byte("TFR\x02"), []byte("TFR\x01\xff"), []byte("TFR\x01"),
	} {
		if _, err := decodeRPC(payload); err == nil {
			t.Fatalf("accepted %q", payload)
		}
	}
	bad := &raft.AppendEntriesRequest{Entries: []*raft.Log{nil}}
	if _, err := encodeRPC(rpcWire{ID: 1, Type: "append", Source: "1", binary: true, value: bad}); err == nil {
		t.Fatal("accepted nil log")
	}
}

func TestConsensusEmptyAppendSize(t *testing.T) {
	for _, w := range []rpcWire{codecFixtures()[0], codecFixtures()[6]} {
		w.ID, w.Source = 123456, "71234567890123456"
		legacy, err := encodeRPC(w)
		if err != nil {
			t.Fatal(err)
		}
		w.binary = true
		compact, err := encodeRPC(w)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("response=%v JSON=%d binary=%d reduction=%.1f%%", w.response, len(legacy), len(compact), 100*(1-float64(len(compact))/float64(len(legacy))))
		if len(compact) >= len(legacy)/2 {
			t.Fatal("binary empty append should be less than half JSON")
		}
		// Log the old response baseline too: deployed JSON omits response source.
		if w.response {
			var old map[string]any
			_ = json.Unmarshal(legacy, &old)
			delete(old, "source")
			oldBytes, _ := json.Marshal(old)
			t.Logf("deployed response JSON=%d binary=%d", len(oldBytes), len(compact))
		}
	}
}

func BenchmarkConsensusCodec(b *testing.B) {
	for _, binary := range []bool{false, true} {
		name := "json"
		if binary {
			name = "binary"
		}
		w := codecFixtures()[0]
		w.ID, w.Source, w.binary = 123456, "71234567890123456", binary
		payload, _ := encodeRPC(w)
		b.Run(name+"/encode", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				if _, err := encodeRPC(w); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/decode", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				if _, err := decodeRPC(payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzConsensusCodec(f *testing.F) {
	for _, binary := range []bool{false, true} {
		for i, w := range codecFixtures() {
			w.ID, w.Source, w.binary = uint64(i+1), "1", binary
			p, err := encodeRPC(w)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(p)
		}
	}
	f.Add([]byte("TFR\x02"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		w, err := decodeRPC(payload)
		if err != nil {
			return
		}
		if _, err := encodeRPC(w); err != nil {
			t.Fatal(err)
		}
	})
}
