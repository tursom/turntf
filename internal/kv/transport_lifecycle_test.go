package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func pendingCount(t *Transport) int { t.mu.Lock(); defer t.mu.Unlock(); return len(t.pending) }

func awaitCall(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("RPC did not finish")
		return nil
	}
}

func TestConsensusSendFailureAndMarshalFailureCleanup(t *testing.T) {
	tr := NewTransport("1", func(raft.ServerAddress, []byte) error { return errors.New("offline") })
	defer tr.Close()
	for i := 0; i < 200; i++ {
		if err := tr.RequestVote("2", "2", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{}); err == nil {
			t.Fatal("missing error")
		}
	}
	if pendingCount(tr) != 0 {
		t.Fatal("send failures leaked pending")
	}
	tr.binaryPeer = func(raft.ServerAddress) bool { return true }
	if err := tr.AppendEntries("2", "2", &raft.AppendEntriesRequest{Entries: []*raft.Log{nil}}, &raft.AppendEntriesResponse{}); err == nil {
		t.Fatal("missing encode error")
	}
	if pendingCount(tr) != 0 {
		t.Fatal("encode failure leaked pending")
	}
}

func TestConsensusSimultaneousSameIDRequests(t *testing.T) {
	for _, binary := range []bool{false, true} {
		var a, b *Transport
		arrived := make(chan struct{}, 2)
		gate := make(chan struct{})
		sender := func(peer **Transport, source raft.ServerAddress) ConsensusSender {
			return func(_ raft.ServerAddress, p []byte) error {
				w, err := decodeRPC(p)
				if err != nil {
					return err
				}
				if !w.response {
					arrived <- struct{}{}
					<-gate
				}
				return (*peer).DeliverFrom(source, p)
			}
		}
		a = NewTransport("1", sender(&b, "1"), WithBinaryConsensus(func(raft.ServerAddress) bool { return binary }))
		b = NewTransport("2", sender(&a, "2"), WithBinaryConsensus(func(raft.ServerAddress) bool { return binary }))
		for _, tr := range []*Transport{a, b} {
			go func(tr *Transport) {
				rpc := <-tr.Consumer()
				req := rpc.Command.(*raft.RequestVoteRequest)
				rpc.Respond(&raft.RequestVoteResponse{Term: req.Term, Granted: true}, nil)
			}(tr)
		}
		done := make(chan error, 2)
		go func() {
			var r raft.RequestVoteResponse
			err := a.RequestVote("2", "2", &raft.RequestVoteRequest{Term: 7}, &r)
			if err == nil && r.Term != 7 {
				err = errors.New("wrong response")
			}
			done <- err
		}()
		go func() {
			var r raft.RequestVoteResponse
			err := b.RequestVote("1", "1", &raft.RequestVoteRequest{Term: 9}, &r)
			if err == nil && r.Term != 9 {
				err = errors.New("wrong response")
			}
			done <- err
		}()
		<-arrived
		<-arrived
		close(gate)
		for i := 0; i < 2; i++ {
			if err := awaitCall(t, done); err != nil {
				t.Fatal(err)
			}
		}
		_ = a.Close()
		_ = b.Close()
	}
}

func TestConsensusResponseMatching(t *testing.T) {
	sent := make(chan []byte, 1)
	tr := NewTransport("1", func(_ raft.ServerAddress, p []byte) error { sent <- p; return nil })
	defer tr.Close()
	done := make(chan error, 1)
	go func() {
		done <- tr.RequestVote("2", "2", &raft.RequestVoteRequest{Term: 7}, &raft.RequestVoteResponse{})
	}()
	req, err := decodeRPC(<-sent)
	if err != nil {
		t.Fatal(err)
	}
	w := rpcWire{ID: req.ID, Type: "vote", Source: "2", response: true, value: &raft.RequestVoteResponse{Term: 7, Granted: true}}
	for _, bad := range []rpcWire{
		{ID: req.ID, Type: "append", Source: "2", response: true, value: &raft.AppendEntriesResponse{}},
		{ID: req.ID, Type: "vote", Source: "3", response: true, value: &raft.RequestVoteResponse{}},
		{ID: req.ID, Type: "vote", Source: "2", response: true, binary: true, value: &raft.RequestVoteResponse{}},
	} {
		data, _ := encodeRPC(bad)
		if err := tr.DeliverFrom(raft.ServerAddress(bad.Source), data); err == nil {
			t.Fatal("accepted mismatched response")
		}
		if pendingCount(tr) != 1 {
			t.Fatal("mismatch consumed pending")
		}
	}
	data, _ := encodeRPC(w)
	if err := tr.DeliverFrom("3", data); err == nil {
		t.Fatal("accepted outer source mismatch")
	}
	// Actual old nodes omit Source on JSON replies.
	w.Source = ""
	data, _ = encodeRPC(w)
	if err := tr.DeliverFrom("2", data); err != nil {
		t.Fatal(err)
	}
	if err := awaitCall(t, done); err != nil {
		t.Fatal(err)
	}
	if err := tr.DeliverFrom("2", data); err != nil {
		t.Fatal("late duplicate not ignored", err)
	}
	if pendingCount(tr) != 0 {
		t.Fatal("response pending leaked")
	}
}

func TestConsensusTimeoutCloseAndBoundedRequests(t *testing.T) {
	tr := NewTransport("1", func(raft.ServerAddress, []byte) error { return nil })
	tr.timeout = 15 * time.Millisecond
	if err := tr.RequestVote("2", "2", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if pendingCount(tr) != 0 {
		t.Fatal("timeout pending leaked")
	}
	_ = tr.Close()
	tr = NewTransport("1", nil, WithConsensusSender(func(ctx context.Context, _ raft.ServerAddress, _ []byte) error { <-ctx.Done(); return ctx.Err() }))
	done := make(chan error, 1)
	go func() { done <- tr.RequestVote("2", "2", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{}) }()
	// Fill incoming workers without replying; Close must unblock both the sender
	// and all requests waiting for Raft, without closing consumer underneath them.
	for i := 1; i <= 128; i++ {
		data, _ := encodeRPC(rpcWire{ID: uint64(i), Type: "vote", Source: "2", value: &raft.RequestVoteRequest{}})
		if err := tr.DeliverFrom("2", data); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := encodeRPC(rpcWire{ID: 129, Type: "vote", Source: "2", value: &raft.RequestVoteRequest{}})
	if err := tr.DeliverFrom("2", data); err == nil {
		t.Fatal("unbounded workers")
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = tr.Close() }()
	}
	wg.Wait()
	if err := awaitCall(t, done); err == nil {
		t.Fatal("close did not fail call")
	}
	if err := tr.DeliverFrom("2", data); err == nil {
		t.Fatal("accepted after close")
	}
	if pendingCount(tr) != 0 {
		t.Fatal("close pending leaked")
	}
}

func TestConsensusNegotiationAndSnapshotBody(t *testing.T) {
	for _, binary := range []bool{false, true} {
		var a, b *Transport
		a = NewTransport("1", func(_ raft.ServerAddress, p []byte) error {
			if bytes.HasPrefix(p, binaryRPCPrefix) != binary {
				return errors.New("wrong request codec")
			}
			return b.DeliverFrom("1", p)
		}, WithBinaryConsensus(func(raft.ServerAddress) bool { return binary }))
		// B intentionally lacks an outgoing binary capability callback. Responses
		// still follow the incoming codec, including during an upgrade handshake.
		b = NewTransport("2", func(_ raft.ServerAddress, p []byte) error {
			if bytes.HasPrefix(p, binaryRPCPrefix) != binary {
				return errors.New("wrong response codec")
			}
			if !binary {
				var legacy rpcWire
				_ = json.Unmarshal(p, &legacy)
				legacy.Source = ""
				p, _ = json.Marshal(legacy)
			}
			return a.DeliverFrom("2", p)
		})
		go func() {
			for rpc := range b.Consumer() {
				switch rpc.Command.(type) {
				case *raft.RequestVoteRequest:
					rpc.Respond(&raft.RequestVoteResponse{Granted: true}, nil)
				case *raft.InstallSnapshotRequest:
					body, err := io.ReadAll(rpc.Reader)
					if !bytes.Equal(body, []byte("snapshot\x00body")) {
						err = errors.New("body mismatch")
					}
					rpc.Respond(&raft.InstallSnapshotResponse{Success: err == nil}, err)
				}
			}
		}()
		if err := a.RequestVote("2", "2", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{}); err != nil {
			t.Fatal(err)
		}
		err := a.InstallSnapshot("2", "2", &raft.InstallSnapshotRequest{Size: 13}, &raft.InstallSnapshotResponse{}, bytes.NewBufferString("snapshot\x00body"))
		if binary && err != nil {
			t.Fatal(err)
		}
		if !binary && err == nil {
			t.Fatal("legacy snapshot silently discarded")
		}
		if binary && a.InstallSnapshot("2", "2", &raft.InstallSnapshotRequest{Size: 20}, &raft.InstallSnapshotResponse{}, bytes.NewBufferString("short")) == nil {
			t.Fatal("accepted truncated snapshot")
		}
		_ = a.Close()
		_ = b.Close()
	}
}

type snapshotReadProbe struct{ reads int }

func (r *snapshotReadProbe) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }

func TestConsensusRejectsOversizedSnapshotBeforeReading(t *testing.T) {
	tr := NewTransport("1", nil, WithBinaryConsensus(func(raft.ServerAddress) bool { return true }))
	defer tr.Close()
	reader := &snapshotReadProbe{}
	err := tr.InstallSnapshot("2", "2", &raft.InstallSnapshotRequest{Size: maxConsensusSnapshotBytes + 1}, &raft.InstallSnapshotResponse{}, reader)
	if err == nil || reader.reads != 0 || pendingCount(tr) != 0 {
		t.Fatalf("oversized snapshot was read or queued: reads=%d error=%v", reader.reads, err)
	}
}

func TestConsensusPipelineCloseCancelsInflightAndBackpressure(t *testing.T) {
	started := make(chan struct{}, 2)
	tr := NewTransport("1", nil, WithConsensusSender(func(ctx context.Context, _ raft.ServerAddress, _ []byte) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	defer tr.Close()
	pipe, _ := tr.AppendEntriesPipeline("2", "2")
	defer pipe.Close()
	f1, _ := pipe.AppendEntries(&raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
	f2, _ := pipe.AppendEntries(&raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
	<-started
	<-started
	done := make(chan error, 1)
	go func() {
		_, err := pipe.AppendEntries(&raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
		done <- err
	}()
	_ = pipe.Close()
	if err := awaitCall(t, done); !errors.Is(err, raft.ErrPipelineShutdown) {
		t.Fatal(err)
	}
	for _, f := range []raft.AppendFuture{f1, f2} {
		if f.Error() == nil {
			t.Fatal("inflight not cancelled")
		}
	}
	if pendingCount(tr) != 0 {
		t.Fatal("pipeline pending leaked")
	}
}

func TestConsensusPipelineFailureUnblocksSenders(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	tr := NewTransport("1", nil, WithConsensusSender(func(ctx context.Context, _ raft.ServerAddress, _ []byte) error {
		started <- struct{}{}
		select {
		case <-release:
			return errors.New("offline")
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	defer tr.Close()
	pipe, _ := tr.AppendEntriesPipeline("2", "2")
	defer pipe.Close()
	for i := 0; i < 2; i++ {
		if _, err := pipe.AppendEntries(&raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{}); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	done := make(chan error, 1)
	go func() {
		_, err := pipe.AppendEntries(&raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
		done <- err
	}()
	close(release)
	select {
	case f := <-pipe.Consumer():
		if f.Error() == nil || f.Response().Success {
			t.Fatal("failed pipeline result was successful")
		}
	case <-time.After(time.Second):
		t.Fatal("no failure result")
	}
	// Raft's decoder exits on the first failed result and will never read another.
	if err := awaitCall(t, done); err == nil {
		t.Fatal("blocked sender did not fail")
	}
}
