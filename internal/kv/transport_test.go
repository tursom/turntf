package kv

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestConsensusTransportUsesOpaqueSender(t *testing.T) {
	var a, b *Transport
	a = NewTransport("1", func(target raft.ServerAddress, payload []byte) error {
		if target != "2" {
			t.Fatalf("A target=%s", target)
		}
		return b.Deliver(payload)
	})
	b = NewTransport("2", func(target raft.ServerAddress, payload []byte) error {
		if target != "1" {
			t.Fatalf("B target=%s", target)
		}
		return a.Deliver(payload)
	})
	go func() {
		rpc := <-b.Consumer()
		req, ok := rpc.Command.(*raft.RequestVoteRequest)
		if !ok || req.Term != 7 {
			t.Errorf("request=%#v", rpc.Command)
		}
		rpc.Respond(&raft.RequestVoteResponse{Term: 7, Granted: true}, nil)
	}()
	resp := &raft.RequestVoteResponse{}
	if err := a.RequestVote("2", "2", &raft.RequestVoteRequest{Term: 7}, resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Granted || resp.Term != 7 {
		t.Fatalf("response=%+v", resp)
	}
}

func TestConsensusPipeline(t *testing.T) {
	var a, b *Transport
	a = NewTransport("1", func(_ raft.ServerAddress, p []byte) error { return b.Deliver(p) })
	b = NewTransport("2", func(_ raft.ServerAddress, p []byte) error { return a.Deliver(p) })
	go func() {
		for rpc := range b.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
		}
	}()
	pipeline, err := a.AppendEntriesPipeline("2", "2")
	if err != nil {
		t.Fatal(err)
	}
	future, err := pipeline.AppendEntries(&raft.AppendEntriesRequest{Term: 1}, &raft.AppendEntriesResponse{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-pipeline.Consumer():
		if got != future || got.Error() != nil {
			t.Fatal("pipeline failed")
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline timeout")
	}
}
