package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type ConsensusSender func(target raft.ServerAddress, payload []byte) error

type Transport struct {
	local    raft.ServerAddress
	send     ConsensusSender
	consumer chan raft.RPC
	mu       sync.Mutex
	next     uint64
	pending  map[uint64]chan rpcWire
}

type rpcWire struct {
	ID       uint64          `json:"id"`
	Type     string          `json:"type"`
	Source   string          `json:"source,omitempty"`
	Request  json.RawMessage `json:"request,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
	Error    string          `json:"error,omitempty"`
}

func NewTransport(local raft.ServerAddress, send ConsensusSender) *Transport {
	return &Transport{local: local, send: send, consumer: make(chan raft.RPC, 128), pending: make(map[uint64]chan rpcWire)}
}
func (t *Transport) Consumer() <-chan raft.RPC                                  { return t.consumer }
func (t *Transport) LocalAddr() raft.ServerAddress                              { return t.local }
func (t *Transport) EncodePeer(_ raft.ServerID, addr raft.ServerAddress) []byte { return []byte(addr) }
func (t *Transport) DecodePeer(b []byte) raft.ServerAddress                     { return raft.ServerAddress(b) }
func (t *Transport) SetHeartbeatHandler(func(raft.RPC))                         {}
func (t *Transport) Close() error                                               { close(t.consumer); return nil }
func (t *Transport) Deliver(payload []byte) error {
	var wire rpcWire
	if err := json.Unmarshal(payload, &wire); err != nil {
		return err
	}
	if wire.ID == 0 {
		return errors.New("kv: consensus message id is zero")
	}
	t.mu.Lock()
	ch := t.pending[wire.ID]
	if ch != nil {
		delete(t.pending, wire.ID)
	}
	t.mu.Unlock()
	if ch != nil {
		ch <- wire
		return nil
	}
	var rpc raft.RPC
	switch wire.Type {
	case "append":
		var req raft.AppendEntriesRequest
		if err := json.Unmarshal(wire.Request, &req); err != nil {
			return err
		}
		rpc.Command = &req
	case "vote":
		var req raft.RequestVoteRequest
		if err := json.Unmarshal(wire.Request, &req); err != nil {
			return err
		}
		rpc.Command = &req
	case "prevote":
		var req raft.RequestPreVoteRequest
		if err := json.Unmarshal(wire.Request, &req); err != nil {
			return err
		}
		rpc.Command = &req
	case "snapshot":
		var req raft.InstallSnapshotRequest
		if err := json.Unmarshal(wire.Request, &req); err != nil {
			return err
		}
		rpc.Command = &req
	case "timeout":
		var req raft.TimeoutNowRequest
		if err := json.Unmarshal(wire.Request, &req); err != nil {
			return err
		}
		rpc.Command = &req
	default:
		return fmt.Errorf("kv: unknown consensus rpc %q", wire.Type)
	}
	resp := make(chan raft.RPCResponse, 1)
	rpc.RespChan = resp
	select {
	case t.consumer <- rpc:
	case <-time.After(10 * time.Second):
		return errors.New("kv: consensus consumer blocked")
	}
	result := <-resp
	var response any
	switch rpc.Command.(type) {
	case *raft.AppendEntriesRequest:
		response = &raft.AppendEntriesResponse{}
	case *raft.RequestVoteRequest:
		response = &raft.RequestVoteResponse{}
	case *raft.RequestPreVoteRequest:
		response = &raft.RequestPreVoteResponse{}
	case *raft.InstallSnapshotRequest:
		response = &raft.InstallSnapshotResponse{}
	case *raft.TimeoutNowRequest:
		response = &raft.TimeoutNowResponse{}
	}
	if result.Response != nil {
		response = result.Response
	}
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	out := rpcWire{ID: wire.ID, Type: wire.Type, Response: data}
	if result.Error != nil {
		out.Error = result.Error.Error()
	}
	encoded, _ := json.Marshal(out)
	return t.send(raft.ServerAddress(wire.Source), encoded)
}
func (t *Transport) call(id raft.ServerID, target raft.ServerAddress, typ string, req any, resp any) error {
	if t.send == nil {
		return errors.New("kv: consensus sender is nil")
	}
	t.mu.Lock()
	t.next++
	rid := t.next
	ch := make(chan rpcWire, 1)
	t.pending[rid] = ch
	t.mu.Unlock()
	b, _ := json.Marshal(req)
	data, _ := json.Marshal(rpcWire{ID: rid, Type: typ, Source: string(t.local), Request: b})
	if err := t.send(target, data); err != nil {
		return err
	}
	select {
	case w := <-ch:
		if w.Error != "" {
			return errors.New(w.Error)
		}
		return json.Unmarshal(w.Response, resp)
	case <-time.After(10 * time.Second):
		t.mu.Lock()
		delete(t.pending, rid)
		t.mu.Unlock()
		return errors.New("kv: consensus rpc timeout")
	}
}
func (t *Transport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return t.call(id, target, "append", args, resp)
}
func (t *Transport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.call(id, target, "vote", args, resp)
}
func (t *Transport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	return t.call(id, target, "prevote", args, resp)
}
func (t *Transport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	if data != nil {
		_, _ = io.Copy(io.Discard, data)
	}
	return t.call(id, target, "snapshot", args, resp)
}
func (t *Transport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return t.call(id, target, "timeout", args, resp)
}
func (t *Transport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return &pipeline{t: t, id: id, target: target, ch: make(chan raft.AppendFuture, 64)}, nil
}

type pipeline struct {
	t      *Transport
	id     raft.ServerID
	target raft.ServerAddress
	ch     chan raft.AppendFuture
}

func (p *pipeline) AppendEntries(a *raft.AppendEntriesRequest, r *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	f := &future{req: a, resp: r, start: time.Now(), done: make(chan struct{})}
	go func() { f.err = p.t.AppendEntries(p.id, p.target, a, r); close(f.done); p.ch <- f }()
	return f, nil
}
func (p *pipeline) Consumer() <-chan raft.AppendFuture { return p.ch }
func (p *pipeline) Close() error                       { return nil }

type future struct {
	req   *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
	start time.Time
	done  chan struct{}
	err   error
}

func (f *future) Error() error                          { <-f.done; return f.err }
func (f *future) Start() time.Time                      { return f.start }
func (f *future) Request() *raft.AppendEntriesRequest   { return f.req }
func (f *future) Response() *raft.AppendEntriesResponse { return f.resp }
