package kv

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/hashicorp/raft"
)

type NodeOptions struct {
	ID            raft.ServerID
	Address       raft.ServerAddress
	Transport     raft.Transport
	FSM           *FSM
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
}

type Node struct {
	raft *raft.Raft
	fsm  *FSM
}

func NewNode(opts NodeOptions) (*Node, error) {
	if opts.ID == "" || opts.Address == "" || opts.Transport == nil {
		return nil, errors.New("kv: raft node identity, address and transport are required")
	}
	if opts.FSM == nil {
		opts.FSM = NewFSM()
	}
	if opts.LogStore == nil {
		opts.LogStore = raft.NewInmemStore()
	}
	if opts.StableStore == nil {
		opts.StableStore = raft.NewInmemStore()
	}
	if opts.SnapshotStore == nil {
		opts.SnapshotStore = raft.NewInmemSnapshotStore()
	}
	cfg := raft.DefaultConfig()
	cfg.LocalID = opts.ID
	cfg.ProtocolVersion = raft.ProtocolVersionMax
	cfg.PreVoteDisabled = false
	r, err := raft.NewRaft(cfg, opts.FSM, opts.LogStore, opts.StableStore, opts.SnapshotStore, opts.Transport)
	if err != nil {
		return nil, err
	}
	return &Node{raft: r, fsm: opts.FSM}, nil
}
func (n *Node) Raft() *raft.Raft { return n.raft }
func (n *Node) FSM() *FSM        { return n.fsm }
func (n *Node) Bootstrap(peers []raft.Server) error {
	return n.raft.BootstrapCluster(raft.Configuration{Servers: peers}).Error()
}
func (n *Node) Apply(ctx context.Context, cmd Command) (Result, error) {
	if n == nil || n.raft == nil {
		return Result{}, errors.New("kv: raft node is nil")
	}
	data, err := jsonMarshal(cmd)
	if err != nil {
		return Result{}, err
	}
	f := n.raft.Apply(data, 10*time.Second)
	if err := f.Error(); err != nil {
		return Result{}, err
	}
	if result, ok := f.Response().(Result); ok {
		return result, nil
	}
	return Result{}, errors.New("kv: unexpected raft response")
}
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
