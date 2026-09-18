package kv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// PersistentNode owns the durable Raft log/stable store and snapshot store.
// The caller is responsible for closing it before process shutdown.
type PersistentNode struct {
	*Node
	bolt *raftboltdb.BoltStore
}

func NewPersistentNode(opts NodeOptions, dataDir string) (*PersistentNode, error) {
	if dataDir == "" {
		return nil, errors.New("kv: data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.bolt"))
	if err != nil {
		return nil, err
	}
	snapshotDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0700); err != nil {
		_ = bolt.Close()
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(snapshotDir, 1, os.Stderr)
	if err != nil {
		_ = bolt.Close()
		return nil, err
	}
	opts.LogStore = bolt
	opts.StableStore = bolt
	opts.SnapshotStore = snapshots
	node, err := NewNode(opts)
	if err != nil {
		_ = bolt.Close()
		return nil, err
	}
	return &PersistentNode{Node: node, bolt: bolt}, nil
}
func (n *PersistentNode) Close() error {
	if n == nil {
		return nil
	}
	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	if n.bolt != nil {
		return n.bolt.Close()
	}
	return nil
}

// ValidateVoters rejects ambiguous or unsafe initial membership. Membership is
// intentionally separate from cluster.peers transport discovery.
func ValidateVoters(local raft.ServerID, voters []raft.Server) error {
	if len(voters) == 0 || len(voters)%2 == 0 {
		return fmt.Errorf("kv: voter count must be a non-zero odd number")
	}
	seen := make(map[raft.ServerID]struct{}, len(voters))
	found := false
	for _, v := range voters {
		if v.ID == "" || v.Address == "" {
			return errors.New("kv: voter id and address are required")
		}
		if _, ok := seen[v.ID]; ok {
			return fmt.Errorf("kv: duplicate voter %q", v.ID)
		}
		seen[v.ID] = struct{}{}
		if v.ID == local {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("kv: local node %q is not a voter", local)
	}
	return nil
}
