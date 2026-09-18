package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/tursom/turntf/internal/kv"
	"github.com/tursom/turntf/internal/mesh"
)

func (m *Manager) startKVConsensus() error {
	cfg := m.cfg.KVConsensus
	if !cfg.Enabled {
		return nil
	}
	voters := make([]raft.Server, 0, len(cfg.Voters))
	for _, raw := range cfg.Voters {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("kv voter %q: %w", raw, err)
		}
		voters = append(voters, raft.Server{ID: raft.ServerID(raw), Address: raft.ServerAddress(raw)})
		_ = id
	}
	if err := kv.ValidateVoters(raft.ServerID(strconv.FormatInt(m.cfg.NodeID, 10)), voters); err != nil {
		return err
	}
	dataDir := cfg.DataDir
	if !filepath.IsAbs(dataDir) {
		dataDir = filepath.Join(".", dataDir)
	}
	transport := kv.NewTransport(raft.ServerAddress(strconv.FormatInt(m.cfg.NodeID, 10)), func(target raft.ServerAddress, payload []byte) error {
		return m.SendConsensusMessage(m.ctx, parseConsensusTarget(target), cfg.GroupID, 0, payload)
	})
	node, err := kv.NewPersistentNode(kv.NodeOptions{ID: raft.ServerID(strconv.FormatInt(m.cfg.NodeID, 10)), Address: raft.ServerAddress(strconv.FormatInt(m.cfg.NodeID, 10)), Transport: transport}, dataDir)
	if err != nil {
		return err
	}
	m.kvNode = node
	m.kvTransport = transport
	m.SetConsensusMessageHandler(func(_ context.Context, _ int64, message *mesh.ConsensusMessage) error {
		return transport.Deliver(message.GetPayload())
	})
	if cfg.Bootstrap && !node.HasExistingState() {
		if err := node.Bootstrap(voters); err != nil {
			_ = node.Close()
			m.kvNode = nil
			m.kvTransport = nil
			return err
		}
	}
	if !cfg.Bootstrap && !node.HasExistingState() {
		_ = node.Close()
		m.kvNode = nil
		m.kvTransport = nil
		return fmt.Errorf("kv consensus node has no durable membership; configure bootstrap on exactly one voter")
	}
	return nil
}
func parseConsensusTarget(target raft.ServerAddress) int64 {
	id, _ := strconv.ParseInt(string(target), 10, 64)
	return id
}
