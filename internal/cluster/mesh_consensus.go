package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"

	"github.com/tursom/turntf/internal/mesh"
)

// SetConsensusMessageHandler registers the CP subsystem handler. The mesh
// runtime remains unaware of Raft or any other consensus implementation.
func (m *Manager) SetConsensusMessageHandler(handler func(context.Context, int64, *mesh.ConsensusMessage) error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.consensusHandler = handler
	m.mu.Unlock()
}

// ConsensusSender returns the adapter expected by internal/kv.Transport.
// ServerAddress is the decimal cluster node ID, keeping kv independent from
// Manager and from the concrete cluster transport implementation.
func (m *Manager) ConsensusSender(groupID string) func(target raft.ServerAddress, payload []byte) error {
	return func(target raft.ServerAddress, payload []byte) error {
		id, err := strconv.ParseInt(string(target), 10, 64)
		if err != nil || id <= 0 {
			return fmt.Errorf("invalid consensus target %q", target)
		}
		return m.SendConsensusMessage(context.Background(), id, groupID, 0, payload)
	}
}

func (m *Manager) SendConsensusMessage(ctx context.Context, targetNodeID int64, groupID string, messageID uint64, payload []byte) error {
	if m == nil || targetNodeID <= 0 || targetNodeID == m.cfg.NodeID {
		return errors.New("invalid consensus target")
	}
	if groupID == "" || len(payload) == 0 {
		return errors.New("consensus group and payload are required")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficConsensus, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_ConsensusMessage{ConsensusMessage: &mesh.ConsensusMessage{
			GroupId: groupID, SourceNodeId: m.cfg.NodeID, TargetNodeId: targetNodeID,
			MessageId: messageID, Payload: append([]byte(nil), payload...),
		}},
	})
}

func (m *Manager) handleMeshConsensusMessage(ctx context.Context, packet *mesh.ForwardedPacket, message *mesh.ConsensusMessage) error {
	if message == nil || message.GetGroupId() == "" || len(message.GetPayload()) == 0 {
		return errors.New("invalid consensus message")
	}
	if message.GetTargetNodeId() != 0 && message.GetTargetNodeId() != m.cfg.NodeID {
		return fmt.Errorf("consensus message target mismatch: %d", message.GetTargetNodeId())
	}
	m.mu.Lock()
	handler := m.consensusHandler
	m.mu.Unlock()
	if handler == nil {
		return errors.New("consensus handler is not configured")
	}
	source := message.GetSourceNodeId()
	if source <= 0 && packet != nil {
		source = packet.GetSourceNodeId()
	}
	return handler(ctx, source, message)
}
