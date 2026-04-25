package cluster

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	meshQueryLoggedInUsersRequestKind  = "logged_in_users.request"
	meshQueryLoggedInUsersResponseKind = "logged_in_users.response"
)

func (m *Manager) routeMeshLoggedInUsersRequest(ctx context.Context, req *internalproto.QueryLoggedInUsersRequest) error {
	if req == nil {
		return errors.New("query logged-in users request cannot be empty")
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	envelope := &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_QueryRequest{
			QueryRequest: &mesh.QueryRequest{
				RequestId: req.RequestId,
				Kind:      meshQueryLoggedInUsersRequestKind,
				Payload:   payload,
			},
		},
	}
	return m.routeMeshEnvelope(ctx, req.TargetNodeId, mesh.TrafficControlQuery, envelope)
}

func (m *Manager) routeMeshLoggedInUsersResponse(ctx context.Context, resp *internalproto.QueryLoggedInUsersResponse) error {
	if resp == nil {
		return errors.New("query logged-in users response cannot be empty")
	}
	payload, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	envelope := &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_QueryResponse{
			QueryResponse: &mesh.QueryResponse{
				RequestId: resp.RequestId,
				Payload:   payload,
			},
		},
	}
	return m.routeMeshEnvelope(ctx, resp.OriginNodeId, mesh.TrafficControlQuery, envelope)
}

func (m *Manager) routeMeshMembershipUpdate(ctx context.Context, targetNodeID int64, update *internalproto.MembershipUpdate) error {
	if update == nil {
		return errors.New("membership update cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficControlCritical, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_MembershipUpdate{
			MembershipUpdate: &mesh.MembershipUpdate{
				MembershipUpdate: proto.Clone(update).(*internalproto.MembershipUpdate),
			},
		},
	})
}

func (m *Manager) handleMeshQueryEnvelope(ctx context.Context, packet *mesh.ForwardedPacket, envelope *mesh.ClusterEnvelope) error {
	if m == nil || envelope == nil {
		return nil
	}
	switch body := envelope.Body.(type) {
	case *mesh.ClusterEnvelope_QueryRequest:
		if body.QueryRequest == nil {
			return errors.New("mesh query request body cannot be empty")
		}
		return m.handleMeshLoggedInUsersRequest(ctx, packet, body.QueryRequest)
	case *mesh.ClusterEnvelope_QueryResponse:
		if body.QueryResponse == nil {
			return errors.New("mesh query response body cannot be empty")
		}
		return m.handleMeshLoggedInUsersResponse(ctx, body.QueryResponse)
	default:
		return fmt.Errorf("unsupported mesh query envelope %T", envelope.Body)
	}
}

func (m *Manager) handleMeshEnvelope(ctx context.Context, packet *mesh.ForwardedPacket, envelope *mesh.ClusterEnvelope) error {
	if m == nil || envelope == nil {
		return nil
	}
	switch body := envelope.Body.(type) {
	case *mesh.ClusterEnvelope_QueryRequest, *mesh.ClusterEnvelope_QueryResponse:
		return m.handleMeshQueryEnvelope(ctx, packet, envelope)
	case *mesh.ClusterEnvelope_ReplicationBatch:
		return m.handleMeshReplicationBatchEnvelope(packet, body.ReplicationBatch)
	case *mesh.ClusterEnvelope_PullRequest:
		return m.handleMeshPullRequestEnvelope(packet, body.PullRequest)
	case *mesh.ClusterEnvelope_ReplicationAck:
		return m.handleMeshReplicationAckEnvelope(packet, body.ReplicationAck)
	case *mesh.ClusterEnvelope_SnapshotManifest:
		return m.handleMeshSnapshotManifestEnvelope(packet, body.SnapshotManifest)
	case *mesh.ClusterEnvelope_SnapshotChunk:
		return m.handleMeshSnapshotChunkEnvelope(packet, body.SnapshotChunk)
	case *mesh.ClusterEnvelope_MembershipUpdate:
		return m.handleMeshMembershipUpdateEnvelope(packet, body.MembershipUpdate)
	default:
		return fmt.Errorf("unsupported mesh envelope %T", envelope.Body)
	}
}

func (m *Manager) routeMeshReplicationBatch(ctx context.Context, targetNodeID int64, sequence uint64, sentAtHlc string, batch *internalproto.EventBatch) error {
	if batch == nil {
		return errors.New("mesh replication batch cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficReplicationStream, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_ReplicationBatch{
			ReplicationBatch: &mesh.ReplicationBatch{
				OriginNodeId: m.cfg.NodeID,
				Sequence:     sequence,
				SentAtHlc:    sentAtHlc,
				EventBatch:   proto.Clone(batch).(*internalproto.EventBatch),
			},
		},
	})
}

func (m *Manager) routeMeshPullRequest(ctx context.Context, targetNodeID int64, pull *internalproto.PullEvents) error {
	if pull == nil {
		return errors.New("mesh pull request cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficReplicationStream, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_PullRequest{
			PullRequest: &mesh.PullRequest{
				OriginNodeId: m.cfg.NodeID,
				PullEvents:   proto.Clone(pull).(*internalproto.PullEvents),
			},
		},
	})
}

func (m *Manager) routeMeshReplicationAck(ctx context.Context, targetNodeID int64, ack *internalproto.Ack) error {
	if ack == nil {
		return errors.New("mesh replication ack cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficControlCritical, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_ReplicationAck{
			ReplicationAck: &mesh.ReplicationAck{
				Ack: proto.Clone(ack).(*internalproto.Ack),
			},
		},
	})
}

func (m *Manager) routeMeshSnapshotManifest(ctx context.Context, targetNodeID int64, digest *internalproto.SnapshotDigest) error {
	if digest == nil {
		return errors.New("mesh snapshot manifest cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficSnapshotBulk, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_SnapshotManifest{
			SnapshotManifest: &mesh.SnapshotManifest{
				SnapshotDigest: proto.Clone(digest).(*internalproto.SnapshotDigest),
			},
		},
	})
}

func (m *Manager) routeMeshSnapshotChunk(ctx context.Context, targetNodeID int64, chunk *internalproto.SnapshotChunk) error {
	if chunk == nil {
		return errors.New("mesh snapshot chunk cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficSnapshotBulk, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_SnapshotChunk{
			SnapshotChunk: &mesh.SnapshotChunk{
				SnapshotChunk: proto.Clone(chunk).(*internalproto.SnapshotChunk),
			},
		},
	})
}

func (m *Manager) handleMeshReplicationBatchEnvelope(packet *mesh.ForwardedPacket, batch *mesh.ReplicationBatch) error {
	if batch == nil {
		return errors.New("mesh replication batch body cannot be empty")
	}
	eventBatch := batch.GetEventBatch()
	if eventBatch == nil {
		return errors.New("mesh replication event batch cannot be empty")
	}
	sess, err := m.meshSessionForPacket(packet, batch.GetOriginNodeId())
	if err != nil {
		return err
	}
	return m.handleEventBatch(sess, &internalproto.Envelope{
		NodeId:    sessionPeerIDForEnvelope(sess, batch.GetOriginNodeId()),
		Sequence:  batch.GetSequence(),
		SentAtHlc: batch.GetSentAtHlc(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: proto.Clone(eventBatch).(*internalproto.EventBatch),
		},
	})
}

func (m *Manager) handleMeshPullRequestEnvelope(packet *mesh.ForwardedPacket, pull *mesh.PullRequest) error {
	if pull == nil {
		return errors.New("mesh pull request body cannot be empty")
	}
	pullEvents := pull.GetPullEvents()
	if pullEvents == nil {
		return errors.New("mesh pull events cannot be empty")
	}
	sess, err := m.meshSessionForPacket(packet, pull.GetOriginNodeId())
	if err != nil {
		return err
	}
	return m.handlePullEvents(sess, &internalproto.Envelope{
		NodeId: sessionPeerIDForEnvelope(sess, pull.GetOriginNodeId()),
		Body: &internalproto.Envelope_PullEvents{
			PullEvents: proto.Clone(pullEvents).(*internalproto.PullEvents),
		},
	})
}

func (m *Manager) handleMeshReplicationAckEnvelope(packet *mesh.ForwardedPacket, ack *mesh.ReplicationAck) error {
	if ack == nil || ack.GetAck() == nil {
		return errors.New("mesh replication ack body cannot be empty")
	}
	sess, err := m.meshSessionForPacket(packet, ack.GetAck().GetNodeId())
	if err != nil {
		return err
	}
	return m.handleAck(sess, &internalproto.Envelope{
		NodeId: sessionPeerIDForEnvelope(sess, ack.GetAck().GetNodeId()),
		Body: &internalproto.Envelope_Ack{
			Ack: proto.Clone(ack.GetAck()).(*internalproto.Ack),
		},
	})
}

func (m *Manager) handleMeshSnapshotManifestEnvelope(packet *mesh.ForwardedPacket, manifest *mesh.SnapshotManifest) error {
	if manifest == nil || manifest.GetSnapshotDigest() == nil {
		return errors.New("mesh snapshot manifest body cannot be empty")
	}
	sess, err := m.meshSessionForPacket(packet, packetSourceNodeID(packet))
	if err != nil {
		return err
	}
	return m.handleSnapshotDigest(sess, &internalproto.Envelope{
		NodeId: sessionPeerIDForEnvelope(sess, packetSourceNodeID(packet)),
		Body: &internalproto.Envelope_SnapshotDigest{
			SnapshotDigest: proto.Clone(manifest.GetSnapshotDigest()).(*internalproto.SnapshotDigest),
		},
	})
}

func (m *Manager) handleMeshSnapshotChunkEnvelope(packet *mesh.ForwardedPacket, chunk *mesh.SnapshotChunk) error {
	if chunk == nil || chunk.GetSnapshotChunk() == nil {
		return errors.New("mesh snapshot chunk body cannot be empty")
	}
	sess, err := m.meshSessionForPacket(packet, packetSourceNodeID(packet))
	if err != nil {
		return err
	}
	return m.handleSnapshotChunk(sess, &internalproto.Envelope{
		NodeId: sessionPeerIDForEnvelope(sess, packetSourceNodeID(packet)),
		Body: &internalproto.Envelope_SnapshotChunk{
			SnapshotChunk: proto.Clone(chunk.GetSnapshotChunk()).(*internalproto.SnapshotChunk),
		},
	})
}

func (m *Manager) handleMeshMembershipUpdateEnvelope(packet *mesh.ForwardedPacket, update *mesh.MembershipUpdate) error {
	if update == nil || update.GetMembershipUpdate() == nil {
		return errors.New("mesh membership update body cannot be empty")
	}
	sourceNodeID := packetSourceNodeID(packet)
	if sourceNodeID <= 0 {
		return errors.New("mesh membership update source node id cannot be empty")
	}
	body := update.GetMembershipUpdate()
	if body.GetOriginNodeId() != sourceNodeID {
		return fmt.Errorf("mesh membership update origin mismatch: got %d want %d", body.GetOriginNodeId(), sourceNodeID)
	}
	return m.handleMembershipUpdateBody(sourceNodeID, body)
}

func (m *Manager) meshSessionForPacket(packet *mesh.ForwardedPacket, fallbackPeerID int64) (*session, error) {
	peerID := packetSourceNodeID(packet)
	if peerID <= 0 {
		peerID = fallbackPeerID
	}
	if peerID <= 0 {
		return nil, errors.New("mesh envelope source node id cannot be empty")
	}
	sess := m.meshPeerSession(peerID)
	if sess == nil {
		return nil, fmt.Errorf("mesh envelope peer session cannot be empty for %d", peerID)
	}
	return sess, nil
}

func packetSourceNodeID(packet *mesh.ForwardedPacket) int64 {
	if packet == nil {
		return 0
	}
	return packet.GetSourceNodeId()
}

func sessionPeerIDForEnvelope(sess *session, fallbackPeerID int64) int64 {
	if sess != nil && sess.peerID > 0 {
		return sess.peerID
	}
	return fallbackPeerID
}

func (m *Manager) handleMeshLoggedInUsersRequest(ctx context.Context, packet *mesh.ForwardedPacket, query *mesh.QueryRequest) error {
	if query.Kind != meshQueryLoggedInUsersRequestKind {
		return fmt.Errorf("unsupported mesh query request kind %q", query.Kind)
	}
	req := &internalproto.QueryLoggedInUsersRequest{}
	if err := proto.Unmarshal(query.Payload, req); err != nil {
		return err
	}
	if req.RequestId == 0 {
		return errors.New("query logged-in users request id cannot be empty")
	}
	if req.OriginNodeId <= 0 {
		return errors.New("query logged-in users origin node id cannot be empty")
	}
	if req.TargetNodeId <= 0 {
		return errors.New("query logged-in users target node id cannot be empty")
	}
	if req.TargetNodeId != m.cfg.NodeID {
		return fmt.Errorf("mesh query delivered to node %d for target %d", m.cfg.NodeID, req.TargetNodeId)
	}
	response := &internalproto.QueryLoggedInUsersResponse{
		RequestId:     req.RequestId,
		TargetNodeId:  req.TargetNodeId,
		OriginNodeId:  req.OriginNodeId,
		RemainingHops: req.RemainingHops,
	}
	if packet != nil && packet.TtlHops > 0 {
		response.RemainingHops = int32(packet.TtlHops)
	}
	users, err := m.listLocalLoggedInUsers(ctx)
	if err != nil {
		response.ErrorCode = "service_unavailable"
		response.ErrorMessage = err.Error()
	} else {
		response.Items = clusterLoggedInUsers(users)
	}
	return m.routeMeshLoggedInUsersResponse(ctx, response)
}

func (m *Manager) handleMeshLoggedInUsersResponse(ctx context.Context, query *mesh.QueryResponse) error {
	_ = ctx
	resp := &internalproto.QueryLoggedInUsersResponse{}
	if err := proto.Unmarshal(query.Payload, resp); err != nil {
		return err
	}
	if resp.RequestId == 0 {
		return errors.New("query logged-in users response id cannot be empty")
	}
	if resp.OriginNodeId <= 0 {
		return errors.New("query logged-in users response origin node id cannot be empty")
	}
	if resp.TargetNodeId <= 0 {
		return errors.New("query logged-in users response target node id cannot be empty")
	}
	if resp.OriginNodeId != m.cfg.NodeID {
		return m.routeMeshLoggedInUsersResponse(context.Background(), resp)
	}
	if !m.resolveLoggedInUsersQuery(resp.RequestId, loggedInUsersQueryResult{response: resp}) {
		m.logDebug("query_logged_in_users_response_ignored").
			Uint64("request_id", resp.RequestId).
			Int64("origin_node_id", resp.OriginNodeId).
			Int64("target_node_id", resp.TargetNodeId).
			Int32("remaining_hops", resp.RemainingHops).
			Msg("ignoring late logged-in users response without pending origin query")
	}
	return nil
}

func (m *Manager) handleMeshForwardedPacket(ctx context.Context, packet *mesh.ForwardedPacket) error {
	_ = ctx
	if m == nil || packet == nil {
		return nil
	}
	if packet.TrafficClass != mesh.TrafficTransientInteractive {
		return fmt.Errorf("unsupported non-transient forwarded packet traffic class %s", packet.TrafficClass.String())
	}
	body := &internalproto.TransientPacket{}
	if err := proto.Unmarshal(packet.Payload, body); err != nil {
		return err
	}
	if body.Recipient == nil {
		return fmt.Errorf("transient packet recipient cannot be empty")
	}
	if body.Sender == nil {
		return fmt.Errorf("transient packet sender cannot be empty")
	}
	transient := store.TransientPacket{
		PacketID:     packet.PacketId,
		SourceNodeID: packet.SourceNodeId,
		TargetNodeID: packet.TargetNodeId,
		Recipient:    store.UserKey{NodeID: body.Recipient.NodeId, UserID: body.Recipient.UserId},
		Sender:       store.UserKey{NodeID: body.Sender.NodeId, UserID: body.Sender.UserId},
		Body:         append([]byte(nil), body.Body...),
		DeliveryMode: clusterDeliveryModeToStore(body.DeliveryMode),
		TTLHops:      int32(packet.TtlHops),
	}
	if transient.TargetNodeID != m.cfg.NodeID {
		return fmt.Errorf("mesh transient delivered to node %d for target %d", m.cfg.NodeID, transient.TargetNodeID)
	}
	m.logDebug("transient_packet_received").
		Uint64("packet_id", transient.PacketID).
		Int64("source_node_id", transient.SourceNodeID).
		Int64("target_node_id", transient.TargetNodeID).
		Int32("ttl_hops", transient.TTLHops).
		Msg("transient packet received via mesh")
	m.deliverTransientLocal(transient)
	return nil
}

func (m *Manager) routeOrQueueTransientPacket(ctx context.Context, packet store.TransientPacket) {
	m.routeOrQueueMeshTransient(ctx, packet)
}

func (m *Manager) routeOrQueueMeshTransient(ctx context.Context, packet store.TransientPacket) {
	if packet.TargetNodeID == m.cfg.NodeID {
		m.removeQueuedTransientPacket(packet)
		m.deliverTransientLocal(packet)
		return
	}
	if packet.TTLHops <= 0 {
		addPacketLogFields(m.logWarn("transient_packet_dropped", nil), packet).
			Str("reason", "ttl_exhausted").
			Msg("dropping transient packet")
		return
	}
	binding := m.MeshRuntime()
	if binding == nil {
		if packet.DeliveryMode != store.DeliveryModeRouteRetry {
			addPacketLogFields(m.logWarn("transient_packet_dropped", nil), packet).
				Str("reason", "mesh_unavailable").
				Msg("dropping transient packet without mesh runtime")
			return
		}
		m.queueTransientPacket(packet)
		return
	}
	payload, err := proto.Marshal(transientPacketProto(packet))
	if err != nil {
		addPacketLogFields(m.logWarn("transient_packet_dropped", err), packet).
			Str("reason", "marshal_failed").
			Msg("dropping transient packet")
		return
	}
	err = m.forwardMeshPayloadWithPacketIDAndTTL(ctx, packet.TargetNodeID, mesh.TrafficTransientInteractive, packet.PacketID, uint32(packet.TTLHops), payload)
	if err == nil {
		m.removeQueuedTransientPacket(packet)
		addPacketLogFields(m.logInfo("transient_packet_forwarded"), packet).
			Msg("forwarding transient packet via mesh")
		return
	}
	if errors.Is(err, mesh.ErrDuplicatePacket) {
		m.removeQueuedTransientPacket(packet)
		addPacketLogFields(m.logInfo("transient_packet_deduplicated"), packet).
			Msg("dropping already-forwarded transient packet from retry path")
		return
	}
	if packet.DeliveryMode != store.DeliveryModeRouteRetry {
		addPacketLogFields(m.logWarn("transient_packet_dropped", err), packet).
			Str("reason", "no_route").
			Msg("dropping transient packet without route")
		return
	}
	m.queueTransientPacket(packet)
}

func (m *Manager) nextMeshForwardPacketID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextMeshPacketID++
	return m.nextMeshPacketID
}

func (m *Manager) meshPeerSession(peerID int64) *session {
	if m == nil || peerID <= 0 || peerID == m.cfg.NodeID {
		return nil
	}
	m.mu.Lock()
	peer := m.peers[peerID]
	if peer == nil {
		peer = &peerState{
			sessions: make(map[uint64]*session),
		}
		m.peers[peerID] = peer
	}
	if peer.sessions == nil {
		peer.sessions = make(map[uint64]*session)
	}
	if peer.active != nil && peer.active.conn == nil && peer.active.peerID == peerID {
		sess := peer.active
		m.mu.Unlock()
		return sess
	}
	m.nextConnectionID++
	connectionID := m.nextConnectionID
	sess := &session{
		manager:                 m,
		peerID:                  peerID,
		connectionID:            connectionID,
		outbound:                true,
		send:                    make(chan *internalproto.Envelope, outboundQueueSize),
		remoteOriginProgress:    make(map[int64]uint64),
		pendingPulls:            make(map[int64]pendingPullState),
		pendingTimeSync:         make(map[uint64]chan timeSyncResult),
		pendingSnapshotParts:    make(map[string]struct{}),
		replicationReady:        true,
		remoteSnapshotVersion:   internalproto.SnapshotVersion,
		remoteMessageWindowSize: m.cfg.MessageWindowSize,
		supportsMembership:      !m.cfg.DiscoveryDisabled,
	}
	peer.active = sess
	peer.clockState = clockStateProbing
	peer.sessions[sess.connectionID] = sess
	m.refreshNodeClockStateLocked()
	m.mu.Unlock()
	return sess
}

func (m *Manager) ensureMeshPeerSessions() {
	if m == nil {
		return
	}
	binding := m.MeshRuntime()
	if binding == nil {
		return
	}
	snapshot := binding.TopologyStore().Snapshot()
	for nodeID := range snapshot.Nodes {
		if nodeID <= 0 || nodeID == m.cfg.NodeID {
			continue
		}
		m.meshPeerSession(nodeID)
	}
}

func (m *Manager) meshPeerSessions() []*session {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*session, 0, len(m.peers))
	for peerID, peer := range m.peers {
		if peerID <= 0 || peerID == m.cfg.NodeID || peer == nil {
			continue
		}
		if peer.active != nil && peer.active.conn == nil {
			out = append(out, peer.active)
		}
	}
	return out
}
