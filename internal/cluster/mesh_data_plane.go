package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	meshQueryResolveUserSessionsRequestKind  = "resolve_user_sessions.request"
	meshQueryResolveUserSessionsResponseKind = "resolve_user_sessions.response"
)

func (m *Manager) routeMeshResolveUserSessionsRequest(ctx context.Context, req *internalproto.QueryResolveUserSessionsRequest) error {
	if req == nil {
		return errors.New("query resolve user sessions request cannot be empty")
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	envelope := &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_QueryRequest{
			QueryRequest: &mesh.QueryRequest{
				RequestId: req.RequestId,
				Kind:      meshQueryResolveUserSessionsRequestKind,
				Payload:   payload,
			},
		},
	}
	return m.routeMeshEnvelope(ctx, req.TargetNodeId, mesh.TrafficControlQuery, envelope)
}

func (m *Manager) routeMeshResolveUserSessionsResponse(ctx context.Context, resp *internalproto.QueryResolveUserSessionsResponse) error {
	if resp == nil {
		return errors.New("query resolve user sessions response cannot be empty")
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
				Kind:      meshQueryResolveUserSessionsResponseKind,
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
				MembershipUpdate: update,
			},
		},
	})
}

func (m *Manager) routeMeshPresenceUpdate(ctx context.Context, targetNodeID int64, snapshot *internalproto.OnlinePresenceSnapshot) error {
	if snapshot == nil {
		return errors.New("online presence snapshot cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficControlCritical, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_PresenceUpdate{
			PresenceUpdate: &mesh.MeshPresenceUpdate{
				PresenceUpdate: snapshot,
			},
		},
	})
}

func (m *Manager) routeMeshConnectivityRumor(ctx context.Context, targetNodeID int64, rumor *internalproto.NodeConnectivityRumor) error {
	if rumor == nil {
		return errors.New("connectivity rumor cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficControlCritical, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_ConnectivityRumor{
			ConnectivityRumor: &mesh.MeshConnectivityRumor{
				ConnectivityRumor: rumor,
			},
		},
	})
}

func (m *Manager) broadcastConnectivityRumor(rumor *internalproto.NodeConnectivityRumor) {
	m.forwardConnectivityRumor(rumor, 0)
}

func (m *Manager) forwardConnectivityRumor(rumor *internalproto.NodeConnectivityRumor, excludePeerNodeID int64) {
	if m == nil || rumor == nil {
		return
	}
	for _, sess := range m.localPresenceSessions() {
		if sess == nil || sess.peerID == excludePeerNodeID {
			continue
		}
		m.sendConnectivityRumor(sess, rumor)
	}
}

func (m *Manager) sendConnectivityRumor(sess *session, rumor *internalproto.NodeConnectivityRumor) {
	if m == nil || sess == nil || rumor == nil || sess.isClosed() || m.MeshRuntime() == nil {
		return
	}
	if err := m.routeMeshConnectivityRumor(context.Background(), sess.peerID, rumor); err != nil {
		m.logSessionWarn("mesh_connectivity_rumor_forward_failed", sess, err).
			Msg("failed to forward connectivity rumor over mesh")
	}
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
		return m.handleMeshQueryRequest(ctx, packet, body.QueryRequest)
	case *mesh.ClusterEnvelope_QueryResponse:
		if body.QueryResponse == nil {
			return errors.New("mesh query response body cannot be empty")
		}
		return m.handleMeshQueryResponse(ctx, body.QueryResponse)
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
	case *mesh.ClusterEnvelope_PresenceUpdate:
		return m.handleMeshPresenceUpdateEnvelope(packet, body.PresenceUpdate)
	case *mesh.ClusterEnvelope_ConnectivityRumor:
		return m.handleMeshConnectivityRumorEnvelope(packet, body.ConnectivityRumor)
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
				EventBatch:   batch,
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
				PullEvents:   pull,
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
				Ack: ack,
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
				SnapshotDigest: digest,
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
				SnapshotChunk: chunk,
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
			EventBatch: eventBatch,
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
			PullEvents: pullEvents,
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
			Ack: ack.GetAck(),
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
			SnapshotDigest: manifest.GetSnapshotDigest(),
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
			SnapshotChunk: chunk.GetSnapshotChunk(),
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

func (m *Manager) handleMeshQueryRequest(ctx context.Context, packet *mesh.ForwardedPacket, query *mesh.QueryRequest) error {
	if query == nil {
		return errors.New("mesh query request cannot be empty")
	}
	switch query.Kind {
	case meshQueryResolveUserSessionsRequestKind:
		return m.handleMeshResolveUserSessionsRequest(ctx, packet, query)
	default:
		return fmt.Errorf("unsupported mesh query request kind %q", query.Kind)
	}
}

func (m *Manager) handleMeshQueryResponse(ctx context.Context, query *mesh.QueryResponse) error {
	if query == nil {
		return errors.New("mesh query response cannot be empty")
	}
	switch query.Kind {
	case meshQueryResolveUserSessionsResponseKind:
		return m.handleMeshResolveUserSessionsResponse(ctx, query)
	default:
		return fmt.Errorf("unsupported mesh query response kind %q", query.Kind)
	}
}

func (m *Manager) handleMeshResolveUserSessionsRequest(ctx context.Context, packet *mesh.ForwardedPacket, query *mesh.QueryRequest) error {
	req := &internalproto.QueryResolveUserSessionsRequest{}
	if err := proto.Unmarshal(query.Payload, req); err != nil {
		return err
	}
	if req.RequestId == 0 {
		return errors.New("query resolve user sessions request id cannot be empty")
	}
	if req.OriginNodeId <= 0 {
		return errors.New("query resolve user sessions origin node id cannot be empty")
	}
	if req.TargetNodeId <= 0 {
		return errors.New("query resolve user sessions target node id cannot be empty")
	}
	if req.User == nil {
		return errors.New("query resolve user sessions user cannot be empty")
	}
	if req.TargetNodeId != m.cfg.NodeID {
		return fmt.Errorf("mesh query delivered to node %d for target %d", m.cfg.NodeID, req.TargetNodeId)
	}
	response := &internalproto.QueryResolveUserSessionsResponse{
		RequestId:     req.RequestId,
		TargetNodeId:  req.TargetNodeId,
		OriginNodeId:  req.OriginNodeId,
		RemainingHops: req.RemainingHops,
		User:          proto.Clone(req.User).(*internalproto.ClusterUserRef),
	}
	if packet != nil && packet.TtlHops > 0 {
		response.RemainingHops = int32(packet.TtlHops)
	}
	user := store.UserKey{NodeID: req.User.GetNodeId(), UserID: req.User.GetUserId()}
	if user.Validate() != nil {
		response.ErrorCode = "invalid_request"
		response.ErrorMessage = "target user is invalid"
		return m.routeMeshResolveUserSessionsResponse(ctx, response)
	}
	for _, session := range m.localUserSessions(user) {
		response.Items = append(response.Items, &internalproto.ClusterSessionRef{
			ServingNodeId:    session.SessionRef.ServingNodeID,
			SessionId:        session.SessionRef.SessionID,
			Transport:        session.Transport,
			TransientCapable: session.TransientCapable,
		})
	}
	return m.routeMeshResolveUserSessionsResponse(ctx, response)
}

func (m *Manager) handleMeshResolveUserSessionsResponse(ctx context.Context, query *mesh.QueryResponse) error {
	_ = ctx
	resp := &internalproto.QueryResolveUserSessionsResponse{}
	if err := proto.Unmarshal(query.Payload, resp); err != nil {
		return err
	}
	if resp.RequestId == 0 || resp.OriginNodeId <= 0 || resp.TargetNodeId <= 0 {
		return errors.New("query resolve user sessions response is invalid")
	}
	if resp.OriginNodeId != m.cfg.NodeID {
		return m.routeMeshResolveUserSessionsResponse(context.Background(), resp)
	}
	if !m.resolveResolveUserSessionsQuery(resp.RequestId, resolveUserSessionsQueryResult{response: resp}) {
		m.logDebug("query_resolve_user_sessions_response_ignored").
			Uint64("request_id", resp.RequestId).
			Int64("origin_node_id", resp.OriginNodeId).
			Int64("target_node_id", resp.TargetNodeId).
			Msg("ignoring late resolve user sessions response without pending origin query")
	}
	return nil
}

func (m *Manager) handleMeshPresenceUpdateEnvelope(packet *mesh.ForwardedPacket, update *mesh.MeshPresenceUpdate) error {
	if update == nil || update.GetPresenceUpdate() == nil {
		return errors.New("mesh online presence update body cannot be empty")
	}
	sourceNodeID := packetSourceNodeID(packet)
	if sourceNodeID <= 0 {
		return errors.New("mesh online presence update source node id cannot be empty")
	}
	body := update.GetPresenceUpdate()
	if body.GetOriginNodeId() != sourceNodeID {
		return fmt.Errorf("mesh online presence update origin mismatch: got %d want %d", body.GetOriginNodeId(), sourceNodeID)
	}
	if !m.applyOnlinePresenceSnapshot(body) {
		return nil
	}
	m.forwardOnlinePresence(body, sourceNodeID)
	return nil
}

func (m *Manager) handleMeshConnectivityRumorEnvelope(packet *mesh.ForwardedPacket, envelope *mesh.MeshConnectivityRumor) error {
	if envelope == nil || envelope.GetConnectivityRumor() == nil {
		return errors.New("mesh connectivity rumor body cannot be empty")
	}
	rumor := envelope.GetConnectivityRumor()
	if rumor.GetTargetNodeId() <= 0 || rumor.GetTargetRuntimeEpoch() == 0 || rumor.GetReporterNodeId() <= 0 || rumor.GetReporterRuntimeEpoch() == 0 {
		return errors.New("mesh connectivity rumor is invalid")
	}
	now := time.Now().UTC()

	m.mu.Lock()
	firstSeen := m.markConnectivityRumorSeenLocked(rumor, now)
	shouldForward := firstSeen
	refuteSelf := false
	if rumor.GetTargetNodeId() == m.cfg.NodeID {
		refuteSelf = rumor.GetTargetRuntimeEpoch() == m.localRuntimeEpoch
		if !refuteSelf {
			shouldForward = false
		}
	} else {
		currentEpoch := m.currentRuntimeEpochForNodeLocked(rumor.GetTargetNodeId())
		if currentEpoch > 0 && rumor.GetTargetRuntimeEpoch() < currentEpoch {
			shouldForward = false
		} else {
			m.rememberRemoteRuntimeEpochLocked(rumor.GetTargetNodeId(), rumor.GetTargetRuntimeEpoch())
			if m.directAdjacencyCounts[rumor.GetTargetNodeId()] == 0 {
				m.noteDisconnectSuspicionLocked(rumor, now)
			}
		}
	}
	m.mu.Unlock()

	if refuteSelf {
		m.broadcastOnlinePresence()
	}
	if shouldForward {
		m.forwardConnectivityRumor(rumor, packetSourceNodeID(packet))
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
	transient, err := transientPacketFromProto(packet.GetTransientPacket(), packet)
	if err != nil {
		return err
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
	err := m.forwardMeshTransientPacket(ctx, packet)
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
