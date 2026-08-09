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

// 网格查询类型常量。
const (
	meshQueryResolveUserSessionsRequestKind  = "resolve_user_sessions.request"
	meshQueryResolveUserSessionsResponseKind = "resolve_user_sessions.response"
)

// routeMeshResolveUserSessionsRequest 通过网格路由用户会话解析请求。
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

// routeMeshResolveUserSessionsResponse 通过网格路由用户会话解析响应。
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

// routeMeshMembershipUpdate 通过网格路由成员资格更新（control_critical流量类别）。
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

// routeMeshPresenceUpdate 通过网格路由在线状态更新。
func (m *Manager) routeMeshPresenceUpdate(ctx context.Context, targetNodeID int64, update *internalproto.OnlinePresenceUpdate) error {
	if update == nil {
		return errors.New("online presence update cannot be empty")
	}
	return m.routeMeshEnvelope(ctx, targetNodeID, mesh.TrafficControlCritical, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_PresenceUpdate{
			PresenceUpdate: &mesh.MeshPresenceUpdate{
				PresenceUpdate: update,
			},
		},
	})
}

// routeMeshConnectivityRumor 通过网格路由连接性传闻。
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

// broadcastConnectivityRumor 向所有本地对等节点广播连接性传闻。
func (m *Manager) broadcastConnectivityRumor(rumor *internalproto.NodeConnectivityRumor) {
	m.forwardConnectivityRumor(rumor, 0)
}

// forwardConnectivityRumor 向所有本地活跃会话转发连接性传闻。
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

// sendConnectivityRumor 向单个会话发送连接性传闻。
func (m *Manager) sendConnectivityRumor(sess *session, rumor *internalproto.NodeConnectivityRumor) {
	if m == nil || sess == nil || rumor == nil || sess.isClosed() || m.MeshRuntime() == nil {
		return
	}
	if err := m.routeMeshConnectivityRumor(context.Background(), sess.peerID, rumor); err != nil {
		m.logMeshForwardFailure("mesh_connectivity_rumor_forward_failed", sess, err, "failed to forward connectivity rumor over mesh")
	}
}

// handleMeshQueryEnvelope 处理网格查询请求或响应。
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

// handleMeshEnvelope 是网格信封的中央分发器。
// 根据信封的oneof类型将请求路由到对应的处理函数。
// 支持的9种信封类型：查询、复制批次、拉取请求、复制确认、
// 快照清单、快照分块、成员资格更新、在线状态更新、连接性传闻。
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

// routeMeshReplicationBatch 通过网格路由复制事件批次（replication_stream流量类别）。
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

// routeMeshPullRequest 通过网格路由事件拉取请求。
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

// routeMeshReplicationAck 通过网格路由复制确认。
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

// routeMeshSnapshotManifest 通过网格路由快照摘要清单（snapshot_bulk流量类别）。
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

// routeMeshSnapshotChunk 通过网格路由快照分块。
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

// handleMeshReplicationBatchEnvelope 处理网格复制事件批次。
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

// handleMeshPullRequestEnvelope 处理网格事件拉取请求。
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

// handleMeshReplicationAckEnvelope 处理网格复制确认。
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

// handleMeshSnapshotManifestEnvelope 处理网格快照清单。
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

// handleMeshSnapshotChunkEnvelope 处理网格快照分块。
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

// handleMeshMembershipUpdateEnvelope 处理网格成员资格更新。
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

// meshSessionForPacket 获取数据包源节点对应的网格会话。
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

// packetSourceNodeID 返回数据包的源节点ID。
func packetSourceNodeID(packet *mesh.ForwardedPacket) int64 {
	if packet == nil {
		return 0
	}
	return packet.GetSourceNodeId()
}

// sessionPeerIDForEnvelope 返回用于构造信封节点ID的对等节点ID。
func sessionPeerIDForEnvelope(sess *session, fallbackPeerID int64) int64 {
	if sess != nil && sess.peerID > 0 {
		return sess.peerID
	}
	return fallbackPeerID
}

// handleMeshQueryRequest 分派网格查询请求到对应的处理函数。
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

// handleMeshQueryResponse 分派网格查询响应到对应的处理函数。
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

// handleMeshResolveUserSessionsRequest 处理解析用户会话的网格查询请求。
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

// handleMeshResolveUserSessionsResponse 处理解析用户会话的网格查询响应。
// 如果目标不是本地发起的查询则继续路由。
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

// handleMeshPresenceUpdateEnvelope 处理网格在线状态更新。
// origin 会分别路由到每个已知节点，因此接收端只需校验并应用。
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
	_, err := m.applyOnlinePresenceUpdate(body)
	return err
}

// handleMeshConnectivityRumorEnvelope 处理网格连接性传闻。
//
// 逻辑：
//   - 传闻针对本节点且纪元匹配 → 本节点被怀疑断开 → 广播在线状态自证
//   - 传闻针对其他节点且纪元较旧 → 忽略（已有更新信息）
//   - 传闻针对其他节点且首次见到 → 记录怀疑并转发
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

// handleMeshForwardedPacket 处理通过网格转发的数据包（瞬时消息流量）。
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

// routeOrQueueTransientPacket 路由或排队一个瞬态数据包。
// 目标为本地时立即投递；需要通过网格转发时尝试路由或排队重试。
func (m *Manager) routeOrQueueTransientPacket(ctx context.Context, packet store.TransientPacket) {
	m.routeOrQueueMeshTransient(ctx, packet)
}

// routeOrQueueMeshTransient 通过网格路由瞬态数据包，失败时排队重试。
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

// nextMeshForwardPacketID 返回下一个网格转发数据包ID。
func (m *Manager) nextMeshForwardPacketID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextMeshPacketID++
	return m.nextMeshPacketID
}

// meshPeerSession 返回或创建指定对等节点的网格会话。
// 网格会话是合成会话（没有实际传输连接），用于通过网格转发的消息处理。
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

// ensureMeshPeerSessions 为拓扑快照中的所有节点创建网格会话。
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

// meshPeerSessions 返回所有网格对等节点的合成会话。
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
