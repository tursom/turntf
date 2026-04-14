package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func (m *Manager) runSession(sess *session) {
	m.logSessionEvent("peer_session_started", sess).
		Msg("peer session started")
	defer sess.close()
	defer m.deactivateSession(sess)

	hello, err := m.buildHelloEnvelope(sess)
	if err != nil {
		log.Warn().Err(err).Str("component", "cluster").Str("event", "build_hello_failed").Msg("build hello failed")
		return
	}

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		m.writeLoop(sess)
	}()

	sess.enqueue(hello)
	m.readLoop(sess)
	sess.close()
	<-writeDone
}

func (m *Manager) buildHelloEnvelope(sessions ...*session) (*internalproto.Envelope, error) {
	var sess *session
	if len(sessions) > 0 {
		sess = sessions[0]
	}
	originProgress, err := m.store.ListOriginProgress(context.Background())
	if err != nil {
		return nil, err
	}

	progress := make([]*internalproto.OriginProgress, 0, len(originProgress))
	for _, item := range originProgress {
		progress = append(progress, &internalproto.OriginProgress{
			OriginNodeId: item.OriginNodeID,
			LastEventId:  uint64(item.LastEventID),
		})
	}

	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_Hello{
			Hello: &internalproto.Hello{
				NodeId:             m.cfg.NodeID,
				AdvertiseAddr:      m.cfg.AdvertisePath,
				ProtocolVersion:    internalproto.ProtocolVersion,
				OriginProgress:     progress,
				SnapshotVersion:    internalproto.SnapshotVersion,
				MessageWindowSize:  uint32(m.cfg.MessageWindowSize),
				SupportsRouting:    m.routingSupported(),
				ConnectionId:       connectionIDForHello(sess),
				SupportsMembership: m.membershipSupported(),
			},
		},
	}, nil
}

func (m *Manager) writeLoop(sess *session) {
	for {
		select {
		case <-m.ctx.Done():
			closeTransport(sess.conn, "shutdown")
			return
		case envelope, ok := <-sess.send:
			if !ok {
				closeTransport(sess.conn, "session closed")
				return
			}

			data, err := m.marshalSignedEnvelope(envelope)
			if err != nil {
				log.Warn().Err(err).Str("component", "cluster").Int64("peer", sess.peerID).Str("event", "marshal_envelope_failed").Msg("marshal envelope failed")
				return
			}

			if err := sess.conn.Send(m.ctx, data); err != nil {
				return
			}
		}
	}
}

func (m *Manager) readLoop(sess *session) {
	for {
		data, err := sess.conn.Receive(m.ctx)
		if err != nil {
			return
		}

		var envelope internalproto.Envelope
		if err := proto.Unmarshal(data, &envelope); err != nil {
			return
		}
		if err := m.verifyEnvelope(&envelope); err != nil {
			return
		}

		if sess.peerID == 0 && envelope.GetHello() == nil {
			return
		}

		switch envelope.Body.(type) {
		case *internalproto.Envelope_Hello:
			if err := m.handleHello(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TimeSyncRequest:
			if err := m.handleTimeSyncRequest(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TimeSyncResponse:
			if err := m.handleTimeSyncResponse(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_QueryLoggedInUsersRequest:
			if err := m.handleQueryLoggedInUsersRequest(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_QueryLoggedInUsersResponse:
			if err := m.handleQueryLoggedInUsersResponse(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_MembershipUpdate:
			if err := m.handleMembershipUpdate(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_Ack:
			if err := m.handleAck(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_EventBatch:
			if err := m.handleEventBatch(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		case *internalproto.Envelope_PullEvents:
			if err := m.handlePullEvents(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_SnapshotDigest:
			if err := m.handleSnapshotDigest(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		case *internalproto.Envelope_SnapshotChunk:
			if err := m.handleSnapshotChunk(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		case *internalproto.Envelope_RoutingUpdate:
			if err := m.handleRoutingUpdate(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TransientPacket:
			if err := m.handleTransientPacket(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		default:
			return
		}
	}
}

func (m *Manager) handleHello(sess *session, envelope *internalproto.Envelope) error {
	if sess.peerID != 0 {
		return errors.New("duplicate hello")
	}

	hello := envelope.GetHello()
	if hello == nil {
		return errors.New("hello body cannot be empty")
	}

	envelopeNodeID := envelope.NodeId
	peerID := hello.NodeId
	if peerID <= 0 || peerID == m.cfg.NodeID {
		return fmt.Errorf("invalid peer id %d", peerID)
	}
	if envelopeNodeID <= 0 {
		return errors.New("hello envelope node id cannot be empty")
	}
	if envelopeNodeID != peerID {
		return fmt.Errorf("hello envelope node id mismatch: got %d want %d", envelopeNodeID, peerID)
	}
	if hello.ProtocolVersion != internalproto.ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", hello.ProtocolVersion)
	}
	if hello.SnapshotVersion != internalproto.SnapshotVersion {
		return fmt.Errorf("unsupported snapshot version %q", hello.SnapshotVersion)
	}
	if hello.MessageWindowSize == 0 {
		return errors.New("hello message window size must be positive")
	}
	if strings.TrimSpace(hello.AdvertiseAddr) == "" {
		return errors.New("hello advertise path cannot be empty")
	}
	if !strings.HasPrefix(strings.TrimSpace(hello.AdvertiseAddr), "/") {
		return errors.New("hello advertise path must start with /")
	}
	if err := m.bindPeerIdentity(sess, peerID); err != nil {
		return err
	}
	if int(hello.MessageWindowSize) != m.cfg.MessageWindowSize {
		m.logSessionWarn("message_window_mismatch", sess, nil).
			Int("local_message_window_size", m.cfg.MessageWindowSize).
			Uint32("remote_message_window_size", hello.MessageWindowSize).
			Msg("continuing with per-node windows")
	}

	sess.remoteSnapshotVersion = hello.SnapshotVersion
	sess.remoteMessageWindowSize = int(hello.MessageWindowSize)
	sess.supportsRouting = hello.SupportsRouting
	sess.supportsMembership = hello.SupportsMembership
	if hello.ConnectionId != 0 {
		sess.connectionID = hello.ConnectionId
	}
	sess.smoothedRTTMs = int64(hello.SmoothedRttMs)
	sess.jitterPenaltyMs = int64(hello.JitterPenaltyMs)
	sess.noteRemoteOriginProgress(hello.OriginProgress)
	m.logSessionEvent("peer_hello_accepted", sess).
		Str("protocol_version", hello.ProtocolVersion).
		Str("snapshot_version", hello.SnapshotVersion).
		Uint32("message_window_size", hello.MessageWindowSize).
		Bool("supports_routing", hello.SupportsRouting).
		Bool("supports_membership", hello.SupportsMembership).
		Msg("peer hello accepted")

	if !sess.beginBootstrap() {
		return errors.New("duplicate bootstrap rejected")
	}
	go m.bootstrapSession(sess)
	return nil
}

func (m *Manager) bootstrapSession(sess *session) {
	if err := m.performTimeSync(sess); err != nil {
		m.logSessionWarn("time_sync_failed", sess, err).
			Msg("time sync failed")
		sess.close()
		return
	}
	state, reason := m.peerClockState(sess.peerID)
	logger := m.logSessionEvent("time_sync_succeeded", sess)
	if state != string(clockStateTrusted) {
		logger = m.logSessionWarn("time_sync_observing", sess, nil)
	}
	logger.
		Str("clock_state", state).
		Str("clock_reason", reason).
		Int64("offset_ms", sess.clockOffset()).
		Int64("rtt_ms", sess.smoothedRTTMs).
		Int64("max_clock_skew_ms", m.cfg.MaxClockSkewMs).
		Msg("time sync completed")
	if !m.activateSession(sess) {
		m.logSessionWarn("duplicate_session_rejected", sess, nil).
			Msg("duplicate session rejected")
		sess.close()
		return
	}
	m.recordSessionDiscoveryConnected(sess)
	m.sendMembershipUpdate(sess)

	sess.markReplicationReady()
	sess.startSyncLoop(func() {
		m.sessionSyncLoop(sess)
	})

	if m.store != nil {
		requested, err := m.requestCatchupIfNeeded(sess)
		if err != nil {
			m.logSessionWarn("request_catchup_failed", sess, err).
				Msg("request catchup failed")
			sess.close()
			return
		}
		if !requested {
			m.sendSnapshotDigest(sess)
		}
	}
}

func (m *Manager) marshalSignedEnvelope(envelope *internalproto.Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("envelope cannot be nil")
	}
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return nil, errors.New("clone envelope")
	}
	signature, err := m.envelopeHMAC(clone)
	if err != nil {
		return nil, err
	}
	clone.Hmac = signature
	return marshalOptions.Marshal(clone)
}

func (m *Manager) verifyEnvelope(envelope *internalproto.Envelope) error {
	if envelope == nil {
		return errors.New("envelope cannot be nil")
	}
	if len(envelope.Hmac) == 0 {
		return errors.New("envelope hmac cannot be empty")
	}
	expected, err := m.envelopeHMAC(envelope)
	if err != nil {
		return err
	}
	if !hmac.Equal(envelope.Hmac, expected) {
		return errors.New("envelope hmac mismatch")
	}
	return nil
}

func (m *Manager) envelopeHMAC(envelope *internalproto.Envelope) ([]byte, error) {
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return nil, errors.New("clone envelope")
	}
	clone.Hmac = nil
	payload, err := marshalOptions.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope for hmac: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(m.cfg.ClusterSecret))
	if _, err := mac.Write(payload); err != nil {
		return nil, fmt.Errorf("write envelope hmac: %w", err)
	}
	return mac.Sum(nil), nil
}

func validatePeerEnvelope(sess *session, envelope *internalproto.Envelope) error {
	envelopeNodeID := envelope.NodeId
	if envelopeNodeID <= 0 {
		return errors.New("envelope node id cannot be empty")
	}
	if sess.peerID == 0 {
		return errors.New("session has not completed hello")
	}
	if envelopeNodeID != sess.peerID {
		return fmt.Errorf("envelope node id mismatch: got %d want %d", envelopeNodeID, sess.peerID)
	}
	return nil
}

func connectionIDForHello(sess *session) uint64 {
	if sess == nil {
		return 0
	}
	return sess.connectionID
}

func (m *Manager) bindPeerIdentity(sess *session, peerID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sess.peerID != 0 {
		if sess.peerID != peerID {
			return fmt.Errorf("session peer id mismatch: got %d want %d", peerID, sess.peerID)
		}
		return nil
	}

	if sess.configuredPeer != nil {
		if sess.configuredPeer.nodeID != 0 && sess.configuredPeer.nodeID != peerID {
			return fmt.Errorf("peer mismatch: expected %d got %d", sess.configuredPeer.nodeID, peerID)
		}
		for _, configured := range m.configuredPeers {
			if configured != sess.configuredPeer && configured.nodeID == peerID {
				return fmt.Errorf("peer %d already bound to configured url %s", peerID, configured.URL)
			}
		}
		sess.configuredPeer.nodeID = peerID
	}

	sess.peerID = peerID
	if _, ok := m.peers[peerID]; !ok {
		m.peers[peerID] = &peerState{
			clockState:   clockStateProbing,
			sessions:     make(map[uint64]*session),
			routeAdverts: make(map[int64]routeAdvertisement),
		}
	}
	m.peers[peerID].sessions[sess.connectionID] = sess
	m.refreshNodeClockStateLocked()
	return nil
}
