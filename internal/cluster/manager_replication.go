package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) broadcastEvent(event store.Event) {
	replicated := store.ToReplicatedEvent(event)
	envelope := &internalproto.Envelope{
		NodeId:    m.cfg.NodeID,
		Sequence:  uint64(event.Sequence),
		SentAtHlc: event.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events:       []*internalproto.ReplicatedEvent{replicated},
				OriginNodeId: event.OriginNodeID,
			},
		},
	}

	for _, sess := range m.activeSessions() {
		sess.enqueue(envelope)
	}
	m.publishLibP2PEvent(envelope)
}

func (m *Manager) handleAck(sess *session, envelope *internalproto.Envelope) error {
	ack := envelope.GetAck()
	if ack == nil {
		return errors.New("ack body cannot be empty")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	if ack.NodeId <= 0 {
		return errors.New("ack node id cannot be empty")
	}
	if ack.NodeId != sess.peerID {
		return fmt.Errorf("ack node id mismatch: got %d want %d", ack.NodeId, sess.peerID)
	}
	if ack.OriginNodeId <= 0 {
		return errors.New("ack origin node id cannot be empty")
	}
	if m.store != nil {
		if err := m.store.RecordPeerAck(context.Background(), sess.peerID, ack.OriginNodeId, int64(ack.AckedEventId)); err != nil {
			return err
		}
	}

	m.mu.Lock()
	if peer, ok := m.peers[sess.peerID]; ok && ack.AckedEventId > peer.lastAck {
		peer.lastAck = ack.AckedEventId
	}
	m.mu.Unlock()
	m.logSessionDebug("peer_ack_recorded", sess).
		Int64("origin_node_id", ack.OriginNodeId).
		Uint64("acked_event_id", ack.AckedEventId).
		Msg("peer ack recorded")
	return nil
}

func (m *Manager) handleEventBatch(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return errors.New("event batch received before replication was ready")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	batch := envelope.GetEventBatch()
	if batch == nil {
		return errors.New("event batch body cannot be empty")
	}

	events := batch.GetEvents()
	originNodeID := batch.GetOriginNodeId()
	if originNodeID <= 0 && len(events) > 0 && events[0] != nil {
		originNodeID = events[0].GetOriginNodeId()
	}
	if len(events) == 0 {
		if batch.GetPullRequestId() == 0 {
			return errors.New("event batch cannot be empty")
		}
		if originNodeID <= 0 {
			return errors.New("empty pull response origin node id cannot be empty")
		}
		sess.noteRemoteOriginEvent(originNodeID, sess.remoteOriginEventID(originNodeID))
		sess.completePendingPull(originNodeID, batch.GetPullRequestId())
		if m.store != nil {
			requested, err := m.requestCatchupIfNeeded(sess)
			if err != nil {
				return err
			}
			if !requested {
				m.sendSnapshotDigest(sess)
			}
		}
		m.logSessionDebug("event_batch_pull_completed", sess).
			Int64("origin_node_id", originNodeID).
			Uint64("pull_request_id", batch.GetPullRequestId()).
			Msg("empty pull response completed")
		return nil
	}

	if envelope.Sequence == 0 {
		return errors.New("event batch sequence cannot be empty")
	}
	if strings.TrimSpace(envelope.SentAtHlc) == "" {
		return errors.New("event batch sent_at_hlc cannot be empty")
	}
	m.mu.Lock()
	if err := m.allowEventApplyLocked(sess.peerID); err != nil {
		m.mu.Unlock()
		m.logSessionWarn("event_batch_rejected_by_clock", sess, nil).
			Str("reason", err.Error()).
			Msg("event batch rejected by clock protection")
		return err
	}
	m.mu.Unlock()
	if err := m.validateBatchHLC(envelope.SentAtHlc, events); err != nil {
		return err
	}

	lastEventID, err := validateOriginEventBatch(originNodeID, events)
	if err != nil {
		return err
	}
	sess.noteRemoteOriginEvent(originNodeID, uint64(lastEventID))

	for _, event := range events {
		if err := m.store.ApplyReplicatedEvent(context.Background(), event); err != nil {
			return err
		}
	}

	shouldAdvanceCursor := batch.GetPullRequestId() > 0 && sess.completePendingPull(originNodeID, batch.GetPullRequestId())
	if !shouldAdvanceCursor && !sess.hasPendingPull(originNodeID) {
		shouldAdvanceCursor = true
	}

	ackedEventID := uint64(0)
	if m.store != nil {
		if shouldAdvanceCursor {
			if err := m.store.RecordOriginApplied(context.Background(), originNodeID, int64(lastEventID)); err != nil {
				return err
			}
		}
		cursor, err := m.store.GetOriginCursor(context.Background(), originNodeID)
		if err != nil {
			return err
		}
		ackedEventID = uint64(cursor.AppliedEventID)
	}

	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_Ack{
			Ack: &internalproto.Ack{
				NodeId:       m.cfg.NodeID,
				OriginNodeId: originNodeID,
				AckedEventId: ackedEventID,
			},
		},
	})
	m.logSessionEvent("event_batch_applied", sess).
		Int64("origin_node_id", originNodeID).
		Int("event_count", len(events)).
		Int64("last_event_id", lastEventID).
		Bool("from_pull", batch.GetPullRequestId() > 0).
		Uint64("pull_request_id", batch.GetPullRequestId()).
		Uint64("acked_event_id", ackedEventID).
		Msg("event batch applied")
	if m.store != nil {
		requested, err := m.requestCatchupIfNeeded(sess)
		if err != nil {
			return err
		}
		if !requested {
			m.sendSnapshotDigest(sess)
		}
	}
	return nil
}

func (m *Manager) handlePullEvents(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return errors.New("pull events received before replication was ready")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	pull := envelope.GetPullEvents()
	if pull == nil {
		return errors.New("pull events body cannot be empty")
	}
	if pull.OriginNodeId <= 0 {
		return errors.New("pull events origin node id cannot be empty")
	}
	if pull.RequestId == 0 {
		return errors.New("pull events request id cannot be empty")
	}

	limit := int(pull.GetLimit())
	if limit <= 0 || limit > pullBatchSize {
		limit = pullBatchSize
	}

	events, err := m.store.ListEventsByOrigin(context.Background(), pull.OriginNodeId, int64(pull.GetAfterEventId()), limit)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		m.logSessionDebug("pull_events_served", sess).
			Int64("origin_node_id", pull.OriginNodeId).
			Uint64("request_id", pull.RequestId).
			Int("event_count", 0).
			Uint64("after_event_id", pull.GetAfterEventId()).
			Msg("served empty pull events response")
		sess.enqueue(&internalproto.Envelope{
			NodeId: m.cfg.NodeID,
			Body: &internalproto.Envelope_EventBatch{
				EventBatch: &internalproto.EventBatch{
					PullRequestId: pull.RequestId,
					OriginNodeId:  pull.OriginNodeId,
				},
			},
		})
		return nil
	}

	replicated := make([]*internalproto.ReplicatedEvent, 0, len(events))
	for _, event := range events {
		replicated = append(replicated, store.ToReplicatedEvent(event))
	}

	last := events[len(events)-1]
	m.logSessionEvent("pull_events_served", sess).
		Int64("origin_node_id", pull.OriginNodeId).
		Uint64("request_id", pull.RequestId).
		Int("event_count", len(events)).
		Uint64("after_event_id", pull.GetAfterEventId()).
		Int64("last_event_id", last.EventID).
		Msg("served pull events response")
	sess.enqueue(&internalproto.Envelope{
		NodeId:    m.cfg.NodeID,
		Sequence:  uint64(last.Sequence),
		SentAtHlc: last.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events:        replicated,
				PullRequestId: pull.RequestId,
				OriginNodeId:  pull.OriginNodeId,
			},
		},
	})
	return nil
}

func (m *Manager) requestCatchupIfNeeded(sess *session) (bool, error) {
	if m.store == nil || sess.peerID == 0 {
		return false, nil
	}

	remoteProgress := sess.remoteOriginProgressSnapshot()
	originNodeIDs := make([]int64, 0, len(remoteProgress))
	for originNodeID := range remoteProgress {
		originNodeIDs = append(originNodeIDs, originNodeID)
	}
	sort.Slice(originNodeIDs, func(i, j int) bool {
		return originNodeIDs[i] < originNodeIDs[j]
	})

	requested := false
	for _, originNodeID := range originNodeIDs {
		remoteLastEventID := remoteProgress[originNodeID]
		cursor, err := m.store.GetOriginCursor(context.Background(), originNodeID)
		if err != nil {
			return false, err
		}
		appliedEventID := uint64(cursor.AppliedEventID)
		if appliedEventID >= remoteLastEventID {
			continue
		}
		requestID, ok := sess.beginPendingPull(originNodeID, appliedEventID)
		if !ok {
			requested = true
			continue
		}

		envelope, err := m.buildPullEventsEnvelope(originNodeID, appliedEventID, requestID)
		if err != nil {
			sess.cancelPendingPull(originNodeID, requestID)
			return false, err
		}
		m.logSessionEvent("catchup_requested", sess).
			Int64("origin_node_id", originNodeID).
			Uint64("after_event_id", appliedEventID).
			Uint64("remote_last_event_id", remoteLastEventID).
			Uint64("request_id", requestID).
			Msg("requested catchup from peer")
		sess.enqueue(envelope)
		requested = true
	}
	return requested, nil
}

func (m *Manager) buildPullEventsEnvelope(originNodeID int64, afterEventID, requestID uint64) (*internalproto.Envelope, error) {
	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_PullEvents{
			PullEvents: &internalproto.PullEvents{
				OriginNodeId: originNodeID,
				AfterEventId: afterEventID,
				Limit:        pullBatchSize,
				RequestId:    requestID,
			},
		},
	}, nil
}

func validateOriginEventBatch(originNodeID int64, events []*internalproto.ReplicatedEvent) (int64, error) {
	if originNodeID <= 0 {
		return 0, errors.New("event batch origin node id cannot be empty")
	}
	if len(events) == 0 {
		return 0, errors.New("event batch cannot be empty")
	}

	var lastEventID int64
	for i, event := range events {
		if event == nil {
			return 0, errors.New("event batch cannot contain nil events")
		}
		if event.OriginNodeId != originNodeID {
			return 0, fmt.Errorf("event batch origin mismatch: got %d want %d", event.OriginNodeId, originNodeID)
		}
		if event.EventId == 0 {
			return 0, errors.New("event batch event id cannot be empty")
		}
		if i > 0 && event.EventId <= lastEventID {
			return 0, fmt.Errorf("event batch event ids must be strictly increasing: %d then %d", lastEventID, event.EventId)
		}
		lastEventID = event.EventId
	}
	return lastEventID, nil
}
