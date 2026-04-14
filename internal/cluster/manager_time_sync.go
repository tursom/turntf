package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

func (m *Manager) handleTimeSyncRequest(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	req := envelope.GetTimeSyncRequest()
	if req == nil {
		return errors.New("time sync request body cannot be empty")
	}
	if req.RequestId == 0 {
		return errors.New("time sync request id cannot be empty")
	}

	receivedAtMs := m.clock.PhysicalTimeMs()
	sentAtMs := m.clock.PhysicalTimeMs()
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_TimeSyncResponse{
			TimeSyncResponse: &internalproto.TimeSyncResponse{
				RequestId:           req.RequestId,
				ClientSendTimeMs:    req.ClientSendTimeMs,
				ServerReceiveTimeMs: receivedAtMs,
				ServerSendTimeMs:    sentAtMs,
			},
		},
	})
	return nil
}

func (m *Manager) handleTimeSyncResponse(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	resp := envelope.GetTimeSyncResponse()
	if resp == nil {
		return errors.New("time sync response body cannot be empty")
	}
	if resp.RequestId == 0 {
		return errors.New("time sync response id cannot be empty")
	}

	if !sess.resolveTimeSync(resp.RequestId, timeSyncResult{
		response:     resp,
		receivedAtMs: m.clock.PhysicalTimeMs(),
	}) {
		return nil
	}
	return nil
}

func (m *Manager) performTimeSync(sess *session) error {
	var (
		best timeSyncSample
		err  error
	)
	if m.timeSyncer != nil {
		best, err = m.timeSyncer(sess)
	} else {
		best, err = m.collectTimeSyncSample(sess)
	}
	if err != nil {
		state, reason := m.recordTimeSyncFailure(sess, err)
		if state == clockStateRejected {
			return fmt.Errorf("peer %d clock rejected after time sync failure: %s", sess.peerID, reason)
		}
		return nil
	}

	sess.setClockOffset(best.offsetMs)
	sess.observeRTT(best.rttMs)
	state, reason := m.recordTimeSyncSample(sess, best)
	if state == clockStateRejected {
		return fmt.Errorf("peer %d clock rejected after time sync sample: %s", sess.peerID, reason)
	}
	return nil
}

func (m *Manager) collectTimeSyncSample(sess *session) (timeSyncSample, error) {
	var (
		best      timeSyncSample
		found     bool
		lastErr   error
		minRTT    int64
		maxRTT    int64
		haveRange bool
	)

	for range timeSyncSampleCount {
		sample, err := m.timeSyncRoundTrip(sess)
		if err != nil {
			lastErr = err
			continue
		}
		if !haveRange {
			minRTT = sample.rttMs
			maxRTT = sample.rttMs
			haveRange = true
		} else {
			if sample.rttMs < minRTT {
				minRTT = sample.rttMs
			}
			if sample.rttMs > maxRTT {
				maxRTT = sample.rttMs
			}
		}
		if !found || sample.rttMs < best.rttMs {
			best = sample
			found = true
		}
	}
	if found {
		jitterMs := maxRTT - minRTT
		best.uncertaintyMs = maxInt64(best.rttMs/2, jitterMs/2) + 50
		best.credible = best.rttMs <= m.cfg.ClockCredibleRttMs
		if best.sampledAt.IsZero() {
			best.sampledAt = time.Now().UTC()
		}
		return best, nil
	}
	if lastErr == nil {
		lastErr = errors.New("time sync failed without successful samples")
	}
	return timeSyncSample{}, lastErr
}

func (m *Manager) timeSyncRoundTrip(sess *session) (timeSyncSample, error) {
	requestID, resultCh := sess.beginTimeSync()
	clientSendTimeMs := m.clock.PhysicalTimeMs()
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_TimeSyncRequest{
			TimeSyncRequest: &internalproto.TimeSyncRequest{
				RequestId:        requestID,
				ClientSendTimeMs: clientSendTimeMs,
			},
		},
	})

	timer := time.NewTimer(m.clockSyncTimeout())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		sess.cancelTimeSync(requestID, context.Canceled)
		return timeSyncSample{}, ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			return timeSyncSample{}, result.err
		}
		if result.response == nil {
			return timeSyncSample{}, errors.New("time sync response was empty")
		}

		serverReceiveMs := result.response.ServerReceiveTimeMs
		serverSendMs := result.response.ServerSendTimeMs
		clientReceiveMs := result.receivedAtMs
		offsetMs := ((serverReceiveMs - clientSendTimeMs) + (serverSendMs - clientReceiveMs)) / 2
		rttMs := clientReceiveMs - clientSendTimeMs - (serverSendMs - serverReceiveMs)
		if rttMs < 0 {
			rttMs = 0
		}
		return timeSyncSample{
			offsetMs:  offsetMs,
			rttMs:     rttMs,
			sampledAt: time.Now().UTC(),
		}, nil
	case <-timer.C:
		sess.cancelTimeSync(requestID, context.DeadlineExceeded)
		return timeSyncSample{}, fmt.Errorf("time sync with peer %d timed out", sess.peerID)
	}
}

func (m *Manager) sessionSyncLoop(sess *session) {
	if m.ctx == nil {
		return
	}
	timeSyncTicker := time.NewTicker(timeSyncInterval)
	defer timeSyncTicker.Stop()
	catchupTicker := time.NewTicker(catchupRetryInterval)
	defer catchupTicker.Stop()
	antiEntropyTicker := time.NewTicker(antiEntropyInterval)
	defer antiEntropyTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timeSyncTicker.C:
			if sess.isClosed() {
				return
			}
			if err := m.performTimeSync(sess); err != nil {
				m.logSessionWarn("periodic_time_sync_failed", sess, err).
					Msg("periodic time sync failed")
				sess.close()
				return
			}
			state, reason := m.peerClockState(sess.peerID)
			eventName := "periodic_time_sync_succeeded"
			logger := m.logSessionDebug(eventName, sess)
			if state != string(clockStateTrusted) {
				eventName = "periodic_time_sync_observing"
				logger = m.logSessionWarn(eventName, sess, nil)
			}
			logger.
				Str("clock_state", state).
				Str("clock_reason", reason).
				Int64("offset_ms", sess.clockOffset()).
				Int64("rtt_ms", sess.smoothedRTTMs).
				Msg("periodic time sync completed")
		case <-catchupTicker.C:
			if sess.isClosed() {
				return
			}
			if _, err := m.requestCatchupIfNeeded(sess); err != nil {
				m.logSessionWarn("periodic_catchup_failed", sess, err).
					Msg("periodic catchup failed")
				sess.close()
				return
			}
		case <-antiEntropyTicker.C:
			if sess.isClosed() {
				return
			}
			m.sendSnapshotDigest(sess)
		}
	}
}

func (m *Manager) markPeerClockSynced(sess *session, offsetMs int64) {
	_, _ = m.recordTimeSyncSample(sess, timeSyncSample{
		offsetMs:      offsetMs,
		rttMs:         maxInt64(sess.smoothedRTTMs, 1),
		uncertaintyMs: 50,
		sampledAt:     time.Now().UTC(),
		credible:      true,
	})
}

func (m *Manager) clearPeerClockSync(sess *session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return
	}
	cleared := false
	if peer.trustedSession == sess {
		peer.trustedSession = nil
		peer.clockState = clockStateObserving
		cleared = true
	}
	m.recomputeRoutesLocked()
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
	if cleared {
		m.logSessionEvent("peer_clock_untrusted", sess).
			Msg("peer clock is no longer trusted")
	}
}

func (m *Manager) recomputeClockOffsetLocked() {
	offsets := make([]int64, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.trustedSession != nil {
			offsets = append(offsets, peer.clockOffsetMs)
		}
	}
	if len(offsets) == 0 {
		m.clock.SetOffsetMs(0)
		return
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	m.clock.SetOffsetMs(offsets[len(offsets)/2])
}

func (m *Manager) validateBatchHLC(sentAtRaw string, events []*internalproto.ReplicatedEvent) error {
	if m.cfg.MaxClockSkewMs == 0 {
		return nil
	}
	maxAllowedWallTime := m.clock.WallTimeMs() + m.cfg.MaxClockSkewMs
	sentAt, err := clock.ParseTimestamp(strings.TrimSpace(sentAtRaw))
	if err != nil {
		return fmt.Errorf("parse event batch sent_at_hlc: %w", err)
	}
	if sentAt.WallTimeMs > maxAllowedWallTime {
		return fmt.Errorf("event batch sent_at_hlc %s exceeds local wall time %d by more than %dms", sentAt, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
	}
	maxEventHLC := clock.Timestamp{}
	for _, event := range events {
		if event == nil {
			continue
		}
		hlc, err := clock.ParseTimestamp(strings.TrimSpace(event.Hlc))
		if err != nil {
			return fmt.Errorf("parse replicated event hlc: %w", err)
		}
		if hlc.WallTimeMs > maxAllowedWallTime {
			return fmt.Errorf("replicated event hlc %s exceeds local wall time %d by more than %dms", hlc, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
		}
		if maxEventHLC == (clock.Timestamp{}) || hlc.Compare(maxEventHLC) > 0 {
			maxEventHLC = hlc
		}
	}
	if maxEventHLC != (clock.Timestamp{}) && sentAt.Compare(maxEventHLC) < 0 {
		return fmt.Errorf("event batch sent_at_hlc %s is earlier than max event hlc %s", sentAt, maxEventHLC)
	}
	return nil
}

func (m *Manager) peerClockState(peerID int64) (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, reason := m.peerClockStateLocked(peerID)
	return string(state), reason
}
